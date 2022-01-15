import gc

import math

import re
from collections import defaultdict
from slugify import slugify

from tqdm import tqdm

import m_config as cf
from api.resources.m_db_item import (
    DBItem,
    DBDeletes,
    DBItemDefault,
    serialize,
    serialize_value,
)
from api.resources.m_item import MItem
from api.utilities import m_io as iw, m_sim
from api.utilities import m_utils as ul
from api import m_f
from contextlib import closing
from multiprocessing.pool import Pool
import heapq


def pool_page_rank(args):
    label_lid, wd_lid = args
    limit = 1000

    try:
        label_pagerank = m_f.m_items().get_pagerank_score(wd_lid)
        label_pagerank = sorted(
            label_pagerank.items(), key=lambda x: x[1], reverse=True
        )[:limit]
        label_pagerank = [
            [lid, (score - cf.WEIGHT_PR_MIN) / cf.WEIGHT_PR_DIV]
            for lid, score in label_pagerank
        ]
        bytes_obj = serialize(label_lid, label_pagerank, integerkey=True)
        return bytes_obj
    except Exception as message:
        iw.print_status(f"{label_lid} - {message}")
        return None

    # try:
    #     wd_set_rank = defaultdict(float)
    #     for _wd_id in wd_lid:
    #         wd_id_page_rank = m_f.m_items().get_pagerank_score(_wd_id)
    #         wd_id_page_rank = (wd_id_page_rank - cf.WEIGHT_PR_MIN) / cf.WEIGHT_PR_DIV
    #         wd_set_rank[_wd_id] = wd_id_page_rank
    #
    #         if len(wd_set_rank) > 1e5:
    #             tmp = sorted(wd_set_rank.items(), key=lambda x: x[1], reverse=True)
    #             tmp = {k: v for k, v in tmp[:limit]}
    #             wd_set_rank.clear()
    #             gc.collect()
    #             wd_set_rank = tmp
    #
    #     wd_set_rank = sorted(wd_set_rank.items(), key=lambda x: x[1], reverse=True)[
    #         :limit
    #     ]
    #     bytes_obj = serialize(label_lid, wd_set_rank, integerkey=True)
    #     return bytes_obj
    # except Exception as message:
    #     iw.print_status(f"{label_lid} - {message}")
    #     return None


def norm_entity_labels(wd_names, is_en=True):
    # Normalize text
    names = set()
    for wd_name in wd_names:
        names.add(wd_name)
        if "(" in wd_name:
            new_name = re.sub(r"\((.*)\)", "", wd_name).strip()
            if new_name:
                names.add(new_name)

    add_wd_names = set()
    for wd_name in names:
        # norm1 = ul.norm_text(can, punctuations=True)
        norm2 = ul.norm_text(wd_name, punctuations=False)
        # add_wd_names.add(norm1)
        # if norm2[:20] != norm1[:20]:
        #     add_wd_names.add(norm2)
        add_wd_names.add(norm2)
    return add_wd_names


def pool_extract_entity_names(wd_id):
    wd_lid = m_f.m_items().get_vocab_wd_id(wd_id)
    if ul.is_redirect_or_disambiguation(wd_lid):
        return wd_id, set(), set()
    wd_names_all = m_f.m_items().get_aliases_multilingual(wd_id)
    wd_names_en = m_f.m_items().get_aliases(wd_id)
    wd_label = m_f.m_items().get_label(wd_id)
    if wd_label and not ul.is_wd_item(wd_label):
        wd_names_en.add(wd_label)
    # Human abbreviation
    wd_types = m_f.m_items().get_types_all_wikidata(wd_lid)
    if wd_types and "Q5" in wd_types:
        name_abb = wd_label.split()
        if len(name_abb) > 1:
            name_abb = name_abb[0][0] + ". " + " ".join(name_abb[1:])
            wd_names_en.add(name_abb)

    # ID form other databases
    wd_claims = m_f.m_items().get_litRel_litTail_wikidata(wd_id)
    if wd_claims:
        for prop_altname in [
            "P528",  # catalog code
            "P3083",  # SIMBAD ID(P3083)
            "P3382",  # GeneDB ID
            "P742",  # pseudonym
            "P1845",  # anti-virus alias (P1845)
            "P8338",  # applies to name of value (P8338)
            "P5168",  # applies to name of item (P5168)
            "P1449",  # nickname (P1449)
            "P4970",  # alternate names (P4970)
            "P2561",  # name (P2561)
            "P1448",  # official name (P1448)
            "P1813",  # short name (P1813)
            "P1843",  # taxon common name (P1843)
        ]:
            search_space = set()
            if wd_claims["string"].get(prop_altname):
                search_space.update(wd_claims["string"].get(prop_altname))
            if wd_claims["links"].get(prop_altname):
                search_space.update(wd_claims["links"].get(prop_altname))
            for wd_other_id in search_space:
                if ul.convert_num(wd_other_id) is None:
                    if ul.isEnglish(wd_other_id):
                        wd_names_en.add(wd_other_id)
                    else:
                        wd_names_all.add(wd_other_id)

    wd_names_all.update(wd_names_en)
    # Normalize
    wd_names_all = norm_entity_labels(wd_names_all)
    wd_names_en = norm_entity_labels(wd_names_en)
    return wd_id, wd_names_en, wd_names_all


class MEntityLabels(DBItem):
    def __init__(self, db_file=cf.DIR_WIKI_LABELS):
        super().__init__(db_file=db_file, max_db=6, map_size=cf.SIZE_1GB * 200)
        self._db_file = db_file
        # word (entity label) - id (local id - int)
        self.db_vocab = self._env.open_db(b"__vocab__")
        # id (local id - int) - word (entity label)
        self.db_vocab_inv = self._env.open_db(b"__vocab_inv_", integerkey=True)

        # id (local id - int) - wd id (entity id (wikidata))
        self.db_labels_en = self._env.open_db(b"__words__", integerkey=True)
        self.db_labels_all = self._env.open_db(
            b"__words_multilingual__", integerkey=True
        )

        # id (local id - int) - page rank scores (float)
        self.db_labels_page_rank_en = self._env.open_db(
            b"__words_page_rank__", integerkey=True
        )
        self.db_labels_page_rank_all = self._env.open_db(
            b"__words_page_rank_multilingual__", integerkey=True
        )

        self.buff_vocab = defaultdict(int)
        self.buff_labels_en = defaultdict(set)
        self.buff_labels_all = defaultdict(set)

        self._len_vocab = self.size_vocab()
        self._max_length = 10000

    def get_label_lid(self, query):
        lid = self.get_value(self.db_vocab, query, compress_value=False)
        if lid is None:
            query_norm = ul.norm_text(query, punctuations=False)
            if query_norm != query:
                lid = self.get_value(self.db_vocab, query_norm, compress_value=False)
        return lid

    def _is_available_lang(self, db, query):
        lid = self.get_label_lid(query)
        if lid is None:
            return False

        if self.is_available(db, lid, integerkey=True):
            return True

        return False

    def is_available_en(self, query):
        return self._is_available_lang(self.db_labels_en, query)

    def is_available_all(self, query):
        return self._is_available_lang(self.db_labels_all, query)

    def _get_wd_qid_lang(
        self, db_labels, db_labels_page_rank, query, page_rank=True, get_qid=True
    ):
        key = self.get_label_lid(query)
        if key is None:
            return None

        if not page_rank:
            responds_lid = self.get_value(
                db_labels,
                key,
                integerkey=True,
                bytes_value=cf.ToBytesType.INT_LIST,
                compress_value=True,
            )
            if responds_lid and get_qid:
                tmp = m_f.m_items().get_vocab_wd_id(responds_lid)
                if tmp:
                    responds_lid = sorted(tmp.values())
        else:
            responds_lid = self.get_value(
                db_labels_page_rank, key, integerkey=True, compress_value=True
            )
            if responds_lid and get_qid:
                responds_lid = [
                    [m_f.m_items().get_vocab_wd_id(k), v] for k, v in responds_lid
                ]

        return responds_lid

    def get_wd_qid_en(self, query, page_rank=True, get_qid=True):
        return self._get_wd_qid_lang(
            self.db_labels_en, self.db_labels_page_rank_en, query, page_rank, get_qid
        )

    def get_wd_qid_all(self, query, page_rank=True, get_qid=True):
        return self._get_wd_qid_lang(
            self.db_labels_all, self.db_labels_page_rank_all, query, page_rank, get_qid
        )

    def iter_vocab(self, from_i=0, to_i=-1, get_values=True):
        return self.get_db_iter(
            self.db_vocab,
            from_i=from_i,
            to_i=to_i,
            compress_value=False,
            get_values=get_values,
        )

    def iter_vocab_inv(self, from_i=0, to_i=-1, get_values=True):
        return self.get_db_iter(
            self.db_vocab_inv,
            from_i=from_i,
            integerkey=True,
            to_i=to_i,
            get_values=get_values,
            compress_value=False,
        )

    def iter_vocab_inv_from_lid(self, from_i=0, to_i=0, get_values=True):
        if to_i == -1:
            to_i = self.get_db_size(self.db_vocab_inv)
        for i in range(from_i, to_i):
            if get_values:
                value = self.get_label_from_lid(i)
                yield value, i
            else:
                yield i

    def iter_update_en_labels(self):
        new_keys = {
            k
            for k in self.get_db_iter(
                self.db_labels_en, get_values=False, integerkey=True
            )
        }
        old_keys = iw.load_obj_pkl(f"{cf.DIR_MODELS}/old_keys_en.pkl")

        iw.save_obj_pkl(f"{cf.DIR_MODELS}/old_keys_en.pkl", new_keys)
        new_keys = set(new_keys) ^ set(old_keys)
        del old_keys
        # new_keys = iw.load_obj_pkl(f"{cf.DIR_MODELS}/new_keys_en.pkl")
        new_keys = sorted(new_keys, key=lambda x: x)
        for key in new_keys:
            label = self.get_label_from_lid(key)
            yield label, key

    def _iter_labels(self, db, from_label=True, from_i=0, to_i=-1):
        if to_i == -1:
            to_i = self.get_db_size(db)
        if from_label:
            iter_item = self.get_db_iter(self.db_vocab, compress_value=False)
        else:
            if db == self.db_labels_all:
                iter_item = self.iter_vocab_inv_from_lid(from_i=from_i, to_i=to_i)
            else:
                iter_item = self.iter_update_en_labels()

        i = 0
        for wd_label, label_lid in iter_item:
            if db == self.db_labels_en:
                if from_label and self.is_available(db, label_lid, integerkey=True):
                    i += 1
                    if i < from_i:
                        continue
                    if i > to_i:
                        break
                    yield wd_label, label_lid
                if not from_label:
                    yield wd_label, label_lid
            else:
                yield wd_label, label_lid

    def iter_labels_en(self, from_label=True, from_i=0, to_i=-1):
        return self._iter_labels(
            db=self.db_labels_en, from_label=from_label, from_i=from_i, to_i=to_i
        )

    def iter_labels_all(self, from_label=True, from_i=0, to_i=-1):
        return self._iter_labels(
            db=self.db_labels_all, from_label=from_label, from_i=from_i, to_i=to_i
        )

    def size_vocab(self):
        return self.get_db_size(self.db_vocab_inv)

    def size_labels_en(self):
        return self.get_db_size(self.db_labels_en)

    def size_labels_all(self):
        return self.get_db_size(self.db_labels_all)

    def get_label_from_lid(self, word_id):
        return self.get_value(
            self.db_vocab_inv, word_id, integerkey=True, compress_value=False
        )

    def save_buff_words(self, buff_words, db_words):
        if not buff_words:
            return
        self.update_bulk_with_buffer(
            env=self._env,
            db=db_words,
            data=buff_words,
            show_progress=True,
            update_type=cf.DBUpdateType.SET,
            bytes_value=cf.ToBytesType.INT_LIST,
            integerkey=True,
            message="en" if db_words == self.db_labels_en else "all",
        )

    def save_buff_vocab(self):
        if not self.buff_vocab:
            return
        self.write_bulk_with_buffer(
            self._env,
            self.db_vocab,
            self.buff_vocab,
            compress_value=False,
            show_progress=False,
        )

        # Call inv
        self.buff_vocab = {v: k for k, v in self.buff_vocab.items()}
        self.write_bulk_with_buffer(
            self._env,
            self.db_vocab_inv,
            self.buff_vocab,
            integerkey=True,
            compress_value=False,
            show_progress=False,
        )
        self.buff_vocab = defaultdict(int)

    def build_labels_page_rank(
        self, db, n_cpu=10, from_i=0, step=50000, buff_limit=cf.LMDB_BUFF_BYTES_SIZE
    ):
        buff = []
        buff_size = 0
        db_save = (
            self.db_labels_page_rank_en
            if db == self.db_labels_en
            else self.db_labels_page_rank_all
        )

        def update_desc():
            return (
                f"labels:{(self.get_db_size(db_save) + len(buff)):,}"
                f"|buff:{buff_size / buff_limit *100:.0f}%"
            )

        p_bar = tqdm(total=self.get_db_size(db), desc=update_desc())
        p_bar.update(from_i)
        # if n_cpu == 1:
        #     for i, (label_lid, wd_lid) in enumerate(
        #         self.get_db_iter(
        #             db,
        #             from_i=from_i,
        #             integerkey=True,
        #             bytes_value=cf.ToBytesType.INT_LIST,
        #         )
        #     ):
        #         bytes_obj = pool_page_rank((label_lid, wd_lid))
        with closing(Pool(n_cpu)) as pool:
            for i, bytes_obj in enumerate(
                pool.imap(
                    pool_page_rank,
                    self.get_db_iter(
                        db,
                        from_i=from_i,
                        integerkey=True,
                        bytes_value=cf.ToBytesType.INT_LIST,
                    ),
                )
            ):
                if i and i % step == 0:
                    p_bar.update(step)
                    p_bar.set_description(update_desc())

                if not bytes_obj:
                    continue
                buff_size += len(bytes_obj[0]) + len(bytes_obj[1])
                buff.append(bytes_obj)

                if buff_size >= buff_limit:
                    DBItem.write_bulk(self._env, db_save, buff, sort_key=False)
                    buff = []
                    buff_size = 0
            if buff:
                DBItem.write_bulk(self._env, db_save, buff, sort_key=False)
        p_bar.close()

    def build_vocab(self, n_cpu=1, buff_save=1e6, step=1000):
        update_from_id = self.size_vocab()
        n_label_all = 0
        n_label_en = 0

        def update_desc():
            return (
                f"vocab:{self._len_vocab:,}"
                f"|en:{self.size_labels_en():,} ({n_label_en / buff_save * 100:.0f}%)"
                f"|all:{self.size_labels_all():,} ({n_label_all / buff_save * 100:.0f}%)"
            )

        p_bar = tqdm(desc=update_desc(), total=m_f.m_items().size())
        # if n_cpu == 1:
        #     for wd_i, wd_id in enumerate(m_f.m_items().keys_lid()):
        #         wd_id, wd_names_en, wd_names_all = pool_extract_entity_names(wd_id)

        with closing(Pool(n_cpu)) as pool:
            for wd_i, (wd_id, wd_names_en, wd_names_all) in enumerate(
                pool.imap_unordered(pool_extract_entity_names, m_f.m_items().keys_lid())
            ):
                if wd_i and wd_i % step == 0:
                    p_bar.update(step)
                    p_bar.set_description(update_desc())

                # Add entity labels all
                for wd_name in wd_names_all:
                    if not wd_name:
                        continue
                    wd_name_id, is_new = self.get_vocab_id(wd_name)
                    self.buff_labels_all[wd_name_id].add(wd_id)
                    n_label_all += 1

                for wd_name in wd_names_en:
                    if not wd_name:
                        continue
                    wd_name_id, is_new = self.get_vocab_id(wd_name)
                    self.buff_labels_en[wd_name_id].add(wd_id)
                    n_label_en += 1

                if n_label_all >= buff_save:
                    self.save_buff_vocab()
                    self.save_buff_words(self.buff_labels_all, self.db_labels_all)
                    self.buff_labels_all = defaultdict(set)
                    n_label_all = 0

                if n_label_en >= buff_save:
                    self.save_buff_words(self.buff_labels_en, self.db_labels_en)
                    self.buff_labels_en = defaultdict(set)
                    n_label_en = 0

            self.save_buff_vocab()
            self.save_buff_words(self.buff_labels_all, self.db_labels_all)
            self.save_buff_words(self.buff_labels_en, self.db_labels_en)

        p_bar.close()
        return update_from_id

    def get_vocab_id(self, entity_label, is_add=True):
        v_id = self.buff_vocab.get(entity_label)
        is_new = False
        if v_id is None:
            v_id = self.get_value(self.db_vocab, entity_label)
        if v_id is None and is_add:
            v_id = self._len_vocab
            self.buff_vocab[entity_label] = v_id
            self._len_vocab += 1
            is_new = True
        return v_id, is_new

    def build_deletes(
        self,
        db_name="",
        source="word",
        update_from_id=None,
        update_to_id=None,
        max_dis=2,
        prefix_len=7,
        min_len=1,
        batch_id=0,
    ):
        db_dir = f"{cf.DIR_MODELS}/deletes_{db_name}_d{max_dis}_pl{prefix_len}_dup_{batch_id}.lmdb"
        iw.print_status(db_dir)
        from api.resources.m_db_item import DBDeletes

        db_deletes = DBDeletes(db_file=db_dir, buff_size=cf.SIZE_1GB, buff_save=5e7)
        # current_db = DBItem(db_file=f"{cf.DIR_MODELS}/deletes_d{max_dis}_pl{prefix_len}.lmdb",
        #                     db_default=True)
        # if update_from_id:
        #     def _iter_vocab():
        #         for word_id in range(update_from_id, self.size_vocab()):
        #             yield self.get_word_from_id(word_id), word_id
        #     iter_vocab = _iter_vocab()
        # else:
        from_i = 0
        if update_from_id:
            from_i = update_from_id

        if source == "word":
            iter_vocab = self.iter_vocab(from_i=from_i)
            # update_from_id -= from_i
            # iter_vocab = self.iter_vocab(from_i=from_i)
        else:
            if update_from_id:

                # def _iter_vocab():
                #     word_dict = defaultdict()
                #     for word_id in range(update_from_id, self.size_vocab()):
                #         word_value = self.get_word_from_id(word_id)
                #         if word_value:
                #             word_dict[word_value] = word_id
                #     return sorted(word_dict.items(), key=lambda x: x[0])
                #
                # iter_vocab = _iter_vocab()
                iter_vocab = self.iter_vocab(from_i=update_from_id)
            else:
                iter_vocab = self.iter_vocab()

        if update_to_id:
            to_i = update_to_id - from_i
        else:
            to_i = self.size_vocab() - from_i

        def update_desc():
            return (
                f"d={max_dis}|lp={prefix_len}|"
                f"deletes:{db_deletes.size_deletes() / 1000000:.1f}M|"
                f"data:{db_deletes.size_data() / 1000000:.1f}M|"
                f"buff:{db_deletes.size_buff() / 1000000:.1f}M"
            )

        with tqdm(total=to_i, desc=update_desc()) as p_bar:
            # p_bar.update(from_i)
            # p_bar.update(update_from_id)

            for i, (words, words_id) in enumerate(iter_vocab):
                if i > to_i:
                    break
                p_bar.update()

                # if i < 91814823:  # 4-10: 91814823  8-10: 48855254 2-20: 87885553
                #     continue
                # tmp_punc = ul.norm_text(words, punctuations=True)

                # tmp = ul.norm_text(words, punctuations=False)
                # if i < update_from_id:
                #     continue

                # if i < update_from_id:
                #     tmp_punc = ul.norm_text(words, punctuations=True)
                #     if tmp_punc != words.lower():
                #         tmp = ul.norm_text(words, punctuations=False)
                #         if tmp == tmp_punc:
                #             continue

                # if "\u002D" not in words:
                #     continue

                if ul.is_wd_item(words):
                    continue

                # available = current_db.get_value_default(words[:prefix_len], decoder="bytes_set")
                # if available and words_id in available:
                #     continue

                if len(words) > self._max_length:
                    words = words[: self._max_length]

                edits = ul.delete_edits_prefix(words, max_dis, prefix_len)

                for delete in edits:
                    if len(delete) >= min_len:
                        db_deletes.add(delete, words_id)
                if i and i % 1000 == 0:
                    p_bar.set_description(update_desc())
            db_deletes.save(see_progress=False)
        db_deletes.collect(max_dis, prefix_len, from_i=0)

    def build_deletes_2(
        self,
        from_label=True,
        lang="en",
        from_i=0,
        to_i=-1,
        n_deletes=2,
        prefix_len=7,
        min_len=1,
        step=10000,
        buff_limit=cf.SIZE_1GB * 10,
    ):
        db_dir = f"{cf.DIR_MODELS}/deletes_{lang}_d{n_deletes}_pl{prefix_len}.lmdb"
        iw.print_status(db_dir)
        db_deletes = DBItemDefault(db_file=db_dir)
        buff = defaultdict(set)
        edit_c = 0

        def update_desc():
            return (
                f"d={n_deletes}|lp={prefix_len}|"
                f"deletes:{db_deletes.size():,}|"
                f"buff:{edit_c/buff_limit*100:.0f}%"
            )

        iter_vocab = (
            self.iter_labels_en(from_label=from_label, from_i=from_i, to_i=to_i)
            if lang == "en"
            else self.iter_labels_all(from_label=from_label, from_i=from_i, to_i=to_i)
        )

        if lang == "en" and not from_label:
            tmp = iw.load_obj_pkl(f"{cf.DIR_MODELS}/old_keys_en.pkl")
            from_i = len(tmp)
            del tmp

        db_size = self.size_labels_en() if lang == "en" else self.size_labels_all()
        p_bar = tqdm(total=db_size, desc=update_desc())
        p_bar.update(from_i)
        for i, (wd_label, label_lid) in enumerate(iter_vocab):
            if i and i % step == 0:
                p_bar.set_description(update_desc())
                p_bar.update(step)

            if ul.is_wd_item(wd_label):
                continue

            wd_label_len = len(wd_label)

            if len(wd_label) > prefix_len:
                wd_label = wd_label[:prefix_len]

            edits = ul.delete_edits_prefix(
                wd_label, n_deletes, prefix_len, min_len=min_len
            )
            for delete in edits:
                if delete != wd_label:
                    set_delete = "".join(list(set(delete)))
                    if set_delete in [" ", '"', "'", "_", "-", "()", "{}", "[]", "."]:
                        continue
                key_with_len = f"{delete}|{wd_label_len}"
                if buff.get(key_with_len) is None:
                    edit_c += 32 + len(key_with_len)
                buff[key_with_len].add(label_lid)
                edit_c += 37

            if edit_c >= buff_limit:
                db_deletes.update_bulk_with_buffer(
                    env=db_deletes._env,
                    db=db_deletes.db,
                    data=buff,
                    show_progress=True,
                    update_type=cf.DBUpdateType.SET,
                    bytes_value=cf.ToBytesType.INT_LIST,
                )
                edit_c = 0
                buff = defaultdict(set)
        if buff:
            db_deletes.update_bulk_with_buffer(
                env=db_deletes._env,
                db=db_deletes.db,
                data=buff,
                show_progress=True,
                update_type=cf.DBUpdateType.SET,
                bytes_value=cf.ToBytesType.INT_LIST,
            )
        p_bar.close()
        db_deletes.copy_lmdb()

    def build(self):
        # m_f.load_pagerank_stats()
        #
        # # label|en:118,923,821 (17%)|all:196,971,050 (51%): [19:57:42<00:00, 1322.71it/s]
        # self.build_vocab(n_cpu=8, buff_save=5e6)
        # label: int - 195,925,403 (problem of encoding)
        # int: label - 195,925,416
        # int label en: qid - 118,332,698
        # int label all: qid - 195,925,416

        # # labels:118,300,000|buff:90%: 118300000/118332698 [2:32:34<00:02, 12922.57it/s]
        # self.build_labels_page_rank(self.db_labels_en)
        # # labels:195,900,000|buff:39%: 195900000/195925416 [4:08:43<00:01, 13126.84it/s]
        # self.build_labels_page_rank(self.db_labels_all)

        # self.copy_lmdb()

        # d=2|lp=14|deletes:3,150,766,102|buff:100%:  36% 121340000/195925416 [21:37:06<11:08:13, 3146.05it/s]
        # self.build_deletes_2(
        #     lang="all",
        #     from_label=False,
        #     n_deletes=2,
        #     prefix_len=14,
        #     from_i=195925400,
        #     buff_limit=cf.SIZE_1GB * 8,
        # )

        # d=4|lp=10|deletes:4,594,133,688|buff:99%:  61% 71660000/118332698 [43:29:09<15:45:29, 822.73it/s]
        self.build_deletes_2(
            lang="en",
            from_label=False,
            n_deletes=4,
            prefix_len=10,
            buff_limit=cf.SIZE_1GB * 16,
        )

        # d=4|lp=10|deletes:3,186,740,461|buff:1%:  26% 51270000/195925416 [68:30:36<193:17:50, 207.88it/s]
        # self.build_deletes_2("all", n_deletes=4, prefix_len=10, from_i=51250000)

    def cal_means_len(self):
        from collections import Counter

        c_fre = Counter()
        for i, k in enumerate(self.iter_vocab(get_values=False)):
            c_fre[len(k)] += 1
            # if i > 1000:
            #     break
        sum_of_numbers = sum(number * count for number, count in c_fre.items())
        count = sum(count for n, count in c_fre.items())
        mean = sum_of_numbers / count
        print(f"{mean:.2f} = {sum_of_numbers}/ {count}")


def stats_deletes():
    db = DBItemDefault(f"{cf.DIR_MODELS}/deletes_multilingual_d{4}_pl{10}.lmdb")
    count = [0 for _ in range(8)]
    p_bar = tqdm(total=db.size())
    step = 1000

    def update_desc(i):
        return "|".join([f"{_i}:{_c / i *100:.0f}" for _i, _c in enumerate(count)])

    big_key = defaultdict(set)
    for i, (k, v) in enumerate(
        db.items(integerkey=False, bytes_value=cf.ToBytesType.INT_LIST)
    ):
        if i and i % step == 0:
            p_bar.update(step)
            p_bar.set_description(desc=update_desc(i))
        tmp_k = math.floor(math.log10(len(v)))

        count[tmp_k] += 1

        if tmp_k >= 6:
            iw.print_status(f'{tmp_k}\t"{k}"\t{len(v)}', is_screen=False)
            big_key[tmp_k].add(k)

    p_bar.close()

    for k, v in big_key.items():
        iw.print_status(k + ": " + ", ".join(list(v)))


def fix_index():
    fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_ENGLISH)
    # fz_db.drop_db(fz_db.db_vocab_inv)
    # fz_db.drop_db(fz_db.db_words_page_rank)
    # fz_db.copy_lmdb()
    # return

    tmp = dict()
    for i, (k, v) in enumerate(
        tqdm(
            fz_db.get_db_iter(fz_db.db_vocab, compress_value=False),
            total=fz_db.size_vocab(),
        )
    ):
        if not isinstance(v, int):
            iw.print_status(f"Value is not Int: {k}: {v}")
        tmp[k] = v
        # if i > 100:
        #     break
    return
    tmp = {v: k for k, v in tmp.items()}
    iw.print_status(len(tmp))

    fz_db.write_bulk_with_buffer(
        fz_db.env, fz_db.db_vocab_inv, tmp, integerkey=True, compress_value=False
    )

    for k, v in tqdm(fz_db.iter_vocab_inv(), total=fz_db.size_vocab()):
        debug = 1


def save_old_en_keys(step=1000000):
    fz_db = MEntityLabels(cf.DIR_WIKI_LABELS)
    keys_en = []
    p_bar = tqdm(total=fz_db.size_labels_en())
    for i, k in enumerate(
        fz_db.get_db_iter(fz_db.db_labels_en, get_values=False, integerkey=True)
    ):
        keys_en.append(k)
        if i and i % step == 0:
            p_bar.update(step)
    p_bar.close()

    old_keys = iw.load_obj_pkl(f"{cf.DIR_MODELS}/old_keys_en.pkl", keys_en)
    new_keys = set(keys_en) ^ set(old_keys)
    iw.save_obj_pkl(f"{cf.DIR_MODELS}/new_keys_en.pkl", new_keys)


if __name__ == "__main__":
    m_f.init()
    fz_db = MEntityLabels(cf.DIR_WIKI_LABELS)  #

    # lid = fz_db.get_label_lid("Tb10.NT.103")
    #
    fz_db.build()

    # db_deletes = DBItemDefault(db_file=f"{cf.DIR_MODELS}/deletes_all_d2_pl14.lmdb")
    # db_deletes.copy_lmdb()

    # query = "America"
    # tmp1 = fz_db.get_wd_qid_en(query, page_rank=True)
    # tmp2 = fz_db.get_wd_qid_all(query, page_rank=True)

    exit()
    # fix_index()
    # m_f.init(is_log=True)
    # stats_deletes()
    # exit()

    # tmp = fz_db.get_words("Mount Everest", page_rank=False)
    # fz_db.build_words_page_rank(n_cpu=6)

    # fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL)
    # fz_db.build_vocab(n_cpu=4, buff_save=1e7)  # 2e7
    # fz_db.build_words_page_rank(n_cpu=6)

    # debug = 1
    # fz_db.copy()

    # for k, v in tqdm(fz_db.iter_words()):
    #     debug = 1

    # for k, v in tqdm():
    # from time import time
    # start = time()
    # fz_db.build_vocab(n_cpu=1, buff_save=1e4)
    # print(time() - start)
    print("Done")


"""
English
from:0|vocab:104.41M|label:104.41M|buff_n:11.66M: 100%|███████| 95064301/95064301 [9:37:00<00:00, 2745.89it/s]

vocab: 113 410 400 
"""
