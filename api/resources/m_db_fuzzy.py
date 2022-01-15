import gc
from multiprocessing import Pool

import os
import struct
import zlib

from api.resources.m_db_item import DBItem
from api.resources.m_item_labels import MEntityLabels
from collections import defaultdict
from contextlib import closing

import lmdb
import msgpack
import numpy as np
from lz4 import frame
from tqdm import tqdm

import m_config as cf
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


class DBDupInt(DBItem):
    def __init__(self, db_file, db_name, map_size=cf.SIZE_1GB * 10):
        super().__init__(
            db_file=db_file, max_db=1, map_size=map_size,
        )
        self.db = self._env.open_db(
            db_name, dupsort=True, integerdup=True, dupfixed=True
        )

    def size(self, len_buff=0):
        res = self.get_db_size(self.db)
        if len_buff:
            res += len_buff
        return res

    def put_multi(self, data, single_write=False):
        added_items = 0
        try:
            with self.env.begin(db=self.db, write=True, buffers=True) as txn:
                if not single_write:
                    _, added_items = txn.cursor().putmulti(data, dupdata=True)
                else:
                    for k, v in data:
                        txn.put(k, v)
                        added_items += 1
        except lmdb.MapFullError:
            curr_limit = self.env.info()["map_size"]
            new_limit = curr_limit + cf.SIZE_1GB * 5
            self.env.set_mapsize(new_limit)
            return self.put_multi(data)
        except lmdb.BadValsizeError:
            iw.print_status(lmdb.BadValsizeError)
        except lmdb.BadTxnError:
            if single_write:
                return self.put_multi(data, single_write=True)
        except Exception:
            raise Exception
        return added_items

    def put_multi_with_buffer(
        self,
        data,
        sort_key=True,
        buff_limit=cf.SIZE_1MB * 512,
        step=10000,
        message="",
        show_progress=True,
    ):
        buff = []
        buff_size = 0

        def update_desc():
            return f"{message} | buffer {buff_size / buff_limit * 100:.0f}%"

        p_bar = None
        if show_progress:
            p_bar = tqdm(total=len(data))

        if sort_key:
            if isinstance(data, dict) or isinstance(data, defaultdict):
                data = sorted(data.items(), key=lambda x: x[0])
            else:
                data.sort(key=lambda x: x[0])

        for i, (k, v) in enumerate(data):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())

            v_db = self.get_multi(k)

            k = str(k).encode(cf.ENCODING)
            if v_db:
                v = set(v) - set(v_db)
            if not v:
                continue
            v = sorted(v)

            for v_int in v:
                v_int = struct.pack("I", v_int)
                buff_size += len(k) + len(v_int)

                buff.append((k, v_int))
                if buff_size >= buff_limit:
                    self.put_multi(buff)
                    buff = []
                    buff_size = 0

        if buff_size:
            self.put_multi(buff)
        if show_progress:
            p_bar.update(len(data) % step)
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def items_dup(self, from_i=0):
        # res = {}
        with self._env.begin(db=self.db) as txn:
            cur = txn.cursor(db=self.db)
            pre_k = None
            v_set = set()
            for i, (k, v) in enumerate(cur.iternext()):
                if i < from_i:
                    continue
                k = k.decode(cf.ENCODING)
                # count = cur.count()
                # if count > 1:
                #     iw.print_status(f"{k}: {count}")
                if pre_k != k:
                    if pre_k is not None:
                        yield pre_k, v_set
                    # res[pre_k] = v_set
                    pre_k = k
                    v_set = set()
                v_set.add(struct.unpack("I", v)[0])
        # return res

    def get_delete_label_len(self, prefix):
        responds = set()
        prefix = f"{prefix}|"
        for k in fdb.get_iter_with_prefix(fdb.db, prefix, get_values=False):
            label_len = k.replace(prefix, "")
            tmp = ul.convert_num(label_len)
            if tmp is not None:
                responds.add(int(tmp))
        responds = sorted(responds)
        return responds

    def get_multi(self, key):
        with self._env.begin(db=self.db) as txn:
            if isinstance(key, list) or isinstance(key, set) or isinstance(key, tuple):
                key_tmp = key
            else:
                key_tmp = [key]

            c = txn.cursor()

            responds = defaultdict(set)
            for k in key_tmp:
                try:
                    if c.set_key(str(k).encode(cf.ENCODING)):
                        responds[k].add(struct.unpack("I", c.value())[0])
                        while c.next_dup():
                            responds[k].add(struct.unpack("I", c.value())[0])
                except Exception:
                    raise Exception

        if isinstance(key, str) or isinstance(key, int):
            responds = responds[key]
        return responds


class DBFuzzy(DBDupInt):
    def __init__(self, lang="en", n_deletes=4, prefix_len=10, min_word_len=1):
        super().__init__(
            db_file=cf.DIR_DB_FUZZY.format(
                lang=lang, n_deletes=n_deletes, prefix_len=prefix_len
            ),
            db_name=b"__db_delLabelLen_labelId__",
            map_size=cf.SIZE_1GB * 500,
        )
        self.lang = lang
        self.n_deletes = n_deletes
        self.prefix_len = prefix_len
        self.min_word_len = min_word_len

    def build(self, n_cpu=1, source="word", from_i=0, to_i=-1, buff_limit=cf.SIZE_1GB):
        if self.lang == "en":
            db = MEntityLabels(cf.DIR_WIKI_LABELS_ENGLISH)
        else:
            db = MEntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL)

        if to_i == -1:
            to_i = db.size_vocab()

        if source == "word":  # ordered dict: word-int, int-word,
            iter_vocab = db.iter_vocab(from_i=from_i, to_i=to_i)
        else:
            iter_vocab = db.iter_vocab_inv(from_i=from_i, to_i=to_i)

        p_bar = tqdm(total=to_i)
        p_bar.update(from_i)
        buff = defaultdict(set)
        buff_size = 0

        # sys.get_size_of(struct.pack("I", 100)) = 37
        # sys.get_size_of(struct.pack("I", 100)) = 28

        def update_desc():
            return (
                f"d{self.n_deletes}|pl{self.prefix_len}|"
                # f"from{from_i}|to{to_i}|"
                f"del2{self.size(len(buff)) / 1000000:.1f}M|"
                f"buff:{buff_size/buff_limit*100:.0f}%"
            )

        if n_cpu == 1:
            for i, (label, label_id) in enumerate(iter_vocab):
                p_bar.update()
                if i and i % 1000 == 0:
                    p_bar.set_description(update_desc())

                if ul.is_wd_item(label):
                    continue

                if len(label) > self.prefix_len:
                    label_pl = label[: self.prefix_len]
                else:
                    label_pl = label

                deletes = ul.delete_edits_prefix(
                    label_pl, self.n_deletes, self.prefix_len
                )

                for delete in deletes:
                    if set(delete) == {" "}:
                        continue
                    key_with_len = f"{delete}|{len(label)}"
                    if key_with_len not in buff:
                        buff_size += len(key_with_len)
                    if label_id not in buff[key_with_len]:
                        buff_size += 37
                    buff[key_with_len].add(label_id)

                if buff_size > buff_limit:
                    self.put_multi_with_buffer(
                        self.db, buff, message=f"{i+from_i}",
                    )
                    buff = defaultdict(set)
                    buff_size = 0
        self.put_multi_with_buffer(self.db, buff)


def copy_to_new_file(n_cpu=1, buff_limit=cf.SIZE_1GB * 2, from_i=0):
    # b"__db_del_labelLen__"
    fdb = DBDupInt(
        db_file=cf.DIR_DB_FUZZY.format(lang="en", n_deletes=4, prefix_len=10) + ".copy",
        db_name=b"__db_delLabelLen_labelId__",
    )
    fdb_new = DBFuzzy(lang="en", n_deletes=4, prefix_len=10, min_word_len=1)

    buff = []
    buff_size = 0
    p_bar = tqdm(total=fdb.size())
    if from_i:
        p_bar.update(from_i)

    def update_desc():
        return (
            f"d{fdb_new.n_deletes}|pl{fdb_new.prefix_len}|"
            # f"from{from_i}|to{to_i}|"
            f"del{fdb_new.size(len(buff)) / 1000000:.1f}M|"
            f"buff:{buff_size / buff_limit * 100:.0f}%"
        )

    if n_cpu == 1:
        for i, (k, v) in enumerate(fdb.items_dup()):
            p_bar.update()
            if i and i % 1000 == 0:
                p_bar.set_description(update_desc())
            buff_size += len(k) + 37 * len(v)
            buff.append((k, v))

            if buff_size > buff_limit:
                fdb_new.put_multi_with_buffer(buff, sort_key=False, message=f"{i}")
                buff = []
                buff_size = 0

        fdb_new.put_multi_with_buffer(buff, sort_key=False)


if __name__ == "__main__":
    # copy_to_new_file(n_cpu=1, from_i=)
    fdb = DBFuzzy(lang="en", n_deletes=4, prefix_len=10, min_word_len=1)
    # fdb.build(n_cpu=1, buff_limit=cf.SIZE_1GB * 10, from_i=14000000)
    prefix = " new e"
    label_len = fdb.get_delete_label_len(prefix)

    # responds = {}
    # for k in fdb.get_iter_with_prefix(fdb.db, prefix, get_values=False):
    #     v = fdb.get_multi(k)
    #     responds[k] = v

    # for k in tqdm(fdb.get_db_iter(fdb.db, get_values=False), total=fdb.size()):
    #     v = fdb.get_multi(k)
    #     debug = 1

    print("Done")
