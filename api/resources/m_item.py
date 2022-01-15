from multiprocessing import Pool

from contextlib import closing
from time import time

import unicodedata

import gzip
import m_config as cf
import numpy as np
import queue
import scipy
import scipy.sparse as sprs
import scipy.spatial
import scipy.sparse.linalg
from api import m_f
from api.resources.m_db_item import DBItem, serialize
from api.resources.m_db_item import (
    serialize_value,
    serialize_key,
)
from api.resources.m_parser_dbpedia import DPItem
from api.resources.m_parser_wikidata import WDItem
from api.resources.m_parser_wikipedia import WPItem
from api.utilities import m_io as iw
from api.utilities import m_utils as ul
from collections import defaultdict, Counter
from datetime import timedelta
from scipy import sparse
from tqdm import tqdm


class MItem(DBItem):
    def __init__(self, db_file=cf.DIR_W_ITEMS):
        super().__init__(db_file=db_file, max_db=19, map_size=cf.SIZE_1GB * 200)
        # QID (str) - item id (int)
        self.db_qid_int = self._env.open_db(b"__qid_int__")
        # item id (int) - QID (str)
        self.db_int_qid = self._env.open_db(b"__int_qid_", integerkey=True)
        self._buff_vocab = defaultdict(int)
        self._len_vocab = self.get_db_size(self.db_int_qid)

        # Mapping ID: item id (int) - mapping item (str)
        self.db_id_wikipedia = self._env.open_db(b"__wikipedia__", integerkey=True)
        self.db_id_dbpedia = self._env.open_db(b"__dbpedia__", integerkey=True)

        # item id (int) - label (str)
        self.db_label = self._env.open_db(b"__label__", integerkey=True)

        # item id (int) - description (str)
        self.db_desc = self._env.open_db(b"__desc__", integerkey=True)

        # item id (int) - aliases_en (set(str)) Compressed
        self.db_aliases_en = self._env.open_db(b"__aliases__", integerkey=True)
        # item id (int) - aliases_multilingual (set(str)) Compressed
        self.db_aliases_all = self._env.open_db(b"__aliases_multi_", integerkey=True)

        # item id (int) - types_specific (list(item id (int))) Numpy array
        self.db_types_dbpedia_direct = self._env.open_db(
            b"__types_specific_dbpedia__", integerkey=True
        )
        # item id (int) - types_specific (list(item id (int))) Numpy array
        self.db_types_dbpedia_all = self._env.open_db(
            b"__types_transitive_dbpedia__", integerkey=True
        )
        # # item id (int) - subclass_of (list(item id (int))) Numpy array
        # self.db_subclass_of = self._env.open_db(b"__subclass_of__", integerkey=True)
        # # item id (int) - part_of (list(item id (int))) Numpy array
        # self.db_part_of = self._env.open_db(b"__part_of__", integerkey=True)

        # Type Inv item id (int) - items (list(item id (int))) Numpy array
        self.db_inv_types_dbpedia_direct = self._env.open_db(
            b"__types_transitive_dbpedia_inv__", integerkey=True
        )

        # item id (int)|property id (int) - items  (list(item id (int))) Numpy array
        self.db_facts_entity = self._env.open_db(b"__claims_entity__")

        # item id (int)|property id (int)|s - string values
        # item id (int)|property id (int)|t - time values
        # item id (int)|property id (int)|q - quantity values
        # item id (int)|property id (int)|m - media values
        # item id (int)|property id (int)|l - links values
        self.db_facts_literal = self._env.open_db(b"__claim_literal__")

        # item id (int) | property name - items  (list(item id (int))) Numpy array
        self.db_facts_entity_others = self._env.open_db(b"__others_entity__")

        # item id (int)|property name|s - string values
        # item id (int)|property name|t - time values
        # item id (int)|property name|q - quantity values
        # item id (int)|property name|m - media values
        # item id (int)|property name|l - link values
        self.db_facts_literal_others = self._env.open_db(b"__others_literal__")

        # Inv item id (int)|property id (int) - items  (list(item id (int))) Numpy array
        self.db_facts_entity_wikidata_inv = self._env.open_db(b"__claims_entity_inv__")
        # Inv item id (int)|property name - items  (list(item id (int))) Numpy array
        self.db_facts_entity_others_inv = self._env.open_db(b"__others_entity_inv__")

        # item id (int) - pagerank score (float)
        self.db_pagerank = self._env.open_db(b"__pagerank__", integerkey=True)

        self.mapper = m_f.m_mapper()

    def get_vocab_id(self, wd_id, is_add=False):
        wd_id_redirect = self.mapper.get_redirect_wikidata(wd_id)
        if wd_id_redirect and wd_id != wd_id_redirect:
            wd_id = wd_id_redirect

        v_id = self._buff_vocab.get(wd_id)
        if v_id is None:
            v_id = self.get_value(self.db_qid_int, wd_id, compress_value=False)

        if is_add and v_id is None:
            v_id = self._len_vocab
            self._buff_vocab[wd_id] = v_id
            self._len_vocab += 1
        return v_id

    def get_vocab_wd_id(self, wd_id):
        v_word = self.get_value(
            self.db_int_qid, wd_id, integerkey=True, compress_value=False
        )
        return v_word

    def save_buff_vocab(self):
        if not self._buff_vocab:
            return
        self.write_bulk_with_buffer(
            self._env, self.db_qid_int, self._buff_vocab, compress_value=False
        )

        # Call inv
        self._buff_vocab = {v: k for k, v in self._buff_vocab.items()}
        self.write_bulk_with_buffer(
            self._env,
            self.db_int_qid,
            self._buff_vocab,
            integerkey=True,
            compress_value=False,
        )
        self._buff_vocab = defaultdict(int)

    def test_iter(self):
        def run_iter(
            db,
            db_name="",
            integerkey=True,
            bytes_value=cf.ToBytesType.OBJ,
            compress_value=False,
        ):
            for k, v in tqdm(
                self.get_db_iter(
                    db,
                    integerkey=integerkey,
                    bytes_value=bytes_value,
                    compress_value=compress_value,
                ),
                total=self.get_db_size(db),
                desc=db_name,
            ):
                debug = 1

        """
        qid_int: 95,054,586/95,054,586 [02:40<00:00, 592,363.38it/s]
        int_qid: 95,054,586/95,054,586 [02:46<00:00, 572,005.02it/s]
        id_wikipedia: 6,571,421/6,571,421 [00:20<00:00, 325,855.91it/s]
        id_dbpedia:  6,448,447/6,448,447 [00:25<00:00, 249,094.38it/s]
        label: 95,051,895/95,051,895 [10:47<00:00, 146,834.14it/s]
        desc: 74,096,181/74,096,181 [07:00<00:00, 176,262.71it/s]
        aliases_en: 95,051,895/95,051,895 [14:41<00:00, 107,804.70it/s]
        aliases_multilingual: 94744942/94744942 [29:17<00:00, 53903.06it/s]
        types_dbpedia_direct: 2849811/2849811 [00:17<00:00, 165498.97it/s]
        types_dbpedia_all: 3282817/3282817 [00:29<00:00, 111336.93it/s]
        db_facts_entity: 332355480/332355480 [23:51<00:00, 232193.73it/s]
        db_facts_entity_others: 31708800/31708800 [02:27<00:00, 215400.25it/s]
        db_facts_literal: 57066516/57066516 [09:22<00:00, 101481.79it/s]
        """

        # run_iter(self.db_qid_int, db_name="qid_int", integerkey=False)
        # run_iter(self.db_int_qid, db_name="int_qid")
        # run_iter(self.db_id_wikipedia, db_name="id_wikipedia")
        # run_iter(self.db_id_dbpedia, db_name="id_dbpedia")
        # run_iter(self.db_label, db_name="label")
        # run_iter(self.db_desc, db_name="desc")
        run_iter(self.db_aliases_en, db_name="aliases_en", compress_value=True)
        run_iter(
            self.db_aliases_all, db_name="aliases_multilingual", compress_value=True
        )
        run_iter(
            self.db_types_dbpedia_direct,
            db_name="types_dbpedia_direct",
            bytes_value=cf.ToBytesType.INT_LIST,
        )
        run_iter(
            self.db_types_dbpedia_all,
            db_name="types_dbpedia_all",
            bytes_value=cf.ToBytesType.INT_LIST,
        )
        run_iter(
            self.db_facts_entity,
            db_name="db_facts_entity",
            integerkey=False,
            bytes_value=cf.ToBytesType.INT_LIST,
        )
        run_iter(
            self.db_facts_entity_others,
            db_name="db_facts_entity_others",
            integerkey=False,
            bytes_value=cf.ToBytesType.INT_LIST,
        )

        run_iter(self.db_facts_literal, db_name="db_facts_literal", integerkey=False)
        run_iter(
            self.db_facts_literal_others, db_name="db_facts_literal", integerkey=False
        )

    def get_item(
        self,
        wikidata_id,
        wikipedia_title=True,
        dbpedia_title=True,
        label=True,
        description=True,
        aliases=False,
        aliases_multilingual=False,
        instance_of=True,
        all_types=False,
        entity_facts=False,
        literal_facts=False,
        pagerank=True,
        dbpedia_types=False,
        entity_facts_others=False,
        literal_facts_others=False,
        inv_entity_facts=False,
        inv_entity_facts_others=False,
    ):
        if not isinstance(wikidata_id, int):
            wikidata_id = self.get_vocab_id(wikidata_id)

        if wikidata_id is None:
            return None
        responds = dict()
        if wikipedia_title:
            responds["wikipedia_title"] = self.get_wikipedia_title(wikidata_id)
        if dbpedia_title:
            responds["dbpedia_title"] = self.get_dbpedia_title(wikidata_id)
        if label:
            responds["label"] = self.get_label(wikidata_id)
        if description:
            responds["description"] = self.get_description(wikidata_id)
        if aliases:
            responds["aliases"] = self.get_aliases(wikidata_id)
        if aliases_multilingual:
            responds["aliases_multilingual"] = self.get_aliases_multilingual(
                wikidata_id
            )
        if instance_of:
            responds["instance_of"] = self.get_instance_of(wikidata_id)
        if all_types:
            responds["all_types"] = self.get_types_all_wikidata(wikidata_id)
        if dbpedia_types:
            responds["dbpedia_types"] = self.get_types_specific_dbpedia(wikidata_id)
        # responds["types"] = self.get_types_all(wd_id)
        if entity_facts:
            responds["entity_facts"] = self.get_entRel_entTail_wikidata(wikidata_id)
        if literal_facts:
            responds["literal_facts"] = self.get_litRel_litTail_wikidata(wikidata_id)
        if entity_facts_others:
            responds["entity_facts_others"] = self.get_entRel_entTail_others(
                wikidata_id
            )
        if literal_facts_others:
            responds["literal_facts_others"] = self.get_litRel_litTail_others(
                wikidata_id
            )
        if pagerank:
            responds["pagerank"] = self.get_pagerank_score(wikidata_id)
        if inv_entity_facts:
            responds["inv_entity_facts"] = self.get_entRel_entHead_wikidata(wikidata_id)
        if inv_entity_facts_others:
            responds["inv_entity_facts_others"] = self.get_entRel_entHead_others(
                wikidata_id
            )
        return responds

    def get_labels(self, wd_id, multilingual=False):
        # return self.get_value(self._db_aliases, wd_id)

        responds = set()
        tmp = self.get_aliases(wd_id)
        if tmp:
            responds.update(set(tmp))
        if multilingual:
            tmp = self.get_aliases_multilingual(wd_id)
            if tmp:
                responds.update(set(tmp))

        # remove unicode text
        new_responds = set()
        for respond in responds:
            if respond:
                new_respond = "".join(
                    filter(lambda c: unicodedata.category(c) != "Cf", respond)
                )
                # new_respond = "".join(filter(lambda x: x in string.printable, respond))
                # new_respond = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", respond)
                # new_respond = (
                #     respond.encode("ascii", errors="ignore")
                #     .strip()
                #     .decode("unicode_escape")
                # )
                if new_respond:
                    new_responds.add(new_respond)
        return new_responds

    def get_statement_values(self, wd_id, multilingual=False):
        statements = {
            "num": defaultdict(set),
            "text": defaultdict(set),
            "entity": defaultdict(set),
        }

        claims_literal = self.get_facts_literal(wd_id)
        if claims_literal:
            for prop, values in claims_literal["quantity"].items():
                for value_text in values:
                    if isinstance(value_text, tuple) or isinstance(value_text, list):
                        value_text = value_text[0]
                    statements["num"][value_text].add(prop)

            text_statements = {
                **claims_literal["string"],
                **claims_literal["time"],
            }
            for prop, values in text_statements.items():
                for value_text in values:
                    if isinstance(value_text, tuple) or isinstance(value_text, list):
                        value_text = value_text[0]
                    statements["text"][value_text].add(prop)

        def update_entity_label(claims_wd):
            if claims_wd:
                for prop, wd_id_set in claims_wd.items():
                    if "Section: " in prop:
                        continue
                    for e_id in wd_id_set:
                        # if get_labels:
                        #     e_label = self.get_labels(e_id, default_return="")
                        # else:
                        #     e_label = ""
                        tmp_labels = self.get_labels(e_id, multilingual=multilingual)
                        if not tmp_labels:
                            continue
                        for e_label in tmp_labels:
                            statements["entity"][e_label].add((prop, e_id))

        update_entity_label(self.get_tail_entity(wd_id))
        update_entity_label(self.get_facts_entities_others(wd_id))

        # # add P31 type of
        # claim_p31p279 = self.get_types_all(wd_id)
        # if claim_p31p279:
        #     for e_id in claim_p31p279:
        #         e_label = self.get_label(e_id)
        #         if not e_label:
        #             e_label = ""
        #         statements["entity"][e_label].add(("P31", e_id))
        #
        # # add P361 part of
        # claim_p361 = self.get_part_of_all(wd_id)
        # for e_id in claim_p361:
        #     e_label = self.get_label(e_id)
        #
        #     if not e_label:
        #         e_label = ""
        #     statements["entity"][e_label].add(("P361", e_id))
        return statements

    def get_entity_info(
        self, wd_id, get_wp=True, get_dp=True, get_label=True, get_desc=True
    ):
        res = defaultdict()
        res["wikidata"] = cf.WD + wd_id if wd_id[0] == "Q" else cf.WDT + wd_id
        if get_wp:
            wp = self.get_wikipedia_title(wd_id)
            if wp:
                res["wikipedia"] = cf.WIKI_EN + wp.replace(" ", "_")
        if get_dp:
            dp = self.get_dbpedia_title(wd_id)
            if dp:
                res["dbpedia"] = cf.DBR + dp.replace(" ", "_")
        if get_label:
            res["label"] = self.get_label(wd_id)
        if get_desc:
            res["desc"] = self.get_description(wd_id)
        return res

    def get_search_info(self, responds, get_info=True):
        responds_info = []
        for r, r_s in responds:
            responds_obj = defaultdict()
            responds_obj["id"] = r
            responds_obj["score"] = r_s
            if get_info:
                responds_obj["label"] = self.get_label(r)
                responds_obj["description"] = self.get_description(r)
                responds_obj["wikidata"] = cf.WD + r if r[0] == "Q" else cf.WDT + r

                wp = self.get_wikipedia_title(r)
                if wp:
                    responds_obj["wikipedia"] = cf.WIKI_EN + wp.replace(" ", "_")

                dp = self.get_dbpedia_title(r)
                if dp:
                    responds_obj["dbpedia"] = cf.DBR + dp.replace(" ", "_")
            responds_info.append(responds_obj)
        return responds_info

    def keys(self):
        return self.get_db_iter(self.db_qid_int, get_values=False)

    def keys_lid(self):
        return self.get_db_iter(self.db_int_qid, get_values=False, integerkey=True)

    def size(self):
        return self.get_db_size(self.db_qid_int)

    def items(self):
        for qid, lid in self.get_db_iter(self.db_qid_int):
            yield qid, self.get_item(lid)

    def get_wikipedia_title(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
        if wd_id is None:
            return None
        return self.get_value(
            self.db_id_wikipedia, wd_id, compress_value=False, integerkey=True
        )

    def get_dbpedia_title(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
        if wd_id is None:
            return None
        return self.get_value(
            self.db_id_dbpedia, wd_id, compress_value=False, integerkey=True
        )

    def get_label(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
        if wd_id is None:
            return None
        respond = self.get_value(
            self.db_label, wd_id, compress_value=False, integerkey=True
        )
        # if "\u200e" in respond:
        #     debug = 1
        if respond and isinstance(respond, str):
            respond = "".join(
                filter(lambda c: unicodedata.category(c) != "Cf", respond)
            )
            # respond = "".join(filter(lambda x: x in string.printable, respond))
            # respond = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", respond)
            # respond = (
            #     respond.encode("ascii", errors="ignore")
            #     .strip()
            #     .decode("unicode_escape")
            # )
        return respond

    def get_description(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
        if wd_id is None:
            return None
        return self.get_value(
            self.db_desc, wd_id, compress_value=False, integerkey=True
        )

    def get_set_of_values(self, db, wd_id, compress_value=True, integerkey=True):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
        if wd_id is None:
            return None
        respond = self.get_value(
            db, wd_id, compress_value=compress_value, integerkey=integerkey
        )
        if not respond:
            return None
        return set(respond)

    def get_aliases(self, wd_id):
        return self.get_set_of_values(
            self.db_aliases_en, wd_id, compress_value=True, integerkey=True
        )

    def get_aliases_multilingual(self, wd_id):
        return self.get_set_of_values(
            self.db_aliases_all, wd_id, compress_value=True, integerkey=True
        )

    def get_tail_obj_with_relation(
        self, db, wd_id, property_id, datatype="a", get_label=False
    ):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
            if wd_id is None:
                return None

        if not isinstance(property_id, int):
            property_id = self.get_vocab_id(property_id)
            if property_id is None:
                return None

        key = f"{wd_id}|{property_id}"
        if db == self.db_facts_entity or db == self.db_facts_entity_others:
            responds_id = self.get_value(db, key, bytes_value=cf.ToBytesType.INT_LIST)
            if responds_id:
                responds_qid = list(
                    self.get_value(
                        self.db_int_qid,
                        responds_id,
                        integerkey=True,
                        compress_value=False,
                    ).values()
                )

                if get_label:
                    responds_id = {qid: self.get_label(qid) for qid in responds_qid}
                else:
                    responds_id = responds_qid
        else:
            # get all datatype
            if datatype == "a":
                responds_id = {
                    "string": self.get_value(db, key + f"|s", compress_value=False),
                    "time": self.get_value(db, key + f"|t", compress_value=False),
                    "quantity": self.get_value(db, key + f"|q", compress_value=False),
                    "media": self.get_value(db, key + f"|m", compress_value=False),
                    "links": self.get_value(db, key + f"|l", compress_value=False),
                }
                if not any(v for v in responds_id.values() if v):
                    responds_id = None
            else:
                responds_id = self.get_value(
                    db, key + f"|{datatype}", compress_value=False
                )
                if not responds_id:
                    responds_id = None

        return responds_id

    # instance of
    def get_instance_of(self, wd_id, get_label=False):
        return self.get_tail_obj_with_relation(
            self.db_facts_entity, wd_id, "P31", get_label=get_label
        )

    # subclass of
    def get_subclass_of(self, wd_id, get_label=False):
        return self.get_tail_obj_with_relation(
            self.db_facts_entity, wd_id, "P279", get_label=get_label
        )

    # part of
    def get_part_of(self, wd_id, get_label=False):
        return self.get_tail_obj_with_relation(
            self.db_facts_entity, wd_id, "P361", get_label=get_label
        )

    def get_objRel_objTail(
        self, db, wd_id, get_tail=True, output_pid=True, output_qid=True
    ):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
            if wd_id is None:
                return None

        # Get all relevant key with wd_id
        map_attr = {
            "s": "string",
            "t": "time",
            "q": "quantity",
            "m": "media",
            "l": "links",
        }
        if get_tail:
            if db in [
                self.db_facts_entity,
                self.db_facts_entity_others,
                self.db_facts_entity_wikidata_inv,
                self.db_facts_entity_others_inv,
            ]:
                responds = {}
            else:
                responds = {
                    "string": dict(),
                    "time": dict(),
                    "quantity": dict(),
                    "media": dict(),
                    "links": dict(),
                }
        else:
            responds = []

        if db in [
            self.db_facts_entity,
            self.db_facts_entity_others,
            self.db_facts_entity_wikidata_inv,
            self.db_facts_entity_others_inv,
        ]:
            bytes_value = cf.ToBytesType.INT_LIST
        else:
            bytes_value = cf.ToBytesType.OBJ

        for respond in self.get_iter_with_prefix(
            db,
            f"{wd_id}|",
            bytes_value=bytes_value,
            get_values=get_tail,
            compress_value=False,
        ):
            if get_tail:
                key, value = respond
            else:
                key, value = respond, None
            if not key:
                continue
            prop = key.split("|")[1]
            if db in [
                self.db_facts_entity,
                self.db_facts_literal,
                self.db_facts_entity_wikidata_inv,
            ]:
                prop = int(prop)
                if output_pid:
                    prop = self.get_value(
                        self.db_int_qid, prop, integerkey=True, compress_value=False
                    )

            if get_tail:
                if db in [
                    self.db_facts_entity,
                    self.db_facts_entity_others,
                    self.db_facts_entity_wikidata_inv,
                    self.db_facts_entity_others_inv,
                ]:
                    if output_qid:
                        value = list(
                            self.get_value(
                                self.db_int_qid,
                                value,
                                integerkey=True,
                                compress_value=False,
                            ).values()
                        )
                    responds[prop] = value
                else:
                    responds[map_attr.get(key.split("|")[2])][prop] = value
            else:
                responds.append(prop)
        return responds

    def get_entRel_entTail_wikidata(self, wd_id, output_pid=True, output_qid=True):
        return self.get_objRel_objTail(
            self.db_facts_entity,
            wd_id,
            get_tail=True,
            output_pid=output_pid,
            output_qid=output_qid,
        )

    def get_entRel_entHead_wikidata(self, wd_id, output_pid=True, output_qid=True):
        return self.get_objRel_objTail(
            self.db_facts_entity_wikidata_inv,
            wd_id,
            get_tail=True,
            output_pid=output_pid,
            output_qid=output_qid,
        )

    def get_entRel_entTail_others(self, wd_id, output_pid=True, output_qid=True):
        return self.get_objRel_objTail(
            self.db_facts_entity_others,
            wd_id,
            get_tail=True,
            output_pid=output_pid,
            output_qid=output_qid,
        )

    def get_entRel_entHead_others(self, wd_id, output_pid=True, output_qid=True):
        return self.get_objRel_objTail(
            self.db_facts_entity_others_inv,
            wd_id,
            get_tail=True,
            output_pid=output_pid,
            output_qid=output_qid,
        )

    def get_entRel_wikidata(self, wd_id, output_pid=True):
        return self.get_objRel_objTail(
            self.db_facts_entity, wd_id, get_tail=False, output_pid=output_pid
        )

    def get_entRel_others(self, wd_id):
        return self.get_objRel_objTail(
            self.db_facts_entity_others, wd_id, get_tail=False
        )

    def get_litRel_litTail_wikidata(self, wd_id, output_pid=True):
        return self.get_objRel_objTail(
            self.db_facts_literal, wd_id, get_tail=True, output_pid=output_pid
        )

    def get_litRel_litTail_others(self, wd_id):
        return self.get_objRel_objTail(
            self.db_facts_literal_others, wd_id, get_tail=True
        )

    def get_litRel_wikidata(self, wd_id, output_pid=True):
        return self.get_objRel_objTail(
            self.db_facts_literal, wd_id, get_tail=False, output_pid=output_pid
        )

    def get_litRel_others(self, wd_id):
        return self.get_objRel_objTail(
            self.db_facts_literal_others, wd_id, get_tail=False
        )

    def get_relations_wikidata(self, wd_id, output_pid=True):
        responds = []
        rel_ent = self.get_entRel_wikidata(wd_id, output_pid=output_pid)
        if rel_ent:
            responds.extend(rel_ent)
        rel_lit = self.get_litRel_wikidata(wd_id, output_pid=output_pid)
        if rel_lit:
            responds.extend(rel_lit)

        responds = list(dict.fromkeys(responds))
        return responds

    def get_relations_others(self, wd_id):
        responds = []
        rel_ent = self.get_entRel_others(wd_id)
        if rel_ent:
            responds.extend(rel_ent)
        rel_lit = self.get_litRel_others(wd_id)
        if rel_lit:
            responds.extend(rel_lit)

        responds = list(dict.fromkeys(responds))
        return responds

    @staticmethod
    def _get_p_recursive_2(wd_id, get_p_func_1, get_p_func_2):
        all_p = set()
        p_items = get_p_func_1(wd_id)
        if p_items and len(p_items):
            process_queue = queue.Queue()
            for p_item in p_items:
                process_queue.put(p_item)
            while process_queue.qsize():
                process_wd = process_queue.get()
                all_p.add(process_wd)
                p_items = get_p_func_2(process_wd)
                if p_items and len(p_items):
                    for item in p_items:
                        if item not in all_p:
                            process_queue.put(item)
        return all_p

    def get_types_all_wikidata(self, wd_id, get_label=False):
        all_types = self._get_p_recursive_2(
            wd_id, self.get_instance_of, self.get_subclass_of
        )
        all_types = [t for t in all_types if t not in cf.WD_TOP]
        if get_label:
            all_types = {t: self.get_label(t) for t in all_types}
        return all_types

    def get_types_all(self, wd_id, get_label=False):
        types = []
        types_wikidata = self.get_types_all_wikidata(wd_id)
        if types_wikidata:
            types.extend(types_wikidata)
        types_dbpedia = self.get_types_specific_dbpedia(wd_id)
        if types_dbpedia:
            types.extend(types_dbpedia)

        types = list(dict.fromkeys(types))
        if get_label:
            types = {t: self.get_label(t) for t in types}
        return types

    def get_tail_entity(self, wd_id, get_values=True):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
            if wd_id is None:
                return None

        # Get all relevant key with wd_id
        if get_values:
            responds = {}
        else:
            responds = []

        for respond in self.get_iter_with_prefix(
            self.db_facts_entity,
            f"{wd_id}|",
            bytes_value=cf.ToBytesType.INT_LIST,
            get_values=get_values,
        ):
            if get_values:
                key, value = respond
                key = int(key.split("|")[-1])
                key = self.get_value(
                    self.db_int_qid, key, integerkey=True, compress_value=False
                )
                value = self.get_value(
                    self.db_int_qid, value, integerkey=True, compress_value=False
                )
                responds[key] = list(value.values())
            else:
                key = int(respond.split("|")[-1])
                key = self.get_value(
                    self.db_int_qid, key, integerkey=True, compress_value=False
                )
                responds.append(key)
        return responds

    def get_facts_literal(self, wd_id):
        return self.get_value(self.db_facts_literal, wd_id)

    def get_facts_entities_others(self, wd_id):
        return self.get_value(self.db_facts_entity_others, wd_id)

    # def get_subclass_of_specific(self, wd_id):
    #     return self.get_value(self.db_subclass_of, wd_id)

    def get_pagerank_score(self, wd_id):
        score = 0
        if wd_id is None:
            return score

        if isinstance(wd_id, list) or isinstance(wd_id, set):
            wd_id = list(wd_id)
            wd_id.sort()
            if not isinstance(wd_id[0], int):
                wd_id = [self.get_vocab_id(i) for i in wd_id]
        else:
            if not isinstance(wd_id, int):
                wd_id = self.get_vocab_id(wd_id)

        pagerank = self.get_value(
            self.db_pagerank, wd_id, compress_value=False, integerkey=True
        )
        if pagerank:
            score = pagerank
        return score

    @staticmethod
    def _get_p_recursive(wd_id, get_p_func):
        all_p = set()
        p_items = get_p_func(wd_id)
        if p_items and len(p_items):
            process_queue = queue.Queue()
            for p_item in p_items:
                process_queue.put(p_item)
            while process_queue.qsize():
                process_wd = process_queue.get()
                all_p.add(process_wd)
                p_items = get_p_func(process_wd)
                if p_items and len(p_items):
                    for item in p_items:
                        if item not in all_p:
                            process_queue.put(item)
        return all_p

    def get_p361_all(self, wd_id):
        return self._get_p_recursive(wd_id, self.get_part_of)

    def get_p279_all(self, wd_id):
        return self._get_p_recursive(wd_id, self.get_subclass_of)

    def get_p279_distance(self, wd_id, cursor_distance=0, max_distance=5):
        if cursor_distance == max_distance:
            return Counter()
        responds = self.get_subclass_of(wd_id)
        if responds:
            responds = {
                respond: cursor_distance + 1 for respond in responds
            }  # if respond not in st.WD_TOP}
        return responds

    def get_p279_all_distances(self, wd_id):
        # return self._get_attribute(self._db_p279, wd_id)
        parents = Counter()
        cursor = self.get_p279_distance(wd_id, cursor_distance=0)
        while cursor:
            for respond, score in cursor.items():
                if not parents.get(respond):
                    parents[respond] = score
                else:
                    parents[respond] = min(parents[respond], score)

            tmp_cursor = Counter()
            for tmp_id, score1 in cursor.items():
                tmp_p279 = self.get_p279_distance(tmp_id, cursor_distance=score1)
                if tmp_p279:
                    for respond, score2 in tmp_p279.items():

                        if not tmp_cursor.get(respond) and not parents.get(respond):
                            tmp_cursor[respond] = score2

                        if parents.get(respond):
                            parents[respond] = min(parents[respond], score2)

                        if tmp_cursor.get(respond):
                            tmp_cursor[respond] = min(tmp_cursor[respond], score2)
            cursor = tmp_cursor
        return parents

    def get_lowest_types(self, type_id_list):
        if len(type_id_list) == 1:
            return type_id_list
        lowest_type = set()
        parents_distance = defaultdict()
        for a_type in type_id_list:
            if any(True for parents in parents_distance.values() if a_type in parents):
                continue
            parents_types = self.get_p279_all_distances(a_type)
            parents_distance[a_type] = parents_types
            if lowest_type:
                is_add = True
                if parents_types:
                    for parent_type in parents_types:
                        if parent_type in lowest_type:
                            lowest_type.remove(parent_type)
                            lowest_type.add(a_type)
                            is_add = False
                if is_add:
                    lowest_type.add(a_type)
                # if any(
                #     a_type in parents_distance[_a_type].keys()
                #     for _a_type in lowest_type
                # ):
                #     lowest_type.add(a_type)
            else:
                lowest_type.add(a_type)

        # if len(lowest_type) == 1:
        #     lowest_type = list(lowest_type)[0]
        # else:
        #     # lowest_type = {c_type: f.wikidata_info().get_popularity(c_type) for c_type in lowest_type}
        #     # lowest_type = sorted(lowest_type.items(), key=lambda x: x[1], reverse=True)
        #     # lowest_type = lowest_type[0][0]
        #     # else:
        #     #     cta_final = c_types_direct_dup[0]
        #     #
        #     if "Q20181813" in lowest_type:
        #         lowest_type = "Q20181813"
        #     elif "Q3624078" in lowest_type:
        #         lowest_type = "Q3624078"
        #     else:
        #         try:
        #             # Get common parents:
        #             parents_common = Counter()
        #             score_common = defaultdict(int)
        #             for a_type in lowest_type:
        #                 for parents, distance in parents_distance[a_type].items():
        #                     parents_common[parents] += 1
        #                     score_common[parents] += distance
        #             parents_common_max = max(parents_common.values())
        #             parents_common = [
        #                 [a_type, score_common[a_type]]
        #                 for a_type, count in parents_common.items()
        #                 if count == parents_common_max
        #             ]
        #             if len(parents_common):
        #                 parents_common.sort(key=lambda x: x[1])
        #                 if parents_common[0][1] * 1.0 / parents_common_max < 3:
        #                     lowest_type = parents_common[0][0]
        #         except Exception as message:
        #             iw.print_status(message)
        #
        # if not isinstance(lowest_type, str):
        #     # Get oldest id
        #     # lowest_type = ul.get_oldest(list(lowest_type))
        #     lowest_type = None

        return list(lowest_type)

    @staticmethod
    def _get_items_recursive(wd_id, get_p_func):
        all_p = set()
        p_items = get_p_func(wd_id)
        if p_items and len(p_items):
            process_queue = queue.Queue()
            for p_item in p_items:
                process_queue.put(p_item)
            while process_queue.qsize():
                process_wd = process_queue.get()
                all_p.add(process_wd)
                p_items = get_p_func(process_wd)
                if p_items and len(p_items):
                    for item in p_items:
                        if item not in all_p:
                            process_queue.put(item)
        return all_p

    def get_qid_items(self, db, wd_id, get_label=False):
        if not isinstance(wd_id, int):
            wd_id = self.get_vocab_id(wd_id)
            if wd_id is None:
                return None

        responds_id = self.get_value(db, wd_id, bytes_value=cf.ToBytesType.INT_LIST)
        if responds_id is None:
            return None

        responds_qid = self.get_value(
            self.db_int_qid, responds_id, integerkey=True, compress_value=False
        )
        if get_label and responds_qid:
            responds = {qid: self.get_label(lid) for lid, qid in responds_qid.items()}
            return responds
        return None

    def get_types_specific_dbpedia(self, wd_id, get_label=False):
        return self.get_qid_items(
            self.db_types_dbpedia_direct, wd_id, get_label=get_label
        )

    def get_part_of_specific(self, wd_id, get_label=False):
        return self.get_qid_items(self.db_part_of, wd_id, get_label=get_label)

    def get_part_of_all(self, wd_id, is_label=False):
        responds = self._get_items_recursive(wd_id, self.get_part_of_specific)
        if is_label and responds:
            responds = {t: self.get_label(t) for t in responds}
        return responds

    def get_wd_outlinks(self, wd_id, is_label=False):
        responds = defaultdict(int)
        # All Wikidata claims
        wd_claims = self.get_entRel_entTail_wikidata(
            wd_id, output_pid=False, output_qid=False
        )
        if wd_claims:
            for _, wd_values in wd_claims.items():
                for wd_value in wd_values:
                    # if not responds.get(wd_value):
                    responds[wd_value] += cf.WEIGHT_WD

        # Other claims
        other_claims = self.get_entRel_entTail_others(
            wd_id, output_pid=False, output_qid=False
        )
        if other_claims:
            for wd_prop, wd_values in other_claims.items():
                weight_prop = cf.WEIGHT_WD
                if "Section" in wd_prop:
                    weight_prop = cf.WEIGHT_W_OTHERS
                for wd_value in wd_values:
                    if not responds.get(wd_value):
                        responds[wd_value] += weight_prop

        clean_responds = defaultdict()
        for wd_i, wd_weight in responds.items():
            wd_i_qid = self.get_vocab_wd_id(wd_i)
            wd_i_redirect = self.mapper.get_redirect_wikidata(wd_i_qid)
            if wd_i_redirect and wd_i_redirect != wd_i:
                wd_i = self.get_vocab_id(wd_i_redirect)

            wd_types = self.get_instance_of(wd_i)
            # Wikimedia disambiguation page
            if wd_types and "Q4167410" in wd_types:
                continue
            clean_responds[wd_i] = wd_weight
        responds = sorted(clean_responds.items(), key=lambda x: x[0])
        if is_label and responds:
            responds = {t: self.get_label(t) for t in responds}
        return responds

    def _build_from_dumps(self, buff_size=cf.SIZE_1MB * 64, step=1000):
        db_wikidata_json = WDItem(cf.DIR_WIKIDATA_ITEMS_JSON)
        # wd_items_nt = WDItem(cf.DIR_WD_ITEMS_NT)
        db_wikipedia = WPItem()
        db_dbpedia = DPItem()

        buff_id_wikipedia = []
        buff_id_wikipedia_size = 0

        buff_id_dbpedia = []
        buff_id_dbpedia_size = 0

        buff_label = []
        buff_label_size = 0

        buff_desc = []
        buff_desc_size = 0

        buff_aliases = []
        buff_aliases_size = 0

        buff_aliases_multi = []
        buff_aliases_multi_size = 0

        buff_types_specific_dbpedia = []
        buff_types_specific_dbpedia_size = 0

        buff_types_transitive_dbpedia = []
        buff_types_transitive_dbpedia_size = 0

        buff_facts_entity_wikidata = []
        buff_facts_entity_wikidata_size = 0

        buff_facts_literal_wikidata = []
        buff_facts_literal_wikidata_size = 0

        buff_facts_entity_others = []
        buff_facts_entity_others_size = 0

        buff_facts_literal_others = []
        buff_facts_literal_others_size = 0

        update_item = 0

        def update_desc():
            return f"Updated {update_item:,}"

        # WikiGraph = Wikidata + (DBpedia, Wikipedia)
        p_bar = tqdm(total=db_wikidata_json.size(), desc=update_desc())
        for c, (n_qid_wikidata, obj_wikidata) in enumerate(db_wikidata_json.items()):
            if c and c % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
            # if n_qid_wikidata[0] != "Q":
            #     continue
            # Key Wikidata ID
            if not ul.is_wd_item(n_qid_wikidata):
                continue

            # 1. Init value
            n_id_wikidata = self.get_vocab_id(n_qid_wikidata, is_add=True)
            skey_id = serialize_key(n_id_wikidata, integerkey=True)

            obj_dbpedia, obj_wikipedia = None, None

            # 2. Value
            # 2.1 Mapping ID
            # 2.1.1 Wikidata ID

            # # 2.1.2 Wikipedia ID
            n_id_wikipedia = self.mapper.get_wikipedia_from_wikidata(n_qid_wikidata)
            if n_id_wikipedia:
                if (
                    self.mapper.get_wikidata_from_wikipedia(n_id_wikipedia)
                    == n_qid_wikidata
                ):
                    svalue_id_wikipedia = serialize_value(
                        n_id_wikipedia, compress_value=False
                    )
                    buff_id_wikipedia_size += len(svalue_id_wikipedia)
                    buff_id_wikipedia.append((skey_id, svalue_id_wikipedia))

                    # Get Wikipedia page
                    obj_wikipedia = db_wikipedia.get_item(n_id_wikipedia)
                else:
                    n_id_wikipedia, obj_wikipedia = None, None

            # 2.1.3 DBpedia ID
            n_id_dbpedia = self.mapper.get_dbpedia_from_wikidata(n_qid_wikidata)
            if n_id_dbpedia and not ul.is_wd_item(n_id_dbpedia):
                if (
                    self.mapper.get_wikidata_from_dbpedia(n_id_dbpedia)
                    == n_qid_wikidata
                ):
                    svalue_id_dbpedia = serialize_value(
                        n_id_dbpedia, compress_value=False
                    )
                    buff_id_dbpedia_size += len(svalue_id_dbpedia)
                    buff_id_dbpedia.append((skey_id, svalue_id_dbpedia))

                    # Get DBpedia page
                    obj_dbpedia = db_dbpedia.get_item(n_id_dbpedia)
                else:
                    n_id_dbpedia, obj_dbpedia = None, None

            # Fix wrong mapping from wikidata to dbpedia and wikipedia
            # if n_id_wikipedia or n_id_dbpedia:
            #     re_map_wikipedia = self.mapper.get_wikidata_from_wikipedia(
            #         n_id_wikipedia
            #     )
            #     re_map_dbpedia = self.mapper.get_wikidata_from_dbpedia(n_id_dbpedia)
            # else:
            #     continue
            # if not (
            #     (n_id_wikipedia and re_map_wikipedia != n_qid_wikidata)
            #     or (n_id_dbpedia and re_map_dbpedia != n_qid_wikidata)
            # ):
            #     continue
            # self.delete(self.db_id_wikipedia, n_id_wikidata, integerkey=True)
            # self.delete(self.db_id_dbpedia, n_id_wikidata, integerkey=True)
            # self.delete(self.db_label, n_id_wikidata, integerkey=True)
            # self.delete(self.db_desc, n_id_wikidata, integerkey=True)
            # self.delete(self.db_aliases_en, n_id_wikidata, integerkey=True)
            # self.delete(self.db_aliases_all, n_id_wikidata, integerkey=True)
            # self.delete(self.db_types_dbpedia_direct, n_id_wikidata, integerkey=True)
            # self.delete(self.db_types_dbpedia_all, n_id_wikidata, integerkey=True)
            # self.delete(self.db_facts_entity, f"{n_id_wikidata}|", with_prefix=True)
            # self.delete(self.db_facts_literal, f"{n_id_wikidata}|", with_prefix=True)
            # self.delete(
            #     self.db_facts_entity_others, f"{n_id_wikidata}|", with_prefix=True
            # )
            # self.delete(
            #     self.db_facts_literal_others, f"{n_id_wikidata}|", with_prefix=True
            # )

            # n_id_wikipedia, n_id_dbpedia = None, None
            # obj_dbpedia, obj_wikipedia = None, None
            # update_item += 1

            # 2.2. Aliases and Label
            # 2.2.1. Wikidata n_aliases
            n_aliases = set()
            n_aliases_multilingual = set()
            # 2.2.2. Wikidata label
            n_label = obj_wikidata["labels"]
            if n_label:
                n_aliases.add(n_label)

            # Wikidata n_aliases
            if obj_wikidata.get("aliases"):
                n_aliases.update(set(obj_wikidata["aliases"]))

            if obj_wikidata.get("labels_multi_lang"):
                n_aliases_multilingual.update(set(obj_wikidata["labels_multi_lang"]))

            if obj_dbpedia and obj_dbpedia.get("aliases_multilingual"):
                n_aliases_multilingual.update(set(obj_dbpedia["aliases_multilingual"]))

            # wd_obj_nt = wd_items_nt.get_item(n_qid_wikidata)
            # if wd_obj_nt:
            #     n_label = wd_obj_nt["labels"]
            #     if n_label:
            #         n_aliases.add(n_label)
            #     if wd_obj_nt["aliases"]:
            #         for alias in wd_obj_nt["aliases"]:
            #             n_aliases.add(alias)
            #

            # # 2.2.3. Wikidata descriptions size < 511
            if obj_wikidata["descriptions"]:
                n_desc = obj_wikidata["descriptions"]
                svalue_desc = serialize_value(n_desc, compress_value=False)
                buff_desc_size += len(skey_id) + len(svalue_desc)
                buff_desc.append((skey_id, svalue_desc))
                # n_aliases.add(obj_wikidata["descriptions"][:cf.LMDB_MAX_KEY])

            # 2.2.4. Wikipedia title and it's redirects
            if n_id_wikipedia:
                n_aliases.add(n_id_wikipedia)
                # redirect of Wikipedia page
                wp_redirect_of = self.mapper.get_redirect_of_wikipedia(n_id_wikipedia)
                if wp_redirect_of:
                    n_aliases.update(set(wp_redirect_of))

            # 2.2.5. DBpedia title and it's redirects and some special property
            if n_id_dbpedia:
                n_aliases.add(n_id_dbpedia)
                # redirect of DBpedia page
                dp_redirect_of = self.mapper.get_redirect_of_dbpedia(n_id_dbpedia)
                if dp_redirect_of:
                    n_aliases.update(set(dp_redirect_of))

                if obj_dbpedia:
                    if obj_dbpedia.get("label"):
                        n_aliases.add(obj_dbpedia["label"])
                    if obj_dbpedia.get("aliases_en"):
                        n_aliases.update(obj_dbpedia["aliases_en"])

            if (not n_label or ul.is_wd_item(n_label)) and len(n_aliases):
                n_label = next(iter(n_aliases))
            if not n_label:
                n_label = n_qid_wikidata

            svalue_wd_label = serialize_value(n_label, compress_value=False)
            buff_label_size += len(svalue_wd_label)
            buff_label.append((skey_id, svalue_wd_label))

            if n_aliases:
                # remove wd id
                wd_aliases_dump = set()
                for wd_alias in n_aliases:
                    if not ul.is_wd_item(wd_alias):
                        if not ul.isEnglish(wd_alias):
                            n_aliases_multilingual.add(wd_alias)
                        wd_aliases_dump.add(wd_alias)

                n_aliases = wd_aliases_dump
                svalue_aliases = serialize_value(n_aliases)
                buff_aliases_size += len(svalue_aliases)
                buff_aliases.append((skey_id, svalue_aliases))

            if n_aliases_multilingual:
                svalue_aliases_multilingual = serialize_value(n_aliases_multilingual)
                buff_aliases_multi_size += len(svalue_aliases_multilingual)
                buff_aliases_multi.append((skey_id, svalue_aliases_multilingual))

            # 3. Types
            def parse_dbpedia_types(att, buff):
                tmp_n_types = set()
                tmp_buff_size = 0
                if obj_dbpedia and obj_dbpedia.get(att):
                    # Mapping DBpedia type to Wikidata
                    for dp_types_specific in obj_dbpedia[att]:
                        map_wd_dp_types_specific = self.mapper.get_wikidata_from_dbpedia_class(
                            dp_types_specific
                        )
                        if map_wd_dp_types_specific:
                            tmp_n_types.add(map_wd_dp_types_specific)

                if tmp_n_types:
                    tmp_n_types = {
                        self.get_vocab_id(i, is_add=True) for i in tmp_n_types
                    }
                    tmp_n_types = sorted(list(tmp_n_types))
                    svalue_types_specific = serialize_value(
                        tmp_n_types, bytes_value=cf.ToBytesType.INT_LIST
                    )
                    tmp_buff_size += len(svalue_types_specific)
                    buff.append((skey_id, svalue_types_specific))
                return tmp_buff_size

            buff_types_specific_dbpedia_size += parse_dbpedia_types(
                att="types_specific", buff=buff_types_specific_dbpedia
            )
            buff_types_transitive_dbpedia_size += parse_dbpedia_types(
                att="types_transitive", buff=buff_types_transitive_dbpedia
            )

            # 4. Triples
            # 4.1. Entity triples
            # 4.1.1. Wikidata triples
            triples_entities = set()
            n_facts_entities = defaultdict(set)
            n_facts_entities_others = defaultdict(set)
            if obj_wikidata["claims_wd"]:
                for wd_prop, wd_entities in obj_wikidata["claims_wd"].items():
                    triples_entities.update(wd_entities)
                redirect_claims_entities = defaultdict(set)
                for prop, ent_ids in obj_wikidata["claims_wd"].items():
                    for ent_id in ent_ids:
                        redirect_ent_id = self.mapper.get_redirect_wikidata(ent_id)
                        if redirect_ent_id:
                            redirect_claims_entities[prop].add(redirect_ent_id)
                        else:
                            redirect_claims_entities[prop].add(ent_id)
                n_facts_entities = redirect_claims_entities

            # if wd_obj_nt and wd_obj_nt["claims_wd"]:
            #     for wd_prop, wd_entities in wd_obj_nt["claims_wd"].items():
            #         triples_entities.update(wd_entities)
            #         n_facts_entities[wd_prop].update(wd_entities)
            #
            # 4.1.2. DBpedia triples
            if obj_dbpedia and obj_dbpedia.get("facts_entity"):
                for dp_prop, dp_entities in obj_dbpedia["facts_entity"].items():
                    map_dp_prop = self.mapper.get_wikidata_from_dbpedia_prop(dp_prop)

                    for dp_entity in dp_entities:
                        map_wd_id = self.mapper.get_wikidata_from_dbpedia(dp_entity)
                        if not map_wd_id or not ul.is_wd_item(map_wd_id):
                            continue

                        if map_dp_prop:
                            n_facts_entities[map_dp_prop].add(map_wd_id)
                        elif map_wd_id not in triples_entities:
                            triples_entities.add(map_wd_id)
                            n_facts_entities_others[dp_prop].add(map_wd_id)

            if obj_wikipedia and obj_wikipedia["claims_wd"]:
                for wp_prop, wp_entities in obj_wikipedia["claims_wd"].items():
                    for wp_entity in wp_entities:
                        map_wd_id = self.mapper.get_wikidata_from_wikipedia(wp_entity)
                        if (
                            map_wd_id
                            and ul.is_wd_item(map_wd_id)
                            and map_wd_id not in triples_entities
                        ):
                            triples_entities.add(map_wd_id)
                            n_facts_entities_others[wp_prop].add(map_wd_id)

            if n_facts_entities:
                for f_prop, f_items in n_facts_entities.items():
                    f_prop = self.get_vocab_id(f_prop, is_add=True)
                    f_key = serialize_key(f"{n_id_wikidata}|{f_prop}")
                    f_items = sorted(
                        list({self.get_vocab_id(i, is_add=True) for i in f_items})
                    )
                    f_items = serialize_value(
                        f_items, bytes_value=cf.ToBytesType.INT_LIST
                    )
                    buff_facts_entity_wikidata_size += len(f_items)
                    buff_facts_entity_wikidata.append((f_key, f_items))

            if n_facts_entities_others:
                for f_prop, f_items in n_facts_entities_others.items():
                    f_key = serialize_key(f"{n_id_wikidata}|{f_prop}")
                    f_items = sorted(
                        list({self.get_vocab_id(i, is_add=True) for i in f_items})
                    )
                    f_items = serialize_value(
                        f_items, bytes_value=cf.ToBytesType.INT_LIST
                    )
                    buff_facts_entity_others_size += len(f_items)
                    buff_facts_entity_others.append((f_key, f_items))

            # 4.2. Literal triples
            triples_literal = set()
            n_facts_literal_others = {
                "string": defaultdict(set),
                "time": defaultdict(set),
                "quantity": defaultdict(set),
            }
            has_value = False

            def add_lit(buff_obj, att, values, get_prop_id=True):
                tmp_size = 0
                for f_prop, f_items in values.items():
                    if get_prop_id:
                        f_prop = self.get_vocab_id(f_prop, is_add=True)
                    f_key = serialize_key(f"{n_id_wikidata}|{f_prop}|{att[0]}")
                    f_items = serialize_value(f_items, compress_value=False)
                    tmp_size += len(f_items)
                    buff_obj.append((f_key, f_items))
                return tmp_size

            if obj_wikidata["claims_literal"]:
                for wd_prop, wd_literal in {
                    **obj_wikidata["claims_literal"]["string"],
                    **obj_wikidata["claims_literal"]["time"],
                    **obj_wikidata["claims_literal"]["quantity"],
                }.items():
                    for literal_obj in wd_literal:
                        if isinstance(literal_obj, str):
                            triples_literal.add(str(literal_obj))
                        else:
                            l_value, l_unit = literal_obj
                            triples_literal.add(str(l_value))
                links = defaultdict()
                media = defaultdict()
                strings = defaultdict()
                facts = obj_wikidata["claims_literal"]

                if facts and facts.get("string"):
                    for p, v in facts["string"].items():
                        p_type = db_wikidata_json.get_types_specific(p)
                        # external sources
                        if "Q19847637" in p_type:
                            links[p] = v
                        # property to link to media files
                        elif (
                            "Q18610173" in p_type
                            or "Q28464773" in p_type
                            or "Q26940804" in p_type
                        ):
                            media[p] = v
                        else:
                            strings[p] = v

                buff_facts_literal_wikidata_size += add_lit(
                    buff_facts_literal_wikidata, "string", strings
                )
                buff_facts_literal_wikidata_size += add_lit(
                    buff_facts_literal_wikidata, "time", facts.get("time")
                )
                buff_facts_literal_wikidata_size += add_lit(
                    buff_facts_literal_wikidata, "quantity", facts.get("quantity")
                )
                buff_facts_literal_wikidata_size += add_lit(
                    buff_facts_literal_wikidata, "media", media
                )
                buff_facts_literal_wikidata_size += add_lit(
                    buff_facts_literal_wikidata, "links", links
                )

            if obj_dbpedia and obj_dbpedia.get("facts_literal"):
                for dp_prop, dp_literal in obj_dbpedia["facts_literal"][
                    "string"
                ].items():
                    for l_value in dp_literal:
                        if l_value not in triples_literal:
                            triples_literal.add(l_value)
                            n_facts_literal_others["string"][dp_prop].add(l_value)
                            has_value = True

                for dp_prop, dp_literal in obj_dbpedia["facts_literal"]["time"].items():
                    for l_value in dp_literal:
                        if l_value not in triples_literal:
                            triples_literal.add(l_value)
                            n_facts_literal_others["time"][dp_prop].add(l_value)
                            has_value = True

                for dp_prop, dp_literal in obj_dbpedia["facts_literal"][
                    "quantity"
                ].items():
                    for l_value, l_unit in dp_literal:
                        if l_value not in triples_literal:
                            triples_literal.add(l_value)
                            n_facts_literal_others["quantity"][dp_prop].add(
                                (l_value, l_unit)
                            )
                            has_value = True

            if has_value:
                buff_facts_literal_others_size += add_lit(
                    buff_facts_literal_others,
                    "string",
                    n_facts_literal_others.get("string"),
                    get_prop_id=False,
                )
                buff_facts_literal_others_size += add_lit(
                    buff_facts_literal_others,
                    "time",
                    n_facts_literal_others.get("time"),
                    get_prop_id=False,
                )
                buff_facts_literal_others_size += add_lit(
                    buff_facts_literal_others,
                    "quantity",
                    n_facts_literal_others.get("quantity"),
                    get_prop_id=False,
                )

            if len(self._buff_vocab) * 12 >= buff_size:
                self.save_buff_vocab()

            if buff_id_wikipedia_size >= buff_size:
                self.write_bulk(self._env, self.db_id_wikipedia, buff_id_wikipedia)
                buff_id_wikipedia = []
                buff_id_wikipedia_size = 0

            if buff_id_dbpedia_size >= buff_size:
                self.write_bulk(self._env, self.db_id_dbpedia, buff_id_dbpedia)
                buff_id_dbpedia = []
                buff_id_dbpedia_size = 0

            if buff_label_size >= buff_size:
                self.write_bulk(self._env, self.db_label, buff_label)
                buff_label = []
                buff_label_size = 0

            if buff_desc_size >= buff_size:
                self.write_bulk(self._env, self.db_desc, buff_desc)
                buff_desc = []
                buff_desc_size = 0

            if buff_aliases_size >= buff_size:
                self.write_bulk(self._env, self.db_aliases_en, buff_aliases)
                buff_aliases = []
                buff_aliases_size = 0

            if buff_aliases_multi_size >= buff_size:
                self.write_bulk(self._env, self.db_aliases_all, buff_aliases_multi)
                buff_aliases_multi = []
                buff_aliases_multi_size = 0

            if buff_types_specific_dbpedia_size >= buff_size:
                self.write_bulk(
                    self._env,
                    self.db_types_dbpedia_direct,
                    buff_types_specific_dbpedia,
                )
                buff_types_specific_dbpedia = []
                buff_types_specific_dbpedia_size = 0

            if buff_types_transitive_dbpedia_size >= buff_size:
                self.write_bulk(
                    self._env, self.db_types_dbpedia_all, buff_types_transitive_dbpedia,
                )
                buff_types_transitive_dbpedia = []
                buff_types_transitive_dbpedia_size = 0

            if buff_facts_entity_wikidata_size >= buff_size:
                self.write_bulk(
                    self._env, self.db_facts_entity, buff_facts_entity_wikidata
                )
                buff_facts_entity_wikidata = []
                buff_facts_entity_wikidata_size = 0

            if buff_facts_literal_wikidata_size >= buff_size:
                self.write_bulk(
                    self._env, self.db_facts_literal, buff_facts_literal_wikidata,
                )
                buff_facts_literal_wikidata = []
                buff_facts_literal_wikidata_size = 0

            if buff_facts_entity_others_size >= buff_size:
                self.write_bulk(
                    self._env, self.db_facts_entity_others, buff_facts_entity_others
                )
                buff_facts_entity_others = []
                buff_facts_entity_others_size = 0

            if buff_facts_literal_others_size >= buff_size:
                self.write_bulk(
                    self._env, self.db_facts_literal_others, buff_facts_literal_others
                )
                buff_facts_literal_others = []
                buff_facts_literal_others_size = 0

        self.save_buff_vocab()
        if buff_id_wikipedia:
            self.write_bulk(self._env, self.db_id_wikipedia, buff_id_wikipedia)
        if buff_id_dbpedia:
            self.write_bulk(self._env, self.db_id_dbpedia, buff_id_dbpedia)
        if buff_label:
            self.write_bulk(self._env, self.db_label, buff_label)
        if buff_desc:
            self.write_bulk(self._env, self.db_desc, buff_desc)
        if buff_aliases:
            self.write_bulk(self._env, self.db_aliases_en, buff_aliases)
        if buff_aliases_multi:
            self.write_bulk(self._env, self.db_aliases_all, buff_aliases_multi)
        if buff_types_specific_dbpedia:
            self.write_bulk(
                self._env, self.db_types_dbpedia_direct, buff_types_specific_dbpedia
            )
        if buff_types_transitive_dbpedia:
            self.write_bulk(
                self._env, self.db_types_dbpedia_all, buff_types_transitive_dbpedia,
            )
        if buff_facts_entity_wikidata:
            self.write_bulk(self._env, self.db_facts_entity, buff_facts_entity_wikidata)
        if buff_facts_literal_wikidata:
            self.write_bulk(
                self._env, self.db_facts_literal, buff_facts_literal_wikidata
            )
        if buff_facts_entity_others:
            self.write_bulk(
                self._env, self.db_facts_entity_others, buff_facts_entity_others
            )
        if buff_facts_literal_others:
            self.write_bulk(
                self._env, self.db_facts_literal_others, buff_facts_literal_others
            )

    def _build_invert_of(self, db_source, db_target, is_pid=True, step=10000):
        invert_index = defaultdict(set)
        c_values = 0

        def update_desc():
            return f"key: {len(invert_index):,} values: {c_values:,}"

        p_bar = tqdm(desc=update_desc(), total=self.get_db_size(db_source),)

        for i, (k, v) in enumerate(
            self.get_db_iter(db_source, bytes_value=cf.ToBytesType.INT_LIST)
        ):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
                # break

            k = k.split("|")
            head_ent = int(k[0])
            if is_pid:
                prop = int(k[1])
            else:
                prop = k[1]
            for tail_ent in v:
                c_values += 1
                key = f"{tail_ent}|{prop}"
                invert_index[key].add(head_ent)

        self.write_bulk_with_buffer(
            self._env, db_target, invert_index, bytes_value=cf.ToBytesType.INT_LIST
        )

    def build_pagerank(self, pagerank_obj=None):
        if pagerank_obj is None:
            pagerank_obj = iw.load_obj_pkl(cf.DIR_WIKI_GRAPH_PAGERANK)

        self.write_bulk_with_buffer(
            self._env, self.db_pagerank, pagerank_obj, compress_value=False
        )
        pagerank_obj.clear()

    def build(self):
        # self._build_from_dumps()
        # 827,441,086: 100%|████████████████████████████████████████████▉| 95050000/95054586 [4:17:43<00:00, 6146.79it/s]
        self.build_db_pagerank(n_cpu=8)
        self._build_invert_of(
            self.db_facts_entity, self.db_facts_entity_wikidata_inv, is_pid=True
        )
        self._build_invert_of(
            self.db_facts_entity_others, self.db_facts_entity_others_inv, is_pid=False
        )
        self.copy_lmdb()

    def build_redirect(self):
        # redirect_of = iw.load_obj_pkl(f"{cf.DIR_MODELS}/redirect_of.pkl")
        # wd_redirects = iw.load_obj_pkl(f"{cf.DIR_MODELS}/wd_redirects.pkl")

        # limit = 1e6
        i_wd = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_PAGE, "rt", encoding="utf-8", newline="\n"
        ) as f:
            # c = 0
            with tqdm() as p_bar:
                for line in f:
                    if not line.startswith("INSERT INTO"):
                        continue
                    for v in ul.parse_sql_values(line):
                        # c += 1
                        if ul.is_wd_item(v[2]):
                            p_bar.update()
                            i_wd[v[0]] = v[2]
                    # if c > limit:
                    #     break

        wd_redirects = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            # c = 0
            with tqdm() as p_bar:
                for line in f:
                    if not line.startswith("INSERT INTO"):
                        continue
                    for v in ul.parse_sql_values(line):
                        # c += 1
                        if ul.is_wd_item(v[2]) and i_wd.get(v[0]):
                            p_bar.update()
                            wd_redirects[i_wd[v[0]]] = v[2]
                    # if c > limit:
                    #     break

        redirect_of = defaultdict(set)
        for wd, wd_redirect in wd_redirects.items():
            redirect_of[wd_redirect].add(wd)

        iw.save_obj_pkl(f"{cf.DIR_MODELS}/wd_redirects.pkl", wd_redirects)
        iw.save_obj_pkl(f"{cf.DIR_MODELS}/redirect_of.pkl", redirect_of)

    def split_id_and_commonds(self):
        buff = []
        buff_size = 0

        def update_desc():
            return f"buff:{buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        p_bar = tqdm(total=self.size())
        for i, k in enumerate(self.keys()):
            p_bar.update()
            if i and i % 1000 == 0:
                p_bar.set_description(desc=update_desc())
            if k == "Q1":
                debug = 1
            links = defaultdict()
            media = defaultdict()
            strings = defaultdict()
            facts = self.get_facts_literal(k)

            if facts and facts.get("string"):
                for p, v in facts["string"].items():
                    p_type = self.get_types_specific_dbpedia(p)
                    # external sources
                    if "Q19847637" in p_type:
                        links[p] = v
                    # property to link to media files
                    elif (
                        "Q18610173" in p_type
                        or "Q28464773" in p_type
                        or "Q26940804" in p_type
                    ):
                        media[p] = v
                    else:
                        strings[p] = v
            v = {
                "string": strings,
                "time": facts.get("time"),
                "quantity": facts.get("quantity"),
                "media": media,
                "links": links,
            }

            k, v = serialize(k, v)
            buff_size += len(k) + len(v)
            buff.append((k, v))
            if buff_size >= cf.LMDB_BUFF_BYTES_SIZE:
                self.write_bulk(self.env, self.db_facts_literal, buff)
                buff_size = 0

        if buff_size:
            self.write_bulk(self.env, self.db_facts_literal, buff)
        p_bar.close()

    @staticmethod
    def compute_pagerank(
        graph, alpha=0.85, max_iter=1000, tol=1e-06, personalize=None, reverse=False
    ):
        if reverse:
            graph = graph.T
            iw.print_status("Reversed matrix")

        n, _ = graph.shape
        iw.print_status(f"Pagerank Calculation: {n} nodes")
        r = np.asarray(graph.sum(axis=1)).reshape(-1)

        k = r.nonzero()[0]

        D_1 = sprs.csr_matrix((1 / r[k], (k, k)), shape=(n, n))

        if personalize is None:
            personalize = np.ones(n)
        personalize = personalize.reshape(n, 1)
        s = (personalize / personalize.sum()) * n

        z_T = (((1 - alpha) * (r != 0) + (r == 0)) / n)[np.newaxis, :]
        W = alpha * graph.T @ D_1

        x = s
        oldx = np.zeros((n, 1))

        iteration = 0

        tmp_tol = scipy.linalg.norm(x - oldx)
        while tmp_tol > tol:
            iw.print_status(f"Iteration {iteration + 1} - Tol: {tmp_tol}")
            oldx = x
            x = W @ x + s @ (z_T @ x)
            iteration += 1
            if iteration >= max_iter:
                break
            tmp_tol = scipy.linalg.norm(x - oldx)
        x = x / sum(x)

        return x.reshape(-1)

    def build_db_pagerank(
        self,
        n_cpu=1,
        alpha=0.85,
        max_iter=1000,
        tol=1e-06,
        personalize=None,
        reverse=True,
        step=10000,
    ):
        row, col, data = [], [], []

        def update_desc():
            return f"{len(row):,}"

        p_bar = tqdm(desc=update_desc(), total=self.size())
        with closing(Pool(n_cpu)) as pool:
            for wd_i, (wd_id, claims) in enumerate(
                pool.imap(
                    pool_get_outlinks,
                    self.get_db_iter(
                        self.db_int_qid,
                        get_values=False,
                        integerkey=True,
                        compress_value=False,
                    ),
                )
            ):
                # if n_cpu == 1:
                #     for wd_i, wd_id in enumerate(
                #         self.get_db_iter(self.db_int_qid, get_values=False, integerkey=True)
                #     ):
                #         wd_id, claims = pool_get_outlinks(wd_id)

                if wd_i and wd_i % step == 0:
                    p_bar.update(step)
                    p_bar.set_description(desc=update_desc())
                if not claims:
                    continue
                # Get entity triples
                for id_tail, v_wd_weight in claims:
                    row.append(wd_id)
                    col.append(id_tail)
                    data.append(int(v_wd_weight))

                # if len(data) > 1000000:
                #     break
        p_bar.close()

        n = self._len_vocab
        # row, col, data = zip(*sorted(zip(row, col, data)))
        graph = sparse.csr_matrix((data, (row, col)), dtype=np.uintc, shape=(n, n))
        del data
        del row
        del col
        pagerank = self.compute_pagerank(
            graph, alpha, max_iter, tol, personalize, reverse
        )

        # save pagerank stats for normalization later
        pagerank = np.array(pagerank)
        pagerank_stats = {
            "max": np.max(pagerank),
            "min": np.min(pagerank),
            "std": np.std(pagerank),
            "mean": np.mean(pagerank),
            "div": np.max(pagerank) - np.min(pagerank),
        }
        iw.save_obj_pkl(cf.DIR_WIKI_PAGERANK_STATS, pagerank_stats)
        iw.print_status(pagerank_stats)
        iw.save_obj_pkl(cf.DIR_WIKI_GRAPH_PAGERANK, pagerank)

        # pagerank = [
        #     [
        #         self.get_value(
        #             self.db_int_qid, i, integerkey=True, compress_value=False
        #         ),
        #         score,
        #     ]
        #     for i, score in enumerate(pagerank)
        # ]
        pagerank = [[i, score] for i, score in enumerate(pagerank)]
        iw.print_status(f"Saving {len(pagerank)}")

        self.write_bulk_with_buffer(
            self._env,
            self.db_pagerank,
            pagerank,
            integerkey=True,
            sort_key=False,
            compress_value=False,
        )
        iw.print_status("Done")


def pool_get_outlinks(wd_id):
    if ul.is_redirect_or_disambiguation(wd_id):
        return wd_id, set()
    return wd_id, m_f.m_items().get_wd_outlinks(wd_id)


def pool_multi_read(key):
    return m_f.m_items().get_item(key)


def query_ja_organizations():
    start = time()
    db = MItem()
    orgs = defaultdict()
    for k in tqdm(db.keys(), total=db.size()):
        facts_entity = db.get_value(db.db_facts_entity, k)
        if not facts_entity:
            continue
        available = facts_entity.get("P3225")
        if available:
            orgs[k] = available
            iw.print_status(f"{k}\t{available}")
    iw.print_status(f"Run time: {timedelta(seconds=time() - start)}")


if __name__ == "__main__":
    m_f.init()
    items = MItem()
    # items.test_iter()

    # tmp = items.get_item("Q1490")
    # tmp1 = items.get_item("Q65120889")
    tmp1 = items.get_item("Q61570087")

    # items.get_wd_outlinks("Q31")

    # items.split_id_and_commonds()
    items.build()
    # items.copy_lmdb()
    # query_ja_organizations()
    # items.get_value(items.db_label, "Q11585415", compress_value=True)
    # tmp = items.get_item("Q513")
    # tmp1 = items.get_item("Q1490")
    # tmp2 = items.get_item("Q11235155")
    # items.cal_max_pagerank()
    # items.build_pagerank(buff_size=10000000)
    iw.print_status("Done")
