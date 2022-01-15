import bz2
import datetime
import gc
import numbers
import queue
from collections import defaultdict
from contextlib import closing
from multiprocessing.pool import Pool
import ujson
from rdflib.util import from_n3
from tqdm import tqdm

import api.utilities.m_io as iw
import m_config as cf
from api.resources.m_db_item import DBItem, serialize
from api.utilities import m_utils as ul


class WDDumpReader(object):
    def __init__(self, dir_dump, lang=cf.LANG):
        self.dir_dump = dir_dump
        self.lang = lang

    def __iter__(self):
        with bz2.BZ2File(self.dir_dump) as f:
            for line in f:
                yield line, self.lang


class WDItem(DBItem):
    def __init__(self, db_file):
        super().__init__(db_file=db_file, max_db=1, map_size=cf.SIZE_1GB * 75)
        self._db_wds = self._env.open_db(b"__wds__")

    def copy(self, compress=False):
        iw.print_status(self.env.info())
        db_names = {b"__wds__": b"__wds__"}
        self.copy_new_file(
            db_names, map_size=cf.SIZE_1GB * 10, compress=compress,
        )

    def info(self):
        iw.print_status(self.env.info())
        iw.print_status("%.0fGB" % (self.env.info()["map_size"] / cf.SIZE_1GB))
        iw.print_status(f"Wikidata items: {self.size()}")

    def items(self, compress=True):
        return self.get_db_iter(self._db_wds, compress_value=compress)

    def keys(self):
        return self.get_db_iter(self._db_wds, get_values=False)

    def size(self):
        return self.get_db_size(self._db_wds)

    def get_item(self, wd_id, compress=True):
        return self.get_value(self._db_wds, wd_id, compress_value=compress)

    def get_subclass_of_specific(self, wd_id, compress=True):
        responds = set()
        wd_obj = self.get_item(wd_id, compress=compress)
        if not wd_obj:
            return responds
        subclass_of = wd_obj["claims_wd"].get("P279")
        if not subclass_of:
            return responds
        return subclass_of

    def get_part_of_specific(self, wd_id, compress=True):
        responds = set()
        wd_obj = self.get_item(wd_id, compress=compress)
        if not wd_obj:
            return responds
        part_of = wd_obj["claims_wd"].get("P361")
        if not part_of:
            return responds
        return part_of

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

    def get_types_specific(self, wd_id, compress=True):
        responds = set()
        wd_obj = self.get_item(wd_id, compress=compress)
        if not wd_obj:
            return responds

        types_specific = wd_obj["claims_wd"].get("P31")
        if not types_specific:
            return responds
        return types_specific

    def get_types_transitive(self, wd_id, compress=True):
        types_direct = self.get_types_specific(wd_id, compress=compress)
        types_transitive = set()
        for type_direct in types_direct:
            branch_transitive = self._get_items_recursive(
                type_direct, self.get_subclass_of_specific
            )
            types_transitive.update(branch_transitive)
        return types_transitive

    def get_part_of_transitive(self, wd_id):
        return self._get_items_recursive(wd_id, self.get_part_of_specific)

    def build_from_nt_dump(
        self, dump_reader, buffer_size=cf.LMDB_BUFF_BYTES_SIZE, lang="en", compress=True
    ):
        buff_wd = []
        c_ok = 0

        def update_desc():
            return f"Wikidata Truthy Parsing: OK:{c_ok}"

        p_bar = tqdm(desc=update_desc())
        for i, entity_obj in enumerate(parse_nt_dump(dump_reader, lang)):
            p_bar.update()
            if not entity_obj:
                continue
            c_ok += 1
            wd_id, wd_obj = entity_obj
            wd_id, wd_obj = ul.get_dump_obj(wd_id, wd_obj, compress=compress)

            buff_wd.append((wd_id, wd_obj))
            if len(buff_wd) == buffer_size:
                p_bar.set_description(desc=update_desc())
                self.write_bulk(self.env, self._db_wds, buff_wd)
                buff_wd = []
                gc.collect()
        if buff_wd:
            self.write_bulk(self.env, self._db_wds, buff_wd)
        p_bar.close()

    def build_from_json_dump(self, iter_items, n_cpu=1):
        buff_wd = []
        buff_size = 0
        # buff_wd_wp = []
        c_ok = 0
        c_en = 0
        c_all = 0
        # c_wp = 0
        # w_map = MappingID()

        def update_desc():
            return f"Wikidata Parsing|OK:{c_ok}|en:{c_en}|multi:{c_all}|buff:{buff_size / cf.SIZE_512MB * 100:.0f}%"

        with tqdm(desc=update_desc()) as p_bar:
            # if n_cpu == 1:
            #     for i, iter_item in enumerate(iter_items):
            #         wd_respond = parse_json_dump(iter_item)
            with closing(Pool(n_cpu)) as pool:
                for i, wd_respond in enumerate(
                    pool.imap_unordered(parse_json_dump, iter_items)
                ):
                    if i and i % 100 == 0:
                        p_bar.set_description(desc=update_desc())
                    p_bar.update()
                    if not wd_respond:
                        continue
                    wd_id, wd_obj, wp_title = wd_respond
                    c_ok += 1
                    # Add buffer data
                    key, value = serialize(wd_id, wd_obj)
                    buff_size += len(key) + len(value)
                    buff_wd.append((key, value))

                    c_en += len(wd_obj["aliases"]) + 1
                    c_all += len(wd_obj["labels_multi_lang"])

                    # if wp_title \
                    #         and len(wp_title) \
                    #         and not w_map.get_wp_title_from_wd_id(wd_id):
                    #     buff_wd_wp.append(ul.get_dump_obj(wd_id, wp_title))
                    #     c_wp += 1

                    # Save buffer data
                    if buff_size > cf.LMDB_BUFF_BYTES_SIZE:
                        p_bar.set_description(desc=update_desc())
                        DBItem.write_bulk(self.env, self._db_wds, buff_wd)
                        buff_wd = []
                        buff_size = 0

                    # if len(buff_wd_wp) == buffer_size:
                    #     w_map.write_mapping_wd_wp(buff_wd_wp)
                    #     del buff_wd_wp
                    #     buff_wd_wp = defaultdict()
                    # if c_ok > 50000:
                    #     break

                if len(buff_wd):
                    DBItem.write_bulk(self.env, self._db_wds, buff_wd)

                # buff_wd_wp = {k: v for k, v in buff_wd_wp.items()
                #               if not w_map.get_wp_title_from_wd_id(k)}
                # if len(buff_wd_wp):
                #     w_map.write_mapping_wd_wp(buff_wd_wp)

    # def build(
    #         self,
    #         iter_items,
    #         size=cf.LMDB_MAP_SIZE,
    #         buff_size=cf.LMDB_BUFF_SIZE,
    #         n_cpu=8,
    # ):
    #     iw.print_status("Build Wikidata resource")
    #     self._build_from_json_dump(iter_items, size, buff_size, n_cpu)
    # self.build_from_nt_dump(iter_items, buff_size)


def parse_json_dump(respond):
    json_line, lang = respond
    line = json_line.rstrip().decode(cf.ENCODING)
    if line in ("[", "]"):
        return None

    if line[-1] == ",":
        line = line[:-1]
    try:
        obj = ujson.loads(line)
    except ValueError:
        return None
    if obj["type"] != "item" and ul.is_wd_item(obj["id"]) is False:
        return None

    wd_id = obj["id"]
    wd_obj = {
        "labels": None,
        "labels_multi_lang": set(),
        "descriptions": None,
        "aliases": set(),
        "claims_wd": defaultdict(set),
        "claims_literal": {
            "string": defaultdict(set),
            "time": defaultdict(set),
            "quantity": defaultdict(set),
        },
    }
    wd_wp = None

    # Labels
    if obj.get("labels") and obj["labels"].get(lang):
        wd_obj["labels"] = obj["labels"][lang]["value"]
    else:
        wd_obj["labels"] = obj["id"]

    if obj.get("labels"):
        wd_obj["labels_multi_lang"].update(
            {i["value"] for i in obj["labels"].values() if i.get("value")}
        )

    # Descriptions
    if obj.get("descriptions") and obj["descriptions"].get(lang):
        wd_obj["descriptions"] = obj["descriptions"][lang]["value"]

    # Aliases
    if obj.get("aliases") and obj["aliases"].get(lang):
        wd_obj["aliases"] = {
            tmp_obj["value"] for tmp_obj in obj["aliases"][lang] if tmp_obj["value"]
        }

    if obj.get("aliases"):
        wd_obj["labels_multi_lang"].update(
            {i["value"] for l in obj["aliases"].values() for i in l if i.get("value")}
        )

    # Wikipedia title
    if obj.get("sitelinks") and obj["sitelinks"].get(f"{lang}wiki"):
        wikipedia_title = obj["sitelinks"][f"{lang}wiki"].get("title")
        if wikipedia_title and len(wikipedia_title):
            wd_wp = ul.norm_wikipedia_title(wikipedia_title)

    # Statements
    if obj.get("claims"):
        for prop, claims in obj["claims"].items():
            for claim in claims:
                if claim.get("mainsnak") and claim["mainsnak"].get("datavalue"):
                    claim_type = claim["mainsnak"]["datavalue"]["type"]
                    claim_value = claim["mainsnak"]["datavalue"]["value"]
                    # Entity statement
                    if claim_type == "wikibase-entityid":
                        wd_obj["claims_wd"][prop].add(claim_value["id"])

                    # Literal statement
                    elif claim_type == "string":
                        wd_obj["claims_literal"]["string"][prop].add(claim_value)

                    elif claim_type == "time":
                        claim_value = claim_value["time"]
                        claim_value = claim_value.replace("T00:00:00Z", "")
                        if claim_value[0] == "+":
                            claim_value = claim_value[1:]
                        wd_obj["claims_literal"]["time"][prop].add(claim_value)

                    elif claim_type == "quantity":
                        claim_unit = claim_value["unit"]
                        claim_unit = claim_unit.replace(cf.WD, "")

                        claim_value = claim_value["amount"]
                        if claim_value[0] == "+":
                            claim_value = claim_value[1:]

                        wd_obj["claims_literal"]["quantity"][prop].add(
                            (claim_value, claim_unit)
                        )
                    elif claim_type == "monolingualtext":
                        claim_value = claim_value["text"]
                        wd_obj["claims_literal"]["string"][prop].add(claim_value)
                    else:
                        # Todo: implement geo info
                        # elif claim_type == "globecoordinate":
                        continue
    return [wd_id, wd_obj, wd_wp]


def get_entity_from_nt_dump(lang="en"):
    dump_reader = WDDumpReader(cf.DIR_DUMP_WD_SEMTAB)
    entities = defaultdict()
    for i, entity_obj in tqdm(enumerate(parse_nt_dump(dump_reader, lang))):
        if not entity_obj:
            continue
        wd_id, wd_obj = entity_obj
        entities[wd_id] = wd_obj


def parse_nt_dump(dump_reader, lang):
    def new_obj():
        wd_id = None
        wd_obj = {
            "labels": None,
            "descriptions": None,
            "aliases": set(),
            "claims_wd": defaultdict(set),
            "claims_literal": {
                "string": defaultdict(set),
                "time": defaultdict(set),
                "quantity": defaultdict(set),
            },
        }
        return wd_id, wd_obj

    c_id, c_obj = new_obj()
    for i, (line, lang) in enumerate(dump_reader):
        line = ul.parse_triple_line(line, remove_prefix=False)
        if not line:
            continue
        head, relation, tail = line
        if cf.WD not in head:
            continue

        head = head.replace(cf.WD, "")

        if c_id and c_id != head:
            yield c_id, c_obj
            c_id, c_obj = new_obj()

        c_id = head

        if not tail:
            continue

        if cf.WDT in relation or cf.WDT3 in relation:
            relation = relation.replace(cf.WDT, "")
            relation = relation.replace(cf.WDT3, "")
            # claims_wd
            if cf.WD in tail:
                tail = tail.replace(cf.WD, "")
                c_obj["claims_wd"][relation].add(tail)
            else:
                # claims_literal
                try:
                    parsed_t = from_n3(tail)
                    parsed_t = parsed_t.value
                except Exception as message:
                    iw.print_status(message, is_screen=False, is_log=False)
                    c_obj["claims_literal"]["string"][relation].add(tail)
                    continue

                # time
                if isinstance(parsed_t, datetime.date) or isinstance(
                    parsed_t, datetime.datetime
                ):
                    c_obj["claims_literal"]["time"][relation].add(str(parsed_t))
                # quantity
                elif isinstance(parsed_t, numbers.Number):
                    c_obj["claims_literal"]["quantity"][relation].add((parsed_t, "1"))
                else:
                    # string
                    if not parsed_t:
                        continue
                        # dp_value = str(tail.toPython())
                    else:
                        dp_value = str(parsed_t)
                    c_obj["claims_literal"]["string"][relation].add(dp_value)
        else:
            if f"@{lang}" not in tail or not (
                cf.WD_PROP_LABEL in relation
                or cf.WD_PROP_DES in relation
                or cf.WD_PROP_ALIAS in relation
            ):
                continue
            try:
                parsed_t = from_n3(tail)
                tail_value = str(parsed_t.value)
            except Exception as message:
                iw.print_status(message, is_screen=False, is_log=False)
                tail_value = tail
            # labels
            if cf.WD_PROP_LABEL in relation:
                c_obj["labels"] = tail_value
            # description
            elif cf.WD_PROP_DES in relation:
                c_obj["descriptions"] = tail_value
            # aliases
            elif cf.WD_PROP_ALIAS in relation:
                c_obj["aliases"].add(tail_value)
            else:
                continue


def test_wikidata_json_reader():
    dump_reader = WDDumpReader(cf.DIR_DUMP_WD)
    n_c = 0
    n_e = 0
    for i, entity_obj in enumerate(dump_reader):
        if not entity_obj:
            n_e += 1
        n_c += 1
        print(f"{n_e}/{n_c}: {str(entity_obj)}")
        if n_c > 5:
            break


def test_wikidata_nt_reader(n=5000):
    dump_reader = WDDumpReader(cf.DIR_DUMP_WD_SEMTAB)
    n_c = 0
    n_e = 0
    for i, entity_obj in enumerate(dump_reader):
        if not entity_obj:
            n_e += 1
        n_c += 1
        print(f"{n_e}/{n_c}: {str(entity_obj)}")
        if n_c > n:
            break


def test_wikidata_json_parser():
    items = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_JSON)
    items.build_from_json_dump(WDDumpReader(cf.DIR_DUMP_WD))
    items.info()


def test_wikidata_nt_parser():
    items = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_NT)
    reader = WDDumpReader(cf.DIR_DUMP_WD_SEMTAB)
    items.build_from_nt_dump(reader)
    items.info()


if __name__ == "__main__":
    # test_wikidata_json_reader()
    # get_entity_from_nt_dump()
    # test_wikidata_nt_reader()
    # test_wikidata_nt_parser()
    # test_wikidata_json_parser()
    build_obj = WDItem(cf.DIR_WIKIDATA_ITEMS_JSON)

    # build_obj = WDItem(db_file=cf.DIR_WD_ITEMS_NT)
    # build_obj.build_from_json_dump(WDDumpReader(cf.DIR_DUMP_WD))
    tmp = build_obj.get_item("Q61570087")
    iw.print_status("Done")

"""
Wikidata Parsing|OK:95064299|en:104946652|multi:177249219|buff:39%: : 95064303it [21:44:02, 1215.00it/s]
"""
