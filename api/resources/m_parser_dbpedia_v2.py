import datetime
import numbers
import sys
from collections import defaultdict

from rdflib import Literal
from rdflib.util import from_n3
from tqdm import tqdm

import m_config as cf
from api.resources.m_db_rocks import DBSubspace
from api.resources.m_mapping_id import MappingID
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


class DumpDBpedia:
    def __init__(self, db_dir=cf.DIR_DBPEDIA_ITEMS):
        self.db_dir = db_dir
        self.db_label = DBSubspace(db_dir + "/db_label")
        self.db_desc = DBSubspace(db_dir + "/db_desc")
        self.db_aliases_en = DBSubspace(db_dir + "/db_aliases_en")
        self.db_aliases_multilingual = DBSubspace(db_dir + "/db_aliases_multilingual")
        self.db_types_specific = DBSubspace(db_dir + "/db_types_specific")
        self.db_types_transitive = DBSubspace(db_dir + "/db_types_transitive")
        self.db_facts_entity = DBSubspace(db_dir + "/db_facts_entity")
        self.db_facts_literal = DBSubspace(db_dir + "/db_facts_literal")

    def get_item(self, key, integerkey=False, mode="msgpack", compress=True):
        return {
            "label": self.db_label.get_item(key, integerkey, mode, compress),
            "desc": self.db_desc.get_item(key, integerkey, mode, compress),
            "aliases_en": self.db_aliases_en.get_item(key, integerkey, mode, compress),
            "aliases_multilingual": self.db_aliases_multilingual.get_item(
                key, integerkey, mode, compress
            ),
            "types_specific": self.db_types_specific.get_item(
                key, integerkey, mode, compress
            ),
            "types_transitive": self.db_types_transitive.get_item(
                key, integerkey, mode, compress
            ),
            "facts_entity": self.db_facts_entity.get_item(
                key, integerkey, mode, compress
            ),
            "facts_literal": self.db_facts_literal.get_item(
                key, integerkey, mode, compress
            ),
        }

    def get_size(self):
        return self.db_label.get_size()

    def keys(self, integerkey=False):
        return self.db_label.keys(integerkey)

    def values(self, mode="msgpack", compress=True):
        for k in self.keys():
            v = self.get_item(k, mode, compress)
            yield v

    def items(self, integerkey=False, mode="msgpack", compress=True):
        for k in self.keys(integerkey):
            v = self.get_item(k, mode, compress)
            yield k, v

    @staticmethod
    def _parse_ttl_1_1(
        file,
        db,
        message,
        buff_limit=cf.SIZE_1GB,
        integerkey=False,
        mode="msgpack",
        compress=True,
    ):
        buff_objs = []
        buff_size = 0
        for line in tqdm(iw.read_line_from_file(file), desc=message):
            if "dbpedia" not in str(line):
                continue

            respond = ul.parse_triple_line(line)
            if not respond:
                continue

            dp_title, _, dp_value = respond
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if dp_title:
                dp_value = from_n3(dp_value)
                if dp_value:
                    obj = (dp_title, str(dp_value.value))
                    buff_objs.append(obj)
                    buff_size += sys.getsizeof(obj)

            if buff_size > buff_limit:
                db.set_items(buff_objs, integerkey, mode, compress)
                buff_objs = []
                buff_size = 0

        if buff_objs:
            db.set_items(buff_objs, integerkey, mode, compress)

    @staticmethod
    def _parse_ttl_1_n(
        file, db, message, integerkey=False, mode="msgpack", compress=True
    ):
        buff_objs = defaultdict(set)
        mapper = MappingID()

        for line in tqdm(iw.read_line_from_file(file), desc=message):
            if "dbpedia" not in str(line):
                continue
            respond = ul.parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, _, dp_obj = respond
            if dp_title and dp_obj and cf.DBR in dp_title:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue

                if cf.DBO in dp_obj or cf.DBR in dp_obj:
                    dp_obj = dp_obj.replace(cf.DBO, "")
                    dp_obj = dp_obj.replace(cf.DBR, "")
                    dp_obj = ul.norm_wikipedia_title(dp_obj, unquote=True)
                    if dp_obj:
                        buff_objs[dp_title].add(dp_obj)

                elif cf.WD in dp_obj:
                    dp_obj = dp_obj.replace(cf.WD, "")
                    if dp_obj and ul.is_wd_item(dp_obj):
                        dp_type_mapper = mapper.get_dbpedia_from_wikidata(dp_obj)
                        if dp_type_mapper:
                            buff_objs[dp_title].add(dp_type_mapper)

        if buff_objs:
            db.set_items(buff_objs, integerkey, mode, compress)

    def _parse_aliases(self, integerkey=False, mode="msgpack", compress=True):
        buff_objs = defaultdict(set)
        mapping_id = MappingID()

        disambiguation_aliases = defaultdict(set)
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_DISAMBIGUATION),
            desc="DBpedia Disambiguation",
        ):
            if "dbpedia" not in str(line):
                continue

            respond = ul.parse_triple_line(line)
            if not respond:
                continue
            dp_title, _, dp_value = respond
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)

            dp_title = dp_title.replace("(disambiguation)", "").strip()

            if dp_title:
                try:
                    dp_value = str(from_n3(dp_value))
                except:
                    dp_value = str(dp_value)

                if dp_value:
                    dp_value = ul.norm_wikipedia_title(dp_value, unquote=True)
                    disambiguation_aliases[dp_value].add(dp_title)

        for dp_title in tqdm(self.keys()):
            dp_obj = self.db_facts_literal.get_item(dp_title)
            for entity_name in ["otherName", "alias"]:
                if dp_obj and dp_obj["string"].get(entity_name):
                    for dp_label in dp_obj["string"][entity_name]:
                        if (
                            "(disambiguation)" in dp_label
                            and "(disambiguation)" not in dp_title
                        ):
                            continue
                        buff_objs[dp_title].add(dp_label)

            redirects_of = mapping_id.get_redirect_of_dbpedia(dp_title)
            if redirects_of:
                for dp_label in redirects_of:
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_objs[dp_title].add(dp_label)

            ambiguous_labels = disambiguation_aliases.get(dp_title)
            if ambiguous_labels:
                for dp_label in ambiguous_labels:
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_objs[dp_title].add(dp_label)

        if buff_objs:
            self.db_aliases_en.set_items(buff_objs, integerkey, mode, compress)

    def _parse_aliases_multilingual(
        self, integerkey=False, mode="msgpack", compress=True
    ):
        buff_objs = defaultdict(set)
        langs = {
            "ar",
            "ca",
            "cs",
            "de",
            "el",
            "eo",
            "es",
            "eu",
            "fr",
            "ga",
            "id",
            "it",
            "ja",
            "ko",
            "nl",
            "pl",
            "pt",
            "ru",
            "sv",
            "uk",
            "zh",
        }
        for lang in langs:
            dump_file = f"{cf.DIR_DUMP_DP}/labels_lang={lang}_uris=en.ttl.bz2"
            for line in tqdm(
                iw.read_line_from_file(dump_file), desc=f"DBpedia labels: {lang}"
            ):
                if "dbpedia" not in str(line):
                    continue

                respond = ul.parse_triple_line(line)
                if not respond:
                    continue
                dp_title, _, dp_value = respond
                dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
                if dp_title:
                    dp_value = from_n3(dp_value)
                    if dp_value:
                        buff_objs[dp_title].add(str(dp_value.value))

        if buff_objs:
            self.db_aliases_multilingual.set_items(
                buff_objs, integerkey, mode, compress
            )

    def _parse_facts(self, integerkey=False, mode="msgpack", compress=True):
        """
        Parse DBpedia facts from Infobox and mapping of objects and literals
        :return:
        :rtype:
        """
        buff_entities = defaultdict()
        buff_literals = defaultdict()
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_INFOBOX), desc="DBpedia infobox",
        ):
            respond = ul.parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if dp_title and dp_value and dp_prop and cf.DBR in dp_title:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue
                dp_prop = ul.remove_prefix(dp_prop)
                if cf.DBR in dp_value:
                    dp_value = dp_value.replace(cf.DBR, "")
                    dp_value = ul.norm_wikipedia_title(dp_value, unquote=True)

                    if not buff_entities.get(dp_title):
                        buff_entities[dp_title] = defaultdict(set)
                    if dp_title and dp_value:
                        buff_entities[dp_title][dp_prop].add(dp_value)

                else:
                    if not buff_literals.get(dp_title):
                        buff_literals[dp_title] = {
                            "string": defaultdict(set),
                            "time": defaultdict(set),
                            "quantity": defaultdict(set),
                        }

                    if dp_value:
                        try:
                            dp_value = from_n3(dp_value)
                        except Exception as message:
                            # iw.print_status(str(dp_value) + message, is_screen=True)
                            buff_literals[dp_title]["string"][dp_prop].add(dp_value)
                        if dp_value and isinstance(dp_value, Literal):
                            if isinstance(dp_value.value, datetime.date) or isinstance(
                                dp_value.value, datetime.datetime
                            ):
                                buff_literals[dp_title]["time"][dp_prop].add(
                                    str(dp_value.value)
                                )
                            elif isinstance(dp_value.value, numbers.Number):
                                buff_literals[dp_title]["quantity"][dp_prop].add(
                                    (dp_value.value, "1")
                                )
                            else:
                                if not dp_value.value:
                                    dp_value = str(dp_value.toPython())
                                else:
                                    dp_value = str(dp_value.value)
                                tmp = ul.clean_text_brackets(dp_value)
                                if tmp:
                                    buff_literals[dp_title]["string"][dp_prop].add(tmp)
            # if dp_title == "Tokyo":
            #     break

        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_LITERALS),
            desc="DBpedia mapping literals",
        ):
            # if len(buff_literals) > 2000:
            #     break
            respond = ul.parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if dp_title and cf.DBR in dp_title and dp_value and dp_prop:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue
                if not buff_literals.get(dp_title):
                    buff_literals[dp_title] = {
                        "string": defaultdict(set),
                        "time": defaultdict(set),
                        "quantity": defaultdict(set),
                    }
                dp_prop = ul.remove_prefix(dp_prop)
                # if "http" in dp_prop:
                #     tmp = 1
                if dp_value:
                    try:
                        dp_value = from_n3(dp_value)
                    except Exception as message:
                        iw.print_status(message, is_screen=False)
                        buff_literals[dp_title]["string"][dp_prop].add(dp_value)
                        continue
                    if not dp_value:
                        continue

                    if isinstance(dp_value, Literal):
                        if isinstance(dp_value.value, datetime.date) or isinstance(
                            dp_value.value, datetime.datetime
                        ):
                            buff_literals[dp_title]["time"][dp_prop].add(
                                str(dp_value.value)
                            )
                        elif isinstance(dp_value.value, numbers.Number):
                            buff_literals[dp_title]["quantity"][dp_prop].add(
                                (dp_value.value, "1")
                            )
                        else:
                            if not dp_value.value:
                                dp_value = str(dp_value.toPython())
                            else:
                                dp_value = str(dp_value.value)
                            tmp = ul.clean_text_brackets(dp_value)
                            if tmp:
                                buff_literals[dp_title]["string"][dp_prop].add(tmp)
            # if dp_title == "Tokyo":
            #     break

        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_OBJECTS),
            desc="DBpedia mapping objects",
        ):
            # if len(buff_entities) > 2000:
            #     break
            respond = ul.parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if (
                dp_title
                and cf.DBR in dp_title
                and dp_prop
                and cf.DBO in dp_prop
                and dp_value
                and cf.DBR in dp_title
            ):
                dp_title = dp_title.replace(cf.DBR, "")
                dp_value = dp_value.replace(cf.DBR, "")
                dp_prop = dp_prop.replace(cf.DBO, "")

                dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
                dp_value = ul.norm_wikipedia_title(dp_value, unquote=True)

                if not buff_entities.get(dp_title):
                    buff_entities[dp_title] = defaultdict(set)
                if dp_title and dp_value:
                    buff_entities[dp_title][dp_prop].add(dp_value)

        if buff_entities:
            self.db_facts_entity.set_items(buff_entities, integerkey, mode, compress)

        if buff_literals:
            self.db_facts_literal.set_items(buff_literals, integerkey, mode, compress)

    def build(self):
        iw.print_status("DBpedia Parsing")
        self._parse_ttl_1_1(cf.DIR_DUMP_DP_LABELS, self.db_label, message="Label")
        self._parse_ttl_1_1(cf.DIR_DUMP_DP_DESC, self.db_desc, message="Desc")
        self._parse_ttl_1_n(
            cf.DIR_DUMP_DP_TYPES_SPECIFIC,
            self.db_types_specific,
            message="Specific Types",
        )
        self._parse_ttl_1_n(
            cf.DIR_DUMP_DP_TYPES_TRANSITIVE,
            self.db_types_transitive,
            message="Transitive Types",
        )
        self._parse_facts()
        self._parse_aliases_multilingual()
        self._parse_aliases()


if __name__ == "__main__":
    dp_items = DumpDBpedia(cf.DIR_DBPEDIA_ITEMS)
    dp_items.build()

    tmp = dp_items.get_item("James Adams (character)")
    dp_id = "Batman: Arkham City"
    # tmp = dp_items.get_item(dp_id)
