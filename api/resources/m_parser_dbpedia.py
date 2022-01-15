from datetime import timedelta
from time import time

import datetime
import numbers
from collections import defaultdict

import rdflib
from rdflib import Literal, URIRef, BNode
from rdflib.namespace import NamespaceManager, Namespace
from rdflib.util import from_n3
from tqdm import tqdm

import m_config as cf
from api import m_f
from api.resources.m_db_item import DBItem, serialize
from api.resources.m_mapping_id import MappingID
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


def from_n3_fix(obj):
    try:
        obj = from_n3_fix_unicodeescape(obj)
    except KeyError:
        obj = str(obj)
    except Exception as message:
        iw.print_status(message)
        debug = 1
    return obj


def from_n3_fix_unicodeescape(s: str, default=None, backend=None, nsm=None):
    """
    Try not use this in the orignial code of rdflib
    value.encode("raw-unicode-escape").decode("unicode-escape")

    :param s:
    :type s:
    :param default:
    :type default:
    :param backend:
    :type backend:
    :param nsm:
    :type nsm:
    :return:
    :rtype:
    """
    if not s:
        return default
    if s.startswith("<"):
        # Hack: this should correctly handle strings with either native unicode
        # characters, or \u1234 unicode escapes.
        return URIRef(s[1:-1].encode("raw-unicode-escape").decode("unicode-escape"))
    elif s.startswith('"'):
        if s.startswith('"""'):
            quotes = '"""'
        else:
            quotes = '"'
        value, rest = s.rsplit(quotes, 1)
        value = value[len(quotes) :]  # strip leading quotes
        datatype = None
        language = None

        # as a given datatype overrules lang-tag check for it first
        dtoffset = rest.rfind("^^")
        if dtoffset >= 0:
            # found a datatype
            # datatype has to come after lang-tag so ignore everything before
            # see: http://www.w3.org/TR/2011/WD-turtle-20110809/
            # #prod-turtle2-RDFLiteral
            datatype = from_n3(rest[dtoffset + 2 :], default, backend, nsm)
        else:
            if rest.startswith("@"):
                language = rest[1:]  # strip leading at sign

        value = value.replace(r"\"", '"')
        # unicode-escape interprets \xhh as an escape sequence,
        # but n3 does not define it as such.
        value = value.replace(r"\x", r"\\x")
        # Hack: this should correctly handle strings with either native unicode
        # characters, or \u1234 unicode escapes.
        try:
            value = value.encode("raw-unicode-escape").decode("unicode-escape")
        except UnicodeDecodeError:
            value = str(value)

        return Literal(value, language, datatype)
    elif s == "true" or s == "false":
        return Literal(s == "true")
    elif s.isdigit():
        return Literal(int(s))
    elif s.startswith("{"):
        identifier = from_n3(s[1:-1])
        return rdflib.graph.QuotedGraph(backend, identifier)
    elif s.startswith("["):
        identifier = from_n3(s[1:-1])
        return rdflib.graph.Graph(backend, identifier)
    elif s.startswith("_:"):
        return BNode(s[2:])
    elif ":" in s:
        if nsm is None:
            # instantiate default NamespaceManager and rely on its defaults
            nsm = NamespaceManager(rdflib.graph.Graph())
        prefix, last_part = s.split(":", 1)
        ns = dict(nsm.namespaces())[prefix]
        return Namespace(ns)[last_part]
    else:
        return BNode(s)


class DPItem(DBItem):
    def __init__(self, db_file=cf.DIR_DBPEDIA_ITEMS):
        super().__init__(db_file=db_file, max_db=8, map_size=cf.SIZE_1GB * 15)
        self.db_label = self._env.open_db(b"__label__")
        self.db_desc = self._env.open_db(b"__desc__")
        self.db_aliases_en = self._env.open_db(b"__aliases_en_")
        self.db_aliases_multilingual = self._env.open_db(b"__aliases_multilingual__")
        self.db_types_specific = self._env.open_db(b"__types_specific__")
        self.db_types_transitive = self._env.open_db(b"__types_transitive__")
        self.db_facts_entity = self._env.open_db(b"__facts_entity__")
        self.db_facts_literal = self._env.open_db(b"__facts_literal__")

    def get_item(self, title):
        responds = dict()

        responds["label"] = self.get_value(self.db_label, title, compress_value=False)
        responds["desc"] = self.get_value(self.db_desc, title)
        responds["aliases_en"] = self.get_value(self.db_aliases_en, title)
        responds["aliases_multilingual"] = self.get_value(
            self.db_aliases_multilingual, title
        )
        responds["types_specific"] = self.get_value(self.db_types_specific, title)
        responds["types_transitive"] = self.get_value(self.db_types_transitive, title)
        responds["facts_entity"] = self.get_value(self.db_facts_entity, title)
        responds["facts_literal"] = self.get_value(self.db_facts_literal, title)
        return responds

    def size(self):
        return self.get_db_size(self.db_label)

    def keys(self):
        return self.get_db_iter(self.db_label, get_values=False)

    def items(self):
        for k in self.keys():
            v = self.get_item(k)
            yield k, v

    def _parse_ttl_1_1(self, file, db, message, compress_value=True):
        """
        Parse DBpedia dump file as subject - predicate - object: 1 - 1 relation
        :param file:
        :type file:
        :param db:
        :type db:
        :param buff_limit:
        :type buff_limit:
        :param compress:
        :type compress:
        :return:
        :rtype:
        """
        buff_objs = []
        for line in tqdm(iw.read_line_from_file(file), desc=message):
            if "dbpedia" not in str(line):
                continue

            respond = ul.parse_triple_line(line)
            if not respond:
                continue

            dp_title, _, dp_value = respond
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if dp_title:
                dp_value = str(from_n3_fix(dp_value))
                if dp_value:
                    buff_objs.append((dp_title, dp_value))

        if buff_objs:
            self.write_bulk_with_buffer(
                self._env, db, data=buff_objs, compress_value=compress_value
            )

    def _parse_ttl_1_n(self, file, db, message, compress_value=True):
        buff_trans = defaultdict(set)
        mapping_id = MappingID()

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
                    dp_obj = mapping_id.get_redirect_dbpedia(dp_obj)
                    if dp_obj:
                        buff_trans[dp_title].add(dp_obj)

                elif cf.WD in dp_obj:
                    dp_obj = dp_obj.replace(cf.WD, "")
                    if dp_obj and ul.is_wd_item(dp_obj):
                        dp_type_mapper = mapping_id.get_dbpedia_from_wikidata(dp_obj)
                        if dp_type_mapper:
                            buff_trans[dp_title].add(dp_type_mapper)

        if buff_trans:
            self.write_bulk_with_buffer(
                self._env, db, data=buff_trans, compress_value=compress_value
            )

    def _parse_aliases(self):
        buff_obj = defaultdict(set)
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
                dp_value = str(from_n3_fix(dp_value))
                if dp_value:
                    dp_value = ul.norm_wikipedia_title(dp_value, unquote=True)
                    disambiguation_aliases[dp_value].add(dp_title)

        for dp_title in tqdm(self.keys()):
            dp_obj = self.get_value(self.db_facts_literal, dp_title)
            for entity_name in ["otherName", "alias"]:
                if dp_obj and dp_obj["string"].get(entity_name):
                    for dp_label in dp_obj["string"][entity_name]:
                        if (
                            "(disambiguation)" in dp_label
                            and "(disambiguation)" not in dp_title
                        ):
                            continue
                        buff_obj[dp_title].add(dp_label)

            redirects_of = mapping_id.get_redirect_of_dbpedia(dp_title)
            if redirects_of:
                for dp_label in redirects_of:
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_obj[dp_title].add(dp_label)

            ambiguous_labels = disambiguation_aliases.get(dp_title)
            if ambiguous_labels:
                for dp_label in ambiguous_labels:
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_obj[dp_title].add(dp_label)

        if buff_obj:
            self.write_bulk_with_buffer(self._env, self.db_aliases_en, buff_obj)

    def _parse_aliases_multilingual(self):
        buff_obj = defaultdict(set)
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
                    dp_value = str(from_n3_fix(dp_value))
                    if dp_value:
                        buff_obj[dp_title].add(dp_value)

        if buff_obj:
            self.write_bulk_with_buffer(
                self._env, self.db_aliases_multilingual, buff_obj
            )

    def _parse_facts(self):
        """
        Parse DBpedia facts from Infobox and mapping of objects and literals
        :return:
        :rtype:
        """
        mapping_id = MappingID()
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
                    dp_value = mapping_id.get_redirect_dbpedia(dp_value)

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
                        dp_value = from_n3_fix(dp_value)
                        if dp_value and isinstance(dp_value, str):
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
                    dp_value = from_n3_fix(dp_value)
                    if dp_value and isinstance(dp_value, str):
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
                dp_value = mapping_id.get_redirect_dbpedia(dp_value)

                if not buff_entities.get(dp_title):
                    buff_entities[dp_title] = defaultdict(set)
                if dp_title and dp_value:
                    buff_entities[dp_title][dp_prop].add(dp_value)

        if buff_entities:
            self.write_bulk_with_buffer(self._env, self.db_facts_entity, buff_entities)

        if buff_literals:
            self.write_bulk_with_buffer(self._env, self.db_facts_literal, buff_literals)

    def build(self):
        start = time()
        iw.print_status("Parse DBpedia dump")
        self._parse_ttl_1_1(
            cf.DIR_DUMP_DP_LABELS,
            self.db_label,
            message="Parse Label",
            compress_value=False,
        )
        self._parse_ttl_1_1(
            cf.DIR_DUMP_DP_DESC,
            self.db_desc,
            message="Parse Desc",
            compress_value=False,
        )
        self._parse_ttl_1_n(
            cf.DIR_DUMP_DP_TYPES_SPECIFIC,
            self.db_types_specific,
            message="Parse Specific Types",
        )
        self._parse_ttl_1_n(
            cf.DIR_DUMP_DP_TYPES_TRANSITIVE,
            self.db_types_transitive,
            message="Parse Transitive Types",
        )
        self._parse_facts()
        self._parse_aliases_multilingual()
        self._parse_aliases()
        iw.print_status(f"DBpedia parsing: {timedelta(seconds=time() - start)}")

    def modify_env_setting(self):
        self.modify_db_compress_value(
            self.db_label, c_compress_value=True, n_compress_value=False
        )
        self.modify_db_compress_value(
            self.db_desc, c_compress_value=True, n_compress_value=False
        )


if __name__ == "__main__":
    m_f.init()
    dp_items = DPItem(cf.DIR_DBPEDIA_ITEMS)
    # dp_items.build()
    # dp_items.modify_env_setting()

    tmp1 = dp_items.get_item("Xiasi Dog")
    tmp2 = dp_items.get_item("James Adams (character)")

    dp_id = "Batman: Arkham City"
    # tmp = dp_items.get_item(dp_id)
    # from api.resources.m_item import MItem

    # items = MItem()
    # tmp2 = items.get_item(dp_id)

    # dp_items.update_claims_literal()
    # dp_items.update_claims_object()
    # items.copy_lmdb()
    # items.head(items._db_labels, 10, compress=True)
    #
    # for k, v in tqdm(items.items(compress=True)):
    #     debug = 1
    # tmp = items.get_item("Pikachu")
    # dp_items.build()
    # items.info()
    # items.copy(compress=False)
    # items = DPItem(cf.DIR_DP_ITEMS + ".copy")
    # items.info()

    iw.print_status("Done")


"""
Parse DBpedia dump
DBpedia infobox: 103711965it [1:44:21, 16563.48it/s]
DBpedia mapping literals: 18375457it [40:15, 7607.74it/s] 
DBpedia mapping objects: 21644739it [11:38, 30998.69it/s]
DB Write: buff:97%: 100%|█████████▉| 6158000/6158379 [00:17<00:00, 360595.87it/s] 
DB Write: buff:59%: 100%|█████████▉| 7118000/7118415 [00:29<00:00, 239257.49it/s]
DBpedia labels: eo: 388568it [00:18, 21113.60it/s]
DBpedia labels: pt: 1183036it [00:53, 22080.26it/s]
DBpedia labels: ru: 1273866it [00:59, 21511.36it/s]
DBpedia labels: fr: 1989581it [01:20, 24615.46it/s]
DBpedia labels: de: 1851154it [01:12, 25504.29it/s]
DBpedia labels: eu: 432120it [00:16, 26744.95it/s]
DBpedia labels: ar: 1100557it [00:48, 22680.38it/s]
DBpedia labels: ja: 1011335it [00:42, 23826.34it/s]
DBpedia labels: ca: 735229it [00:28, 25731.70it/s]
DBpedia labels: cs: 569125it [00:22, 25764.62it/s]
DBpedia labels: ko: 545600it [00:22, 24764.50it/s]
DBpedia labels: es: 1495663it [00:58, 25363.02it/s]
DBpedia labels: ga: 123351it [00:04, 29511.24it/s]
DBpedia labels: zh: 1018368it [00:42, 23767.15it/s]
DBpedia labels: el: 295290it [00:12, 24087.03it/s]
DBpedia labels: it: 1562975it [00:58, 26616.43it/s]
DBpedia labels: nl: 1193961it [00:46, 25804.23it/s]
DBpedia labels: id: 492131it [00:19, 25844.80it/s]
DBpedia labels: uk: 994226it [00:45, 21616.23it/s]
DBpedia labels: sv: 1091984it [00:45, 23950.20it/s]
DBpedia labels: pl: 1283487it [00:54, 23752.08it/s]
DB Write: buff:74%: 100%|█████████▉| 2893000/2893175 [00:04<00:00, 595262.95it/s] 
DBpedia Disambiguation: 1887309it [00:51, 36380.21it/s]
15904559it [04:53, 54190.43it/s]
DB Write: buff:63%: 100%|█████████▉| 3897000/3897759 [00:05<00:00, 748776.79it/s] 
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 0, 'entries': 8}
"""
