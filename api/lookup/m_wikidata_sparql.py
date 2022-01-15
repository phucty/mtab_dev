import logging
from time import sleep
from SPARQLWrapper import SPARQLWrapper, JSON
import api.m_f
from collections import defaultdict
import m_config as cf
from tqdm import *
from multiprocessing.pool import Pool
from contextlib import closing
import sys
from api.utilities import m_sim, m_io as iw
from api.utilities import m_utils as ul
import ssl


class Sparql_Wikidata(object):
    def __init__(self):
        self.URL = "https://query.wikidata.org/sparql"  # 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
        self.WDT = "http://www.wikidata.org/prop/direct/"
        self.WD = "http://www.wikidata.org/entity/"
        self.WDT2 = "http://www.wikidata.org/prop/statement/"
        # user_agent = "%s.%s 13 Aug" % (sys.version_info[0], sys.version_info[1])
        self.sparql = SPARQLWrapper(self.URL)
        self.sparql.setTimeout(100000)
        self.sparql.setReturnFormat(JSON)

    def _retrieve(
        self,
        query,
        params,
        limit=5000,
        _filter=None,
        retries=0,
        offset=0,
        get_status=False,
    ):
        res = []
        has_next = True
        logging.debug(query)
        max_limit = limit
        status = 1
        while has_next:
            try:
                q = query  # + '\nLIMIT %s\nOFFSET %d' % (limit, offset)
                self.sparql.setQuery(q)
                self.sparql.setMethod("POST")
                result_query = self.sparql.query().convert()
                results = result_query["results"]["bindings"]

                for result in results:
                    tup = []
                    for p in params:
                        if p in result.keys():
                            tup.append(result[p]["value"])
                        else:
                            tup.append("")

                    # tup = [result[p]['value'] for p in params]
                    if _filter:
                        if all([_f(t) for t, _f in zip(tup, _filter)]):
                            res.append(tup)
                    else:
                        res.append(tup)

                offset += limit
                if len(res) < offset:
                    logging.debug("Received Data: " + str(len(res)))
                    has_next = False
                elif len(res) % limit == 0:
                    iw.print_status(
                        "Received Data: %d - %d"
                        % (len(res), len({wd for wd, _ in res}))
                    )

                if limit * 2 > max_limit:
                    limit = max_limit
                else:
                    limit = limit * 2
                status = 0
            except Exception as e:
                if "HTTP Error 429: Too Many Requests" in str(e):
                    sleep(60)
                    iw.print_status(
                        "HTTP Error 429: Too Many Requests: wait 100", is_screen=True
                    )
                    return self._retrieve(
                        query,
                        params,
                        limit,
                        _filter,
                        retries - 1,
                        get_status=get_status,
                    )
                # if retries > 0:
                #     iw.print_status('Connection lost: %s. Try again...(%d)' % (e, retries), is_screen=False)
                #     sleep(10)
                #     return self._retrieve(query, params, limit, filter, retries - 1)
                # else:
                else:
                    has_next = False
                    iw.print_status(
                        "Connection error: %s" % str(e)[:100], is_screen=True
                    )
                    iw.print_status(query, is_screen=False)
        if get_status:
            return status, res
        else:
            return res

    def get_wikidata_properties(self):
        query = (
            "SELECT DISTINCT ?p ?pLabel ?pDescription ?pAltLabel WHERE { \n"
            "  ?p wikibase:propertyType ?pType . \n"
            '  SERVICE wikibase:label {bd:serviceParam wikibase:language "en" }  \n'
            "}"
        )
        responds = self._retrieve(
            query, ["p", "pLabel", "pDescription", "pAltLabel"], limit=10000
        )
        responds = [
            [p, pLabel, pDescription, pAltLabel]
            for p, pLabel, pDescription, pAltLabel in responds
        ]
        if responds and len(responds):
            for p, pLabel, pDescription, pAltLabel in responds:
                p = p.replace(self.WD, "")
                wd_types_direct = set()
                wd_claims_wd = defaultdict(set)
                wd_claim_literal = {
                    "string": defaultdict(set),
                    "time": defaultdict(set),
                    "quantity": defaultdict(set),
                }

                for prop, wd_id, wd_label in self.get_wikidata_item_info(p):
                    if prop == "P31":
                        wd_types_direct.add(wd_id.replace(self.WD, ""))
                    else:
                        if self.WD in wd_id:
                            wd_claims_wd[prop].add(wd_id.replace(self.WD, ""))
                        else:
                            wd_claim_literal["string"][prop].add(wd_label)
                yield p, {
                    "label": pLabel,
                    "description": pDescription,
                    "aliases": pAltLabel.split(", "),
                    "links": 0,
                    "types_direct": wd_types_direct,
                    "claims_wd": wd_claims_wd,
                    "claim_literal": wd_claim_literal,
                }

    def get_wikidata_item_info(self, wd_id):
        query = (
            "SELECT DISTINCT ?p ?v ?vLabel WHERE { \n"
            "  wd:%s ?p ?v. ?propertyItem wikibase:directClaim ?p. \n"
            '  SERVICE wikibase:label {bd:serviceParam wikibase:language "en" }  \n'
            "}" % wd_id
        )
        responds = self._retrieve(query, ["p", "v", "vLabel"])
        if responds and len(responds):
            responds = [
                [respond[0].replace(self.WDT, ""), respond[1], respond[2]]
                for respond in responds
            ]
        return responds

    def get_same_as(self, wd_id):
        query = (
            "SELECT DISTINCT ?item WHERE { \n"
            "  {wd:%s owl:sameAs ?item} \n"
            "  UNION {?item owl:sameAs wd:%s}  \n"
            "}" % (wd_id, wd_id)
        )
        responds = self._retrieve(query, ["item"])
        if responds:
            responds = [ul.norm_namespace(respond[0]) for respond in responds]
            responds = [respond for respond in responds if ul.is_wd_item(respond)]
        return responds

    def get_wikidata_item_text_andInfo(self, wd_id):
        query = (
            "SELECT DISTINCT ?p ?pLabel ?pDescription ?pAltLabel WHERE { \n"
            "  VALUES ?p {wd:%s} \n"
            '  SERVICE wikibase:label {bd:serviceParam wikibase:language "en" }  \n'
            "}" % wd_id
        )

        responds = self._retrieve(query, ["p", "pLabel", "pDescription", "pAltLabel"])
        if responds:
            for p, pLabel, pDescription, pAltLabel in responds:
                p = p.replace(self.WD, "")
                wd_types_direct = set()
                wd_claims_wd = defaultdict(set)
                wd_claim_literal = {
                    "string": defaultdict(set),
                    "time": defaultdict(set),
                    "quantity": defaultdict(set),
                }

                for prop, wd_id, wd_label in self.get_wikidata_item_info(p):
                    if prop == "P31":
                        wd_types_direct.add(wd_id.replace(self.WD, ""))
                    else:
                        if self.WD in wd_id:
                            wd_claims_wd[prop].add(wd_id.replace(self.WD, ""))
                        else:
                            wd_claim_literal["string"][prop].add(
                                wd_label.replace("T00:00:00Z", "")
                            )
                return (
                    p,
                    {
                        "label": pLabel,
                        "description": pDescription,
                        "aliases": pAltLabel.split(", "),
                        "links": 0,
                        "types_direct": wd_types_direct,
                        "claims_wd": wd_claims_wd,
                        "claim_literal": wd_claim_literal,
                    },
                )
        else:
            return wd_id, responds

    def get_stars_name(self):
        query = (
            "SELECT DISTINCT ?s ?sLabel { \n"
            "  ?s wdt:P31/wdt:P279* wd:Q523. \n"
            '  SERVICE wikibase:label {bd:serviceParam wikibase:language "en" }  \n'
            "}"
        )
        responds = self._retrieve(query, ["s", "sLabel"])
        if responds and len(responds):
            responds = {s.replace(self.WD, ""): s_label for s, s_label in responds}
            iw.save_obj_pkl(
                "%s/temp/Round%d/star.pkl" % (st.DIR_ROOT, st.ROUND), responds
            )
        return responds

    def get_labels(self, items, item_limit=200):
        count = 0
        input_id = ""
        res_types = defaultdict(set)
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT ?s ?sLabel { \n"
                    "  VALUES ?s { %s} \n"
                    '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }  \n'
                    "}" % input_id
                )
                for s, sLabel in self._retrieve(query, ["s", "sLabel"]):
                    s = s.replace(self.WD, "")
                    if len(sLabel):
                        if sLabel == s:
                            res_types[s] = set()
                        else:
                            res_types[s].add(sLabel)

                input_id = ""
                count = 0
        return res_types

    def get_also_know_as(self, items, item_limit=100):
        count = 0
        input_id = ""
        res_types = {item: set() for item in items}
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT ?s ?sLabel { \n"
                    "  VALUES ?s { %s} \n"
                    "  ?s skos:altLabel ?sLabel. \n"
                    "  FILTER(lang(?sLabel)='en') \n"
                    "}" % input_id
                )
                for s, sLabel in self._retrieve(query, ["s", "sLabel"]):
                    res_types[s.replace(self.WD, "")].add(sLabel)
                input_id = ""
                count = 0
        return res_types

    def get_redirect(self, items, item_limit=100):
        count = 0
        input_id = ""
        res_types = defaultdict()
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT ?s ?s2 { \n"
                    "  VALUES ?s { %s} \n"
                    "  ?s owl:sameAs ?s2. \n"
                    "}" % input_id
                )
                for s, sLabel in self._retrieve(query, ["s", "s2"]):
                    res_types[s.replace(self.WD, "")] = sLabel.replace(self.WD, "")
                input_id = ""
                count = 0
        return res_types

    def get_equivalent_types(self, items, item_limit=100):
        count = 0
        input_id = ""
        res_types = {item: set() for item in items}
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT ?s ?s1 { \n"
                    "  VALUES ?s { %s} \n"
                    "  {?s wdt:P1709 ?s1.} \n"
                    "  UNION {?s1 wdt:P1709 ?s.} \n"
                    "  UNION {?s wdt:P2888 ?s1.} \n"
                    "  UNION {?s1 wdt:P2888 ?s.} \n"
                    "}" % (input_id)
                )
                for s, s_type in self._retrieve(query, ["s", "s1"]):
                    if "http://dbpedia.org/ontology/" in s_type:
                        s_type = s_type.replace("http://dbpedia.org/ontology/", "")
                        res_types[s.replace(self.WD, "")].add(s_type)
                input_id = ""
                count = 0
        return res_types

    def get_types(self, items, item_limit=10):
        count = 0
        input_id = ""
        res_types = defaultdict(list)
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT DISTINCT ?s ?t { \n"
                    "  VALUES ?s { %s} \n"
                    "  ?s wdt:P31/wdt:P279* ?t. \n"
                    "}" % (input_id)
                )
                for s, s_type in self._retrieve(query, ["s", "t"]):
                    res_types[s.replace(self.WD, "")].append(
                        s_type.replace(self.WD, "")
                    )
                input_id = ""
                count = 0
        return res_types

    def get_types_direct(self, items, item_limit=10):
        count = 0
        input_id = ""
        res_types = defaultdict(set)
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                query = (
                    "SELECT DISTINCT ?s ?t { \n"
                    "  VALUES ?s { %s} \n"
                    "  ?s wdt:P31 ?t. \n"
                    "}" % (input_id)
                )
                for s, s_type in self._retrieve(query, ["s", "t"]):
                    res_types[s.replace(self.WD, "")].add(s_type.replace(self.WD, ""))
                input_id = ""
                count = 0
        return res_types

    def get_lk_two_cell(self, main_text, other_id, item_limit=10):
        if not other_id or not len(other_id) or other_id[0] not in ["Q", "P"]:
            return []
        query = (
            "SELECT ?s ?sLabel { \n"
            "  ?s ?p wd:%s. \n"
            "  ?x wikibase:directClaim ?p. \n"
            "  ?s rdfs:label ?sLabel. \n"
            '  FILTER((LANG(?sLabel)) = "en") \n'
            '  FILTER CONTAINS(?sLabel, "%s") . \n'
            "}" % (other_id, main_text)
        )
        responds = self._retrieve(query, ["s", "sLabel"])
        if responds and len(responds):
            responds = [[s.replace(self.WD, ""), s_label] for s, s_label in responds]
        return responds

    def _get_facet_two_cell(self, core_text_org, other_text):
        core_text = core_text_org
        if not core_text or not len(core_text) or not other_text or not len(other_text):
            return []

        core_text = core_text.replace('"', '\\"')
        other_text = other_text.replace('"', '\\"')
        other_text = other_text.replace("dasdasj", "caesium")
        other_text = other_text.replace("gramophone record", "vinyl record")
        other_text = other_text.replace(
            "electric multiple unit train", "electric multiple unit"
        )

        query = (
            "SELECT DISTINCT ?item ?p ?v ?ordinal{ \n"
            "  { ?item ?p ?v. \n"
            '    OPTIONAL {?v rdfs:label ?vL FILTER (LANG(?vL) = "en")}\n'
            "  } \n"
            "  UNION { ?item ?ps [?p ?v]. \n"
            "          ?wd wikibase:claim ?ps; wikibase:statementProperty ?p. \n"
            '          OPTIONAL {?v rdfs:label ?vL FILTER (LANG(?vL) = "en")} \n'
            "        } \n"
            '  FILTER(CONTAINS(LCASE(?v), "%s") || (CONTAINS(LCASE(?vL), "%s"))) \n'
            "  SERVICE wikibase:mwapi { \n"
            '    bd:serviceParam wikibase:endpoint "www.wikidata.org"; \n'
            '                    wikibase:api "EntitySearch"; \n'
            '                    mwapi:search "%s"; \n'
            '                    mwapi:language "en". \n'
            "    ?item wikibase:apiOutputItem mwapi:item. \n"
            "    ?ordinal wikibase:apiOrdinal true . \n"
            "  } \n"
            "} \nORDER BY ?ordinal" % (other_text, other_text, core_text)
        )

        responds = self._retrieve(query, ["item", "p", "v", "ordinal"])
        res_score = defaultdict(int)
        res = defaultdict()
        if responds and len(responds):
            for cell_1, prop, cell_2, ordinal in responds:
                cell_1 = cell_1.replace(self.WD, "")
                prop = prop.replace(self.WDT, "")
                prop = prop.replace(self.WDT2, "")
                ordinal = int(ordinal)
                if not ul.is_wd_item(prop):
                    continue
                res_score[cell_1] = ordinal
                if not res.get(cell_1):
                    res[cell_1] = {"items": set(), "text": set()}
                if self.WD in cell_2:
                    res[cell_1]["items"].add((prop, cell_2.replace(self.WD, "")))
                else:
                    res[cell_1]["items"].add((prop, cell_2))
        responds = sorted(res_score.items(), key=lambda x: x[1], reverse=True)
        responds = [
            {"core": cell_1, "core_label": core_text_org, "other": res[cell_1]}
            for cell_1, _ in responds
        ]
        return responds

    def _get_facet_two_cell_values(self, core_wds, other_text):
        if not core_wds or not len(core_wds) or not other_text or not len(other_text):
            return []

        wds = " ".join(["wd:%s" % wd for wd, _ in core_wds[:20]])
        if isinstance(other_text, str):
            other_text = other_text.lower()
            other_text = other_text.replace('"', '\\"')
            other_text = other_text.replace("dasdasj", "caesium")
            other_text = other_text.replace("gramophone record", "vinyl record")
            other_text = other_text.replace(
                "electric multiple unit train", "electric multiple unit"
            )

            query = (
                "SELECT DISTINCT ?item ?itemLabel ?p ?v{ \n"
                "  VALUES ?item {%s}. \n"
                "  { ?item ?p ?v. \n"
                '    OPTIONAL {?v rdfs:label ?vL FILTER (LANG(?vL) = "en")}\n'
                "  } \n"
                "  UNION { ?item ?ps [?p ?v]. \n"
                "          ?wd wikibase:claim ?ps; wikibase:statementProperty ?p. \n"
                '          OPTIONAL {?v rdfs:label ?vL FILTER (LANG(?vL) = "en")} \n'
                "        } \n"
                '  FILTER(CONTAINS(LCASE(?v), "%s") || (CONTAINS(LCASE(?vL), "%s"))) \n'
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }  \n'
                "}" % (wds, other_text, other_text)
            )

        else:
            other_text = " ".join(["wd:%s" % wd for wd, _ in other_text[:20]])

            query = (
                "SELECT DISTINCT ?item ?itemLabel ?p ?v{ \n"
                "  VALUES ?item {%s}. \n"
                "  VALUES ?v {%s}. \n"
                "  { ?item ?p ?v. \n"
                "  } \n"
                "  UNION { ?item ?ps [?p ?v]. \n"
                "          ?wd wikibase:claim ?ps; wikibase:statementProperty ?p. \n"
                "        } \n"
                '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }  \n'
                "}" % (wds, other_text)
            )

        responds = self._retrieve(query, ["item", "itemLabel", "p", "v"])
        res_score = defaultdict(int)
        res = defaultdict()
        res_labels = defaultdict()
        if responds and len(responds):
            for cell_1, cell_1_label, prop, cell_2 in responds:
                cell_1 = cell_1.replace(self.WD, "")
                prop = prop.replace(self.WDT, "")
                prop = prop.replace(self.WDT2, "")
                if not ul.is_wd_item(prop):
                    continue
                ordinal = 0
                res_labels[cell_1] = cell_1_label
                res_score[cell_1] = ordinal
                if not res.get(cell_1):
                    res[cell_1] = {"items": set(), "text": set()}
                if self.WD in cell_2:
                    res[cell_1]["items"].add((prop, cell_2.replace(self.WD, "")))
                else:
                    res[cell_1]["items"].add((prop, cell_2))
        responds = sorted(res_score.items(), key=lambda x: x[1], reverse=True)
        responds = [
            {"core": cell_1, "core_label": res_labels[cell_1], "other": res[cell_1]}
            for cell_1, _ in responds
        ]
        return responds

    def _get_facet_two_cell_2(self, core_text_org, other_text):
        if (
            not core_text_org
            or not len(core_text_org)
            or not other_text
            or not len(other_text)
            or other_text
            in [
                "France",
                "United States of America",
                "Germany",
                "People's Republic of China",
                "United Kingdom",
                "Russia",
                "paper",
                "human",
                "Netherlands",
                "blue",
                "Latin script",
            ]
        ):
            return []

        core_text = core_text_org.replace('"', '\\"')
        other_text = other_text.replace('"', '\\"')
        other_text = other_text.replace(" f ", " of ")
        other_text = other_text.replace("dasdasj", "caesium")
        other_text = other_text.replace("gramophone record", "vinyl record")
        other_text = other_text.replace(
            "electric multiple unit train", "electric multiple unit"
        )

        query = (
            "SELECT ?v { \n"
            "  SERVICE wikibase:mwapi { \n"
            '    bd:serviceParam wikibase:endpoint "www.wikidata.org"; \n'
            '                    wikibase:api "EntitySearch"; \n'
            '                    mwapi:search "%s"; \n'
            '                    mwapi:language "en". \n'
            "    ?v wikibase:apiOutputItem mwapi:item. \n"
            "  } \n"
            "}" % (other_text)
        )
        responds = self._retrieve(query, ["v"])
        if len(responds) > 50 or not len(responds):
            return []

        query = (
            "SELECT DISTINCT ?item ?itemL ?p ?v{ \n"
            "  ?item ?p ?v. \n"
            '  ?item rdfs:label ?itemL FILTER (LANG(?itemL) = "en"). \n'
            "  SERVICE wikibase:mwapi { \n"
            '    bd:serviceParam wikibase:endpoint "www.wikidata.org"; \n'
            '                    wikibase:api "EntitySearch"; \n'
            '                    mwapi:search "%s"; \n'
            '                    mwapi:language "en". \n'
            "    ?v wikibase:apiOutputItem mwapi:item. \n"
            "  } \n"
            "}" % (other_text)
        )

        responds = self._retrieve(query, ["item", "itemL", "p", "v"])
        responds_tmp = defaultdict(list)
        responds_label = defaultdict(str)
        res_score = defaultdict(float)
        if responds and len(responds):
            for wd_1, wd_1_label, prop, wd_2 in responds:
                wd_1 = wd_1.replace(self.WD, "")
                prop = prop.replace(self.WDT, "")

                if not ul.is_wd_item(prop):
                    continue
                wd_2 = wd_2.replace(self.WD, "")
                responds_tmp[wd_1].append([prop, wd_2])
                responds_label[wd_1] = wd_1_label
                res_score[wd_1] = sim.sim_string_fuzz(core_text, wd_1_label)
            res_score = sorted(res_score.items(), key=lambda x: x[1], reverse=True)
            res_score = [
                [cell_1, cell_1_score]
                for cell_1, cell_1_score in res_score[:5]
                if cell_1_score > 0.7
            ]
            responds = self._get_facet_two_cell_values(res_score, other_text)
            # for cell_1, cell_1_score in res_score[:3]:
            #     if cell_1_score > 0.7 and cell_1_score == res_score[0][1]:
            #         tmp_responds = self.get_facet_two_cell(responds_label[cell_1], other_text, is_other=False)
            #         if len(tmp_responds):
            #             final_res.extend(tmp_responds)
            # responds = final_res
            update_keys = {responds_label[respond["core"]] for respond in responds}
            if len(responds) and len(update_keys):
                f.lk_wikidata().update(core_text_org, update_keys)
            # if len(res_score):
            #     responds = [{"core": cell_1, "core_label": responds_label[cell_1], "other": responds_tmp[cell_1]}
            #                 for cell_1, cell_1_score in res_score if cell_1_score > 0.7 and cell_1_score == res_score[0][1]]
        return responds

    def _get_facet_two_cell_2_ngrams(self, core_text, other_text):
        status = 1
        if not core_text or not len(core_text) or not other_text or not len(other_text):
            return status, []

        core_text = core_text.replace('"', '\\"')
        other_text = other_text.replace('"', '\\"')
        other_text = other_text.replace(" f ", " of ")
        other_text = other_text.replace("dasdasj", "caesium")
        other_text = other_text.replace("gramophone record", "vinyl record")
        other_text = other_text.replace(
            "electric multiple unit train", "electric multiple unit"
        )

        query = (
            "SELECT DISTINCT ?item ?itemL ?p ?v{ \n"
            "  ?item ?p ?v. \n"
            "  [] wikibase:directClaim ?p. \n"
            '  ?item rdfs:label ?itemL FILTER (LANG(?itemL) = "en" && CONTAINS(?itemL, "%s")). \n'
            "  SERVICE wikibase:mwapi { \n"
            '    bd:serviceParam wikibase:endpoint "www.wikidata.org"; \n'
            '                    wikibase:api "EntitySearch"; \n'
            '                    mwapi:search "%s"; \n'
            '                    mwapi:language "en". \n'
            "    ?v wikibase:apiOutputItem mwapi:item. \n"
            "  } \n"
            "}" % (core_text, other_text)
        )

        status, responds = self._retrieve(
            query, ["item", "itemL", "p", "v"], get_status=True
        )
        responds_tmp = defaultdict(set)
        responds_label = defaultdict(str)
        if responds and len(responds):
            for wd_1, wd_1_label, prop, wd_2 in responds:
                wd_1 = wd_1.replace(self.WD, "")
                prop = prop.replace(self.WDT, "")
                if not ul.is_wd_item(prop):
                    continue
                wd_2 = wd_2.replace(self.WD, "")
                responds_tmp[wd_1].add((prop, wd_2))
                responds_label[wd_1] = wd_1_label
            if len(responds_label):
                responds = [
                    {
                        "core": cell_1,
                        "core_label": responds_label[cell_1],
                        "other": {"items": responds_tmp[cell_1], "text": set()},
                    }
                    for cell_1, cell_1_score in responds_label.items()
                ]
        return status, responds

    def get_facet_two_cell_2(self, core_text, other_text):
        core_text = core_text.replace("#", " ")
        n_words = len(core_text.split())
        n_gram = n_words
        responds = []
        status = 0
        while not len(responds) and n_gram > 0 and status == 0:
            n_grams = TextBlob(core_text).ngrams(n_gram)
            join_grams = [" ".join(grams) for grams in n_grams]
            for sub_core_text in join_grams:
                status, responds = self._get_facet_two_cell_2_ngrams(
                    sub_core_text, other_text
                )
                if len(responds) or status > 0:
                    break
            n_gram -= 1
        return responds

    def get_facet_two_cell(self, cell_1, cell_2, is_other=False):
        responds = []
        if not is_other:
            # if not f.fix_labels().get(core_text_org):
            #     return responds
            # fixed_cell_1 = set()
            # fixed_cell_1.add(cell_1)
            #
            # fixed_cell_1.update(f.fix_labels().get(cell_1, set()))
            #
            # fixed_cell_1 = {fix_text: sim.sim_string_fuzz(fix_text, cell_1) for fix_text in fixed_cell_1}
            #
            # fixed_cell_1 = sorted(fixed_cell_1.items(), key=lambda x: x[1], reverse=True)

            # fixed_cell_1 = [[core_text_org, 1]]
            # for core_text, core_text_score in fixed_cell_1[:20]:
            #     if core_text_score <= 0.5:
            #         continue
            #     # core_text = core_text_org
            #     other_text = cell_2.lower()
            #     responds = self._get_facet_two_cell(core_text, other_text)

            #     # Check year
            #     if not len(responds) and "/" in other_text:
            #         other_text = other_text.replace("/", "-")
            #         responds = self._get_facet_two_cell(core_text, other_text)
            #
            #     if not len(responds) and "-01-01" in other_text:
            #         responds = self._get_facet_two_cell(core_text, other_text.replace("-01-01", ""))
            #
            #     if not len(responds):
            #         try:
            #             float_text = float(cell_2)
            #             if str(float_text) != cell_2:
            #                 responds = self._get_facet_two_cell(core_text, str(float_text))
            #             if not len(responds):
            #                 responds = self._get_facet_two_cell(core_text, str(int(float_text)))
            #             if not len(responds):
            #                 responds = self._get_facet_two_cell(core_text, str(round(float_text)))
            #         except Exception as message:
            #             iw.print_status(message)
            #             pass
            #
            #     if not len(responds) or "ID" in core_text:
            #         wd_core_text = f.lk_wikidata().run_service_prop(core_text)
            #         if wd_core_text and len(wd_core_text):
            #             wd_other_text = cell_2
            #             responds = self._get_facet_two_cell_values(wd_core_text, wd_other_text)
            #             if not len(responds):
            #                 wd_other_text = f.lk_wikidata().run_service_prop(cell_2)
            #                 responds = self._get_facet_two_cell_values(wd_core_text, wd_other_text)
            #             if len(responds):
            #                 f.lk_wikidata().update(cell_1, wd_core_text)
            #                 if not isinstance(wd_other_text, str):
            #                     f.lk_wikidata().update(cell_2, wd_other_text)
            #
            #     if len(responds):
            #         # iw.print_status("[%s] [%s]" % (core_text_org, core_text), is_screen=False)
            #         break

            # if not len(responds):
            #     responds = self._get_facet_two_cell(f.autocorrect()(core_text), other_text)
            #
            # if not len(responds):
            #     responds = self._get_facet_two_cell(f.correct_spell().check(core_text, distance=0), other_text)
            #
            # if not len(responds):
            #     responds = self._get_facet_two_cell(f.correct_spell().check(core_text, distance=1), other_text)
            #
            # if not len(responds):
            #     responds = self._get_facet_two_cell(f.correct_spell().check(core_text, distance=2), other_text)
            #
            # if not len(responds):
            #     responds = self._get_facet_two_cell(str(TextBlob(core_text).correct()), other_text)

            # Using fix labels
            if not len(responds):
                wd_core_text = f.lk_wikidata().get(cell_1, min_sim=0.7, limit=20)
                if len(wd_core_text):
                    wd_other_text = cell_2
                    responds = self._get_facet_two_cell_values(
                        wd_core_text, wd_other_text
                    )
                    if not len(responds):
                        try:
                            float_text = float(cell_2)
                            if str(float_text) != cell_2:
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(float_text)
                                )
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(int(float_text))
                                )
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(round(float_text))
                                )
                        except:
                            pass
                    if not len(responds):
                        wd_other_text = f.lk_wikidata().get(
                            cell_2, min_sim=0.7, limit=20
                        )
                        responds = self._get_facet_two_cell_values(
                            wd_core_text, wd_other_text
                        )
            if not len(responds):
                wd_core_text = f.lk_fz_res().get(cell_1, [])
                if len(wd_core_text):
                    wd_other_text = cell_2
                    responds = self._get_facet_two_cell_values(
                        wd_core_text, wd_other_text
                    )
                    if not len(responds) and "/" in wd_other_text:
                        wd_other_text = wd_other_text.replace("/", "-")
                        responds = self._get_facet_two_cell_values(
                            wd_core_text, wd_other_text
                        )

                    if not len(responds) and "-01-01" in wd_other_text:
                        responds = self._get_facet_two_cell_values(
                            wd_core_text, wd_other_text.replace("-01-01", "")
                        )

                    if not len(responds):
                        try:
                            float_text = float(cell_2)
                            if str(float_text) != cell_2:
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(float_text)
                                )
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(int(float_text))
                                )
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(round(float_text))
                                )
                            # if not len(responds):
                            #     responds = self._get_facet_two_cell_values(wd_core_text, str(float_text)[:4])
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(float_text)[:3]
                                )
                            if not len(responds):
                                responds = self._get_facet_two_cell_values(
                                    wd_core_text, str(float_text)[:2]
                                )
                        except:
                            pass
                    if not len(responds):
                        wd_other_text = f.lk_fz_res().get(cell_2, [])
                        responds = self._get_facet_two_cell_values(
                            wd_core_text, wd_other_text
                        )

        # if not len(responds):
        #     responds = self._get_facet_two_cell_2(core_text_org, other_text_org)
        if not len(responds) and is_other:
            responds = self._get_facet_two_cell_2(cell_1, cell_2)

        return responds

    def get_entity_info(self, items, item_limit=10, is_direct_prop=False):
        count = 0
        input_id = ""
        responds = {item: [] for item in items}
        for c_i, i in enumerate(items):
            count += 1
            input_id += "wd:{0} ".format(i)
            if count == item_limit or (items[-1] == i and count < item_limit):
                if is_direct_prop:
                    query = (
                        "SELECT DISTINCT ?e ?p ?v ?vLabel { \n"
                        "  VALUES ?e { %s} \n"
                        "  ?e ?p ?v. \n"
                        "  ?x wikibase:directClaim ?p. \n"
                        '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } \n'
                        "}" % input_id
                    )
                    # '  MINUS { ?x wdt:P31/wdt:P279* wd:Q18610173} \n' \
                    # '  MINUS { ?x wdt:P1647 wd:P2699} \n' \
                    # '  MINUS { ?x wdt:P31/wdt:P279* wd:Q19847637} \n' \
                    # '  MINUS { VALUES ?p { wdt:P580 wdt:P625 wdt:P487 wdt:P1332 wdt:P1334 wdt:P1335 ' \
                    # 'wdt:P1813 wdt:1613 wdt:P968 wdt:P1613 wdt:P973}} \n' \
                    # MINUS { ?x wdt:P1647 wd:P2699.}
                    args = ["e", "p", "v", "vLabel"]
                else:
                    query = (
                        "SELECT ?s ?ps ?ps_ ?ps_Label  { \n"
                        "  VALUES ?s { %s} \n"
                        "  ?s ?p ?statement . \n"
                        "  ?statement ?ps ?ps_ . \n"
                        "  ?wd wikibase:claim ?p.  \n"
                        "  ?wd wikibase:statementProperty ?ps. \n"
                        '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } \n'
                        "}" % input_id
                    )
                    args = ["s", "ps", "ps_", "ps_Label"]

                for e, p, v, v_label in self._retrieve(query, args):
                    p = p.replace(self.WDT, "")
                    p = p.replace(self.WDT2, "")
                    v = v.replace("T00:00:00Z", "")
                    v_label = v_label.replace("T00:00:00Z", "")
                    if self.WD in v:
                        v_id = v.replace(self.WD, "")
                        responds[e.replace(self.WD, "")].append([p, v_id, v_label])
                    else:
                        if v != v_label:
                            responds[e.replace(self.WD, "")].append([p, "", v])
                        responds[e.replace(self.WD, "")].append([p, "", v_label])
                input_id = ""
                count = 0
        return responds


def pool_process_a_table(lk_query):
    row_text = [cell["text"] for cell in lk_query]
    main_cell_text = lk_query[0]["text"]
    row_responds = []
    responds_wd = set()
    for other_cell in lk_query[1:]:
        other_wd_id = other_cell["cea"]
        responds = f.sparql_wikidata().get_lk_two_cell(main_cell_text, other_wd_id)
        for wd_id, wd_label in responds:
            if wd_id not in responds_wd:
                responds_wd.add(wd_id)
                row_responds.append(wd_id)
    return tuple(row_text), row_responds


def get_two_cell_lk(n_cpu=2):
    # Merge results
    res1 = iw.load_obj_pkl(
        "%s/temp/Round%d/lk_two_cells_1.pkl" % (st.DIR_ROOT, st.ROUND), is_message=True
    )
    res2 = iw.load_obj_pkl(
        "%s/temp/Round%d/lk_two_cells_2.pkl" % (st.DIR_ROOT, st.ROUND), is_message=True
    )
    for key, responds in res2.items():
        res1_respond = res1.get(key)
        if not res1_respond:
            res1[key] = responds
    iw.save_obj_pkl(
        "%s/temp/Round%d/lk_two_cells.pkl" % (st.DIR_ROOT, st.ROUND),
        res1,
        is_message=True,
    )

    return
    new_queries = iw.load_obj_pkl(st.DIR_LK_QUERIES_GT_WD + "_1")
    try:
        lk_results = iw.load_obj_pkl(
            "%s/temp/Round%d/lk_two_cells.pkl" % (st.DIR_ROOT, st.ROUND),
            is_message=True,
        )
    except:
        lk_results = defaultdict()

    lk_queries = []
    for new_query in new_queries:
        query_text = [cell["text"] for cell in new_query]
        if not lk_results.get(tuple(query_text)):
            # if query_text[0] == "Novel Coronavirus":
            lk_queries.append(new_query)
            #  is None:
    lk_queries.reverse()
    count = 0
    iw.print_status(lk_queries[0])
    with tqdm(total=len(lk_queries)) as p_bar:
        with closing(Pool(processes=n_cpu)) as p:
            for key, responds in p.imap_unordered(pool_process_a_table, lk_queries):
                p_bar.update()
                lk_results[key] = responds
                if len(responds):
                    count += 1
                    p_bar.set_description("OK: %d" % count)
                if count % 50 == 0:
                    iw.save_obj_pkl(
                        "%s/temp/Round%d/lk_two_cells.pkl" % (st.DIR_ROOT, st.ROUND),
                        lk_results,
                        is_message=True,
                    )
    iw.save_obj_pkl(
        "%s/temp/Round%d/lk_two_cells.pkl" % (st.DIR_ROOT, st.ROUND),
        lk_results,
        is_message=True,
    )


def pool_query_lk(lk_query):
    lk_responds = f.sparql_wikidata().get_facet_two_cell(lk_query[1], lk_query[2])
    # lk_responds = []
    # if lk_query[0] == 1:
    #     lk_responds = f.sparql_wikidata().get_facet_two_cell(lk_query[1], lk_query[2], is_other=True)
    # else:
    #     lk_responds = f.sparql_wikidata().get_facet_two_cell(lk_query[1], lk_query[2], is_other=False)
    return lk_query, lk_responds


def lookup_2cells(n_cpu):
    DIR_LK_RES = "%s/temp/Round%d/lk_2cells_res.pkl" % (st.DIR_ROOT, st.ROUND)
    try:
        lk_res = iw.load_obj_pkl(DIR_LK_RES, is_message=True)
    except Exception as message:
        iw.print_status(message)
        lk_res = defaultdict()

    lk_queries = set()
    c = 0
    p_bar = tqdm()
    for lk_query in iw.load_obj_pkl(st.DIR_LK_QUERIES_GT_2CELLS):
        p_bar.update()
        # if lk_query[1] == "Паша Дура" or lk_query[2] == "Паша Дура":
        #     lk_queries.add(lk_query)
        if lk_query[0] != 1:
            continue
        responds = lk_res.get((lk_query[1], lk_query[2]))

        # new_responds = []
        # try:
        #     for respond in responds:
        #         new_other = {"items": set(), "text": set()}
        #         if isinstance(respond["other"], dict):
        #             for prop_id, value_obj in respond["other"]["items"]:
        #                 if ul.is_Wikidata_item(prop_id):
        #                     new_other["items"].add((prop_id, value_obj))
        #             for prop_id, value_obj in respond["other"]["text"]:
        #                 if ul.is_Wikidata_item(prop_id):
        #                     new_other["text"].add((prop_id, value_obj))
        #         else:
        #             for prop_id, value_obj in respond["other"]:
        #                 new_other["items"].add((prop_id, value_obj))
        #
        #         if len(new_other["items"]) or len(new_other["text"]):
        #             new_responds.append({"core": respond["core"],
        #                                  "core_label": respond["core_label"],
        #                                  "other": new_other})
        #     if new_responds != responds:
        #         lk_res[(lk_query[1], lk_query[2])] = new_responds
        #         c += 1
        #         p_bar.set_description("%d Changed" % c)
        # except:
        #     iw.print_status(responds)

        # responds_fuzz = f.lk_fuzzy_search().get(lk_query[1])
        # if responds_fuzz and 20 > len(responds_fuzz) > 0:
        #     set_fuzz = {r for r, _ in responds_fuzz}
        #     set_responds = {r['core'] for r in responds}
        #     set_minus = set_responds - set_fuzz
        #     if len(set_minus) > 0:
        #         lk_queries.add(lk_query)
        #         iw.print_status("\n%s" % str(lk_query), is_screen=False)
        #         for r_i, r in enumerate(responds[:5]):
        #             iw.print_status("%d. %s - %s" % (r_i, r['core'], r['core_label']), is_screen=False)
        #         for r_i, r in enumerate(responds_fuzz[:5]):
        #             iw.print_status("%d. %s - %s" % (r_i, r[0], r[1]), is_screen=False)

        if not responds:
            #     # if not lk_res.get((lk_query[1], lk_query[2])) and lk_query[0] == 1:   and fix_label.get(lk_query[1])
            #     # or f.fix_labels().get(lk_query[1]) and lk_query[0] == 1  and lk_query[0] == 1
            #     # if not lk_res.get((lk_query[1], lk_query[2])) \
            #     #          and not fix_label.get(lk_query[1]):
            lk_queries.add(lk_query)
        else:
            iw.print_status(
                "\nOK:[%s]-%d| %s | %s"
                % (lk_query[0], len(responds), lk_query[1], lk_query[2]),
                is_screen=False,
            )
            for respond_i, respond in enumerate(responds):
                if isinstance(respond["other"], dict):
                    others_info = respond["other"]["items"]
                    others_info.update(respond["other"]["text"])
                else:
                    others_info = respond["other"]
                for prop_id, value in others_info:
                    if ul.is_wd_item(value):
                        value_label = str(f.wikidata_info().get_labels(value))
                    else:
                        value_label = ""
                    iw.print_status(
                        "%d. %s[%s] - %s - %s[%s]"
                        % (
                            respond_i + 1,
                            respond.get("core", ""),
                            respond.get("core_label", ""),
                            prop_id,
                            value,
                            value_label,
                        ),
                        is_screen=False,
                    )

        # else:
        #     new_responds = []
        #     for respond in responds:
        #         if not respond.get('core_label'):
        #             respond['core_label'] = f.wikidata_info().get_labels(respond['core'])
        #         new_responds.append(respond)
        #     lk_res[(lk_query[1], lk_query[2])] = new_responds

        if len(lk_queries) > 1000:
            break

    # iw.save_obj_pkl(DIR_LK_RES, lk_res)
    # return

    lk_queries = sorted(list(lk_queries), key=lambda x: x[1], reverse=True)
    count = 0
    error = 0
    is_change = False
    error_core = set()
    with closing(Pool(processes=n_cpu)) as p:
        with tqdm(total=len(lk_queries)) as p_bar:
            # for lk_query in lk_queries:
            #     key, responds = pool_query_lk(lk_query)
            #     key = lk_query
            for key, responds in p.imap_unordered(pool_query_lk, lk_queries):
                p_bar.update()

                if len(responds):
                    lk_res[(key[1], key[2])] = responds
                    count += 1
                    is_change = True
                    # for respond in responds:
                    #     core_label_text = respond.get("core_label")
                    # if core_label_text and core_label_text != key[1]:
                    #     fix_label[key[1]].add(core_label_text)
                    # responds_text = "%d - %s" % (len(responds),
                    #                              " ".join(["%s[%s]" % (respond.get("core", ""),
                    #                                                    respond.get("core_label", ""))
                    #                                        for respond in responds]))
                    # iw.print_status("OK:[%s] %s | %s | %s" % (key[0], key[1], key[2], responds_text), is_screen=False)

                    iw.print_status(
                        "\nOK:[%s]-%d| %s | %s"
                        % (key[0], len(responds), key[1], key[2]),
                        is_screen=False,
                    )
                    for respond_i, respond in enumerate(responds):
                        if isinstance(respond["other"], dict):
                            others_info = respond["other"]["items"]
                            others_info.update(respond["other"]["text"])
                        else:
                            others_info = respond["other"]
                        for prop_id, value in others_info:
                            if ul.is_wd_item(value):
                                value_label = str(f.wikidata_info().get_labels(value))
                            else:
                                value_label = ""
                            iw.print_status(
                                "%d. %s[%s] - %s - %s[%s]"
                                % (
                                    respond_i + 1,
                                    respond.get("core", ""),
                                    respond.get("core_label", ""),
                                    prop_id,
                                    value,
                                    value_label,
                                ),
                                is_screen=False,
                            )

                else:
                    iw.print_status(
                        "\nError:[%s] %s | %s | %s"
                        % (key[0], key[1], key[2], str(f.lk_fz_res().get(key[1]))),
                        is_screen=False,
                    )
                    # if key[0] == 1:
                    error += 1
                    error_core.add(key[1])
                p_bar.set_description(
                    "Success: %d - Error: %d - Str: %d"
                    % (count, error, len(error_core))
                )
                if is_change and count and count % 1000 == 0:
                    iw.save_obj_pkl(DIR_LK_RES, lk_res)
                    is_change = False
            iw.save_obj_pkl(DIR_LK_RES, lk_res)


def add_fix_stars():
    DIR_LK_RES = "%s/temp/Round%d/lk_2cells_res.pkl" % (st.DIR_ROOT, st.ROUND)
    DIR_LK_FIX = "%s/temp/Round%d/lk_2cells_fix_10.pkl" % (st.DIR_ROOT, st.ROUND)
    try:
        lk_res = iw.load_obj_pkl(DIR_LK_RES, is_message=True)
    except Exception as message:
        iw.print_status(message)
        lk_res = defaultdict()

    star_names = iw.load_obj_pkl("%s/temp/Round%d/star_1.pkl" % (st.DIR_ROOT, st.ROUND))
    star_names_labels = star_names.values()
    for lk_query in iw.load_obj_pkl(st.DIR_LK_QUERIES_GT_2CELLS):
        if not lk_res.get((lk_query[1], lk_query[2])) and lk_query[0] == 0:
            correct_labels = {
                star_name: sim.sim_string_fuzz(lk_query[1], star_name)
                for star_name in star_names_labels
            }

            correct_labels = [
                star_name
                for star_name, sim_score in correct_labels.items()
                if sim_score > 0.7
            ]
            if len(correct_labels):
                f.fix_labels()[lk_query[1]] = correct_labels

    iw.save_obj_pkl(DIR_LK_FIX, f.fix_labels())


def get_prop_info():
    buf_entity = defaultdict()
    for wd_id, entity_obj in tqdm(f.sparql_wikidata().get_wikidata_properties()):
        buf_entity[wd_id] = entity_obj

    iw.save_obj_pkl("%s/prop_info.pkl" % st.DIR_TEMP, buf_entity)


if __name__ == "__main__":
    ssl._create_default_https_context = ssl._create_unverified_context
    wd_query = Sparql_Wikidata()
    tmp = wd_query.get_same_as("Q30")

    # f.init()
    # DIR_LOCAL = "%s/lk_fuzzysearch.pkl" % st.DIR_TEMP
    # lk_responds = iw.load_obj_pkl(DIR_LOCAL)
    #
    # iw.save_obj_pkl(DIR_LOCAL, lk_responds)

    # tmp = f.sparql_wikidata().get_entity_info(["Q16156064"])

    # tmp = f.sparql_wikidata().get_facet_two_cell("Brookhaven College", "Dallas County Community College District")
    # exit()
    # f.init()
    # get_prop_info()
    # f.sparql_wikidata().get_stars_name()
    # add_fix_stars()
    # exit()
    # f.lk_wikidata()
    # caesium-117 | dasdasj
    # f.sparql_wikidata().get_facet_two_cell("|ttrium89", "748105.1416559999", is_other=False)
    # f.sparql_wikidata().get_facet_two_cell_2("218 Giro d'Italia Stage 17", "Chris Froome")
    # pool_query_lk((0, "1907 U.S. National Championships – Women'v Singles", "1907/01/01"))
    # lookup_2cells(n_cpu=3)
    # f.lk_wikidata().save()
    # f.gg_spell().save()
    # exit()

    # f.sparql_wikidata().get_facet_two_cell("kingston", "Canada")

    # print(f.sparql_wikidata().get_labels(["Q76", "Q2618087", "Q672805"]))
    # get_two_cell_lk(n_cpu=3)
    # exit()
    # tmp = f.sparql_wikidata().get_entity_info(["Q16156064"])
    # f.sparql_wikidata().get_also_know_as(["Q13442814"])
    #
    # tmp = f.sparql_wikidata().get_entity_info(["Q2478344"])
    #
    # print(f.sparql_wikidata().get_types(["Q76", "Q2618087", "Q672805"]))
    # print(f.sparql_wikidata().get_entity_info(["Q76", "Q2618087", "Q672805"]))

    # CEA 0.989411809919353 0.990
    # CPA 0.99500301619743 0.995
    # CTA 0.974296392024638 0.974
