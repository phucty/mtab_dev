import requests
import f
import re
import setting as st
from bs4 import BeautifulSoup
from multiprocessing.pool import Pool
from tqdm import *
from contextlib import closing
from collections import defaultdict
from utilities import similarities as sim, io_worker as iw
import utils as ul


class Lookup_Wikidata(object):
    def __init__(self):
        self.KEYWORD = "https://www.wikidata.org/w/api.php"
        self.HEADER = {"Accept": "application/json"}
        self.dir_file = st.DIR_LK_RES_WIKIDATA
        self._data = None
        self.load_data()

    def load_data(self):
        try:
            self._data = iw.load_obj_pkl(self.dir_file, is_message=True)
        except Exception as message:
            iw.print_status(message)
            self._data = defaultdict()

    def run_service(self, query_string, max_hits=100, near_match=False):
        results = []
        if not len(query_string):
            return results
        if not near_match:
            args = {
                "action": "query",
                "list": "search",
                "format": "json",
                "srsearch": query_string,
                "srprop": "titlesnippet|snippet",
                "srlimit": max_hits,
                "srenablerewrites": True,
            }
        else:
            args = {
                "action": "query",
                "list": "search",
                "format": "json",
                "srsearch": query_string,
                "srprop": "titlesnippet|snippet",
                "srlimit": max_hits,
                "srwhat": "nearmatch",
                "srenablerewrites": True,
            }
        try:
            responds = requests.get(self.KEYWORD, params=args).json(encoding="utf8")
            for respond in responds["query"]["search"]:
                title_id = respond["title"]
                title_label = ""
                if respond.get("titlesnippet"):
                    title_label = BeautifulSoup(
                        respond["titlesnippet"], features="lxml"
                    ).get_text()
                results.append([title_id, title_label])
        except Exception as e:
            iw.print_status("%s. %s" % (e, query_string))

        if not len(results) and not near_match:
            results = self.run_service_prop(query_string, max_hits)

        # if not len(results) and not near_match:
        #     results = self.run_service(query_string, max_hits, near_match=True)

        new_labels = []
        for wd, wd_label in results:
            update_item = wd  # [wd, wd_label]
            # if not len(wd_label):
            #     new_label = f.wikidata_info().get_labels(wd)
            #     if new_label and len(new_label) and new_label != wd:
            #         update_item = [wd, new_label]
            #         is_update = True

            new_labels.append(update_item)
        results = new_labels
        return results

    def run_service_prop(self, query_string, max_hits=100):
        results = []
        if not len(query_string):
            return results
        args = {
            "action": "wbsearchentities",
            "format": "json",
            "search": query_string,
            "language": "en",
            "limit": max_hits,
            "type": "property",
        }
        try:
            responds = requests.get(self.KEYWORD, params=args).json(encoding="utf8")
            for respond in responds["search"]:
                title_id = respond["id"]
                # title_label = respond["label"]
                # results.append([title_id, title_label])
                results.append(title_id)
        except Exception as e:
            iw.print_status("%s. %s" % (e, query_string))
        return results

    def items(self):
        for key, value in self._data.items():
            yield key, value

    def get_data(self, query_text):
        return self._data.get(query_text)

    def get(self, query_text, min_sim=0.0, limit=100):
        responds = self._data.get(query_text)

        return responds
        #
        # if responds and min_sim:
        #     score_text_fixs = {(wd_id, wd_label): sim.sim_string_fuzz(wd_label, query_text)
        #                       for wd_id, wd_label in responds}
        #     score_text_fixs = sorted(score_text_fixs.items(), key=lambda x: x[1], reverse=True)
        #     score_text_fixs = [[wd_id, wd_label]
        #                       for (wd_id, wd_label), sim_score in score_text_fixs if sim_score > min_sim]
        #     responds = score_text_fixs
        #
        # if min_sim > 0 and (not responds or not len(responds)):
        #     responds = self.get(query_text)
        #
        # if responds and len(responds):
        #     return responds[:limit]
        # else:
        #     return []

    def update(self, query_text, responds):
        self._data[query_text] = responds

    def save(self):
        iw.save_obj_pkl(self.dir_file, self._data)

    def save_data(self):
        self._data = {
            key: [wd for wd, _ in responds] for key, responds in self._data.items()
        }
        iw.save_obj_pkl(self.dir_file, self._data)

    def search(self, query_string_org, max_hits=st.max_hits):
        responds = []
        # if not len(responds):
        #     fix_texts = f.fix_labels().get(query_string_org, set())
        #     core_text_fixs = {core_text_fix: sim.sim_string_fuzz(core_text_fix, query_string_org)
        #                       for core_text_fix in fix_texts}
        #
        #     core_text_fixs = sorted(core_text_fixs.items(), key=lambda x: x[1], reverse=True)
        #     # core_text_fixs = [[text_fix, text_score] for text_fix, text_score in core_text_fixs if text_score > 0.7]
        #     # core_text_fixs = [[core_text_org, 1]]
        #     for core_text, _ in core_text_fixs:
        #         responds = self.run_service(core_text, max_hits)
        #         if len(responds):
        #             break
        query_string = query_string_org.replace(":", " ")

        # responds = self.run_service(query_string, max_hits)
        #
        # if "(" in query_string:
        #     new_query_string = re.sub(r"\((.*)\)", "", query_string).strip()
        #     if new_query_string != query_string:
        #         responds = self.run_service(new_query_string, max_hits)
        #
        # if "[" in query_string:
        #     new_query_string = re.sub(r"\([.*]\)", "", query_string).strip()
        #     if new_query_string != query_string:
        #         responds = self.run_service(new_query_string, max_hits)
        #
        # if not len(responds):
        #     new_query_string = re.search(r"\((.*)\)", query_string)
        #     if new_query_string:
        #         new_query_string = new_query_string.group(1)
        #         if new_query_string != query_string:
        #             responds = self.run_service(new_query_string, max_hits)
        #
        # if not len(responds):
        #     new_query_string = ul.normalize_text(query_string)
        #     if new_query_string != query_string:
        #         responds = self.run_service(new_query_string, max_hits)

        query_string = ul.normalize_text(query_string_org)
        if not len(responds):
            new_query_string = f.correct_spell().word_seg(query_string)
            if (
                new_query_string
                and len(new_query_string)
                and new_query_string != query_string_org
            ):
                responds = self.run_service(new_query_string, max_hits)

        if not len(responds):
            new_query_string = f.correct_spell().check(query_string, distance=1)
            if (
                new_query_string
                and len(new_query_string)
                and new_query_string != query_string_org
            ):
                responds = self.run_service(new_query_string, max_hits)

        #
        # if not len(responds) and "(" in query_string:
        #     new_query_string = re.sub(r"\((.*)\)", "", query_string).strip()
        #     if new_query_string != query_string:
        #         query_string = new_query_string
        #         responds = self.run_service(new_query_string, max_hits)
        #
        # if not len(responds) and "[" in query_string:
        #     new_query_string = re.sub(r"\[(.*)\]", "", query_string).strip()
        #     if new_query_string != query_string:
        #         query_string = new_query_string
        #         responds = self.run_service(new_query_string, max_hits)
        #
        # if not len(responds) and "." in query_string:
        #     query_string = re.sub(r"(.)\.", "", query_string).strip()
        #     responds = self.run_service(query_string, max_hits)

        # if not len(responds):
        #     fix_texts = {f.correct_spell().check(query_string_org, i) for i in range(0, 3)}
        #     for fix_text in fix_texts:
        #         if fix_text != query_string_org:
        #             responds = self.run_service(fix_text, max_hits)
        #             if len(responds):
        #                 # f.fix_labels()[query_string_org] = {fix_text}
        #                 break

        # iw.print_status("Query: %s | %s" % (query_string_org, str(f.fix_labels().get(query_string_org))), is_screen=False)
        iw.print_status("Query: %s" % query_string_org, is_screen=False)
        iw.print_status(
            "Results: %d\n    %s"
            % (
                len(responds),
                "\n    ".join(
                    [
                        "%d. %s - %s"
                        % (q_i, q_id, f.wikidata_info().get_labels(q_id, q_id))
                        for q_i, q_id in enumerate(responds[:3])
                    ]
                ),
            ),
            is_screen=False,
        )
        return responds


def pool_query_lk(lk_query):
    lk_responds = f.lk_wikidata().search(lk_query)
    return lk_query, lk_responds


def query_lk(n_cpu):
    f.lk_wikidata()
    # f.lk_wikidata().save_data()
    lk_queries = set()
    for lk_query in tqdm(iw.load_obj_pkl(st.DIR_LK_QUERIES_GT)):
        # if ":" in lk_query and not f.lk_wikidata().get(lk_query):

        if not f.lk_wikidata().get_data(lk_query):  # or f.fix_labels().get(lk_query)
            lk_queries.add(lk_query)

        # if len(lk_queries) > 100:
        #     break
        # else:
        #     check_labels = f.lk_wikidata().get(lk_query, min_sim=0.7)
        #     if not check_labels:
        #         lk_queries.add(lk_query)
        #     else:
        #         fix_texts = f.fix_labels().get(lk_query, set())
        #         core_text_fixs = {core_text_fix: sim.sim_string_fuzz(core_text_fix, lk_query)
        #                           for core_text_fix in fix_texts}
        #
        #         core_text_fixs1 = [text_fix for text_fix, text_score in core_text_fixs.items() if text_score > 0.7]
        #         if len(fix_texts) and not len(core_text_fixs1):
        #             lk_queries.add(lk_query)

    # lk_queries = [[lk_query,""] for lk_query in lk_queries]
    # iw.save_text_obj("%s/temp/Round%d/fix.txt" % (st.DIR_ROOT, st.ROUND), lk_queries, deliminator="|")
    # return

    count = 1
    with tqdm(total=len(lk_queries)) as p_bar:
        if n_cpu == 1:
            for lk_query in lk_queries:
                key, responds = pool_query_lk(lk_query)
                p_bar.update()
                f.lk_wikidata().update(key, responds)
                if len(responds):
                    count += 1
                    p_bar.set_description("Success: %d" % count)
                    if count % 2000 == 0:
                        f.lk_wikidata().save()
            f.lk_wikidata().save()
        else:
            with closing(Pool(processes=n_cpu)) as p:
                for key, responds in p.imap_unordered(pool_query_lk, lk_queries):
                    p_bar.update()
                    f.lk_wikidata().update(key, responds)
                    if len(responds):
                        count += 1
                        p_bar.set_description("Success: %d" % count)
                        if count % 2000 == 0:
                            f.lk_wikidata().save()
                f.lk_wikidata().save()


def update_labels():
    for query_text, wd_responds in tqdm(f.lk_wikidata().items()):
        new_labels = []
        is_update = False
        for wd, wd_label in wd_responds:
            update_item = [wd, wd_label]
            if not len(wd_label):
                wd_info = f.wikidata_info().get_entity(wd)
                if wd_info:
                    new_label = wd_info.get("label")
                    if new_label and len(new_label) and new_label != wd:
                        update_item = [wd, new_label]
                        is_update = True

            new_labels.append(update_item)
        if is_update:
            f.lk_wikidata().update(query_text, new_labels)
    f.lk_wikidata().save()


if __name__ == "__main__":
    f.init()
    # update_labels()
    # exit()
    # f.fix_labels()
    # f.lk_wikidata().search("Tb7.NT.76")
    # f.correct_spell()
    # tmp = f.lk_wikidata().search("1995 NHL Entry Eraft", max_hits=50)
    query_lk(n_cpu=5)
    # iw.save_obj_pkl("%s/temp/Round%d/lk_2cells_fix_2.pkl" % (st.DIR_ROOT, st.ROUND), f.fix_labels())

    # query = 'Çalıkuşu'
    # query = 'Paris-Charles de Gaulle Airport Terminal 1'
    # query = "H. Riipinen"
    # print("Query: %s" % query)
    # for r_i, (r, rlabel) in enumerate(f.lk_wikidata().get(query)):
    #     print("%3d. %s - %s" % (r_i + 1, r, rlabel))

    # query = 'berl'
    # print("Query: %s" % query)
    # for r_i, r in enumerate(f.dbpedia_lookup().get(query)):
    #     print("%3d. %s" % (r_i + 1, r))
    #     for t in r.types:
    #         print("     %s" % t)
