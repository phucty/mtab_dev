import re
from collections import defaultdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from tqdm import tqdm


from api.utilities import m_io as iw
from api.resources.m_item import MItem
import m_config as cf
from api.resources.m_db_item import DBItem
from api.resources.m_item import MItem
from api.utilities import m_io as iw
from api.utilities import m_utils as ul
from time import time
from api import m_f
import http.client

http.client._MAXLINE = 655360


class ESearch(object):
    def __init__(self):
        self.client = Elasticsearch(
            [{"host": "localhost", "port": 9200, "timeout": 300}]
        )

        self.index_en = cf.ES_INDEX_NAME_EN
        self.index_all = cf.ES_INDEX_NAME_ALL

        # self._db_english = None
        # self._db_multilingual = None
        self._db_labels = None
        # self._wiki_items = None

    @property
    def db_labels(self):
        if not self._db_labels:
            self._db_labels = m_f.m_item_labels()
        return self._db_labels

    # @property
    # def db_english(self):
    #     if not self._db_english:
    #         self._db_english = m_f.m_item_labels()
    #     return self._db_english
    #
    # @property
    # def db_multilingual(self):
    #     if not self._db_multilingual:
    #         self._db_multilingual = m_f.m_entity_db_multilingual()
    #     return self._db_multilingual

    # @property
    # def wiki_items(self):
    #     if not self._wiki_items:
    #         self._wiki_items = m_f.m_items()
    #     return self._wiki_items

    def build(self):
        status = self.ping()
        if not status:
            return

        self.get_indices()
        self.build_index(self.index_en)
        self.build_index(self.index_all)

    def ping(self):
        respond = self.client.ping()
        if respond:
            print("Elastic Search is running at localhost:9200")
            return True
        else:
            print("Could not connect Elastic Search")
            return False

    def get_indices(self):
        if not self.client.indices.exists(self.index_en):
            self.client.indices.create(index=self.index_en, body=cf.ES_MAPPING)
            iw.print_status(f"Create {self.index_en}")
        if not self.client.indices.exists(self.index_all):
            self.client.indices.create(index=self.index_all, body=cf.ES_MAPPING)
            iw.print_status(f"Create {self.index_all}")

    # @staticmethod
    # def gen_index_document(obj, index_name):
    #     obj.update({"_op_type": "index", "_index": index_name})
    #     return obj

    @staticmethod
    def gen_index_docs(iter_items, index_name):
        for label, label_id in iter_items:
            yield {"_op_type": "index", "_index": index_name, "label": label}

    def build_index(self, index_name, buff_size=100000, from_i=None):
        if from_i is None:
            from_i = 0
        if index_name == cf.ES_INDEX_NAME_EN:
            iter_items = self.db_labels.iter_labels_en(from_i=from_i)
            total = self.db_labels.size_labels_en()
        else:
            iter_items = self.db_labels.iter_labels_all(from_i=from_i)
            total = self.db_labels.size_labels_all()
        obj_gen = self.gen_index_docs(iter_items, index_name)
        if from_i:
            total -= from_i

        for status, response in tqdm(
            streaming_bulk(self.client, chunk_size=buff_size, actions=obj_gen),
            total=total,
            desc=f"{index_name}",
        ):
            if not status:
                iw.print_status(response)

    def search_label(self, input_text, lang="en", fuzzy=False):
        input_text = ul.norm_queries(input_text, punctuations=False)

        def combine_result(is_fuzzy=False):
            res = defaultdict(float)
            index_name = self.index_en if lang == "en" else self.index_all
            try:
                if is_fuzzy:
                    q_text = {
                        "size": cf.LIMIT_SEARCH_ES,
                        "query": {"fuzzy": {"label": input_text}},
                    }
                else:
                    q_text = {
                        "size": cf.LIMIT_SEARCH_ES,
                        "query": {"match": {"label": input_text}},
                    }
                response = self.client.search(index=index_name, body=q_text)
            except Exception as message:
                iw.print_status(message)
                response = {}
            # _max_score = 0
            if response.get("hits", []):
                # if response["hits"].get("max_score"):
                #     _max_score = response["hits"]["max_score"]
                for hit in response["hits"].get("hits", []):
                    if not hit.get("_source"):
                        continue
                    res[hit["_source"]["label"]] = max(
                        res[hit["_source"]["label"]], hit["_score"]
                    )
            if res:
                min_scores = min(res.values())
                div_scores = max(res.values()) - min_scores
                if div_scores:
                    res = {k: ((v - min_scores) / div_scores) for k, v in res.items()}
                else:
                    res = {k: (v / min_scores) for k, v in res.items()}
            return res

        responds = combine_result(is_fuzzy=False)

        # responds = defaultdict(float)
        # for res_i, res_s in res_bm25.items():
        #     responds[res_i] += res_s
        if fuzzy:
            res_fuzzy = None
            try:
                res_fuzzy = combine_result(is_fuzzy=True)
            except Exception as message:
                iw.print_status(message, is_screen=False)
            if res_fuzzy:
                for res_i, res_s in res_fuzzy.items():
                    if responds.get(res_i):
                        responds[res_i] = max(res_s, responds[res_i])
                    else:
                        responds[res_i] = res_s
                    # responds = {k: v / 2. for k, v in responds.items()}
        if responds:
            max_score = max(responds.values())
        else:
            max_score = 0
        # if max_score:
        #     res = {k: (v / max_score) for k, v in res.items()}
        return responds, max_score

    def search_wd(self, input_text, limit=0, lang="en", fuzzy=False):
        if not input_text:
            return defaultdict(float)
        responds_label = defaultdict(float)
        max_score = 0
        is_wd_id = ul.is_wd_item(input_text)
        if is_wd_id:
            responds_label = {input_text.upper(): 1}

        query_text = input_text

        if not responds_label:
            responds_label, max_score = self.search_label(
                query_text, lang=lang, fuzzy=fuzzy
            )

        if not responds_label:
            if "(" in query_text:
                new_query_string = re.sub(r"\((.*)\)", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label, max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )

            if "[" in query_text:
                new_query_string = re.sub(r"\([.*]\)", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label, max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )

        if '("' in input_text:
            new_query_string = re.search(r"\(\"(.*)\"\)", input_text)
            if new_query_string:
                new_query_string = new_query_string.group(1)
                if new_query_string != input_text:
                    extra, extra_max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )
                    if extra:
                        for e_i, e_s in extra.items():
                            if not responds_label.get(e_i):
                                responds_label[e_i] = e_s

        if "[" in input_text:
            new_query_string = re.sub(r"\[(.*)\]", "", input_text)
            if new_query_string != input_text:
                extra, extra_max_score = self.search_label(
                    new_query_string, lang=lang, fuzzy=fuzzy
                )
                if extra:
                    responds_label = {k: v * 0.99 for k, v in responds_label.items()}
                    # responds_label_set = set(responds_label.keys())
                    for e_i, e_s in extra.items():
                        if responds_label.get(e_i):
                            responds_label[e_i] = max(e_s, responds_label[e_i])
                        else:
                            responds_label[e_i] = e_s
                        # if e_i not in responds_label_set:
                        #     responds_label[e_i] = e_s
                        #     responds_label_set.add(e_i)

        responds = responds_label
        # responds = defaultdict(float)
        # for r, r_s in responds_label.items():
        #     db_words = self.db_english if lang == "en" else self.db_multilingual
        #     respond_wds = db_words.get_words(r, page_rank=True)
        #     if not respond_wds:
        #         continue
        #     for res_wd, prank in respond_wds:
        #         if responds.get(res_wd):
        #             continue
        #         # prank = self.wiki_items.get_pagerank_score(res_wd)
        #         responds[res_wd] = r_s * cf.WEIGHT_ES + prank * cf.WEIGHT_PAGERANK

        if not responds:
            return []

        responds = sorted(responds.items(), key=lambda x: x[1], reverse=True)
        if not limit:
            limit = len(responds)

        if limit:
            responds = responds[:limit]
        return responds


def test():
    from api.lookup import m_entity_search

    queries = [
        "Communism[citation needed]]",
        "Floridaaa",
        "Hideaki Takeda",
        "hideaki takeda",
        "hidEAki tAKeda",
        "Град Скопјее",
        "Oregon, OR",
        "Sarrah Mcgloclyn",
        "* TM-88",
        "Zachary Knight Galifianacisss",
        "Hedeki Tjkeda",
        "H. Tjkeda",
        "Colin Rand Kapernikuss",
        "Chuck Palahichikkk",
        "American rapper",
        "Tokio",
        "Phucc Nguyan",
        "Tatyana Kolotilshchikova",
        "T Kolotilshchikova",
        "T. Kolotilshchikova",
        "R. Millar",
        "Tokyo",
        "tokyo",
        "M. Nykänen",
        "Tb10.NT.103",
        "Apaizac beto expeditin",
        "{irconium-83",
        'assassination of"John F. Kennedy',
        "corona japan",
        "SDS J100759.52+102152.8",
        "2MASS J10540655-0031018",
        "6C 124133+40580",
        "2MASS J0343118+6137172",
        "[RMH2016] 011.243560+41.94550",
        "ruthenium-:8",
        "bismuth-393",
        "rubidium-7",
        "Rubidium-7",
        "rUBIdium-7",
        "rubIDIum-7",
        "rUbidIUm-2",
        "neod{mium-133",
        "neodymiwm-155",
        "SDSS J001733.60+0040306",
        "Abstraction Latin Patricia Phelps de Cisneros Collection",
        "Geometric Abstraction: Latin American Art from the Patricia Phelps de Cisneros Collection",
    ]
    start_1 = time()
    c_ok = 0
    for query in queries:
        iw.print_status("\nQuery: %s" % query)

        def print_result(lang="en", c_ok=0):
            start = time()
            responds, _, _ = m_entity_search.search(
                query, mode="b", lang=lang, limit=cf.LIMIT_SEARCH, fuzzy=True
            )
            iw.print_status(f"Lang: {lang}")
            iw.print_status(
                f"About {len(responds)} results ({(time() - start):.5f} seconds)"
            )
            responds = m_f.m_items().get_search_info(responds[:10])
            if responds:
                c_ok += 1
            for i, respond in enumerate(responds):
                r_score = respond["score"]
                r_label = respond["label"]
                r_wd = respond["wd"]
                r_des = respond["des"]
                iw.print_status(
                    f"{i + 1}. " f"{r_score:.5f} - {r_wd}[{r_label}] - {r_des}"
                )
            return c_ok

        # c_ok += print_result(lang="en")
        c_ok += print_result(lang="all")

    iw.print_status(f"{c_ok}/{len(queries)} - Run time {time() - start_1:.10f} seconds")


if __name__ == "__main__":
    m_f.init()
    # fzs = ESearch()
    # fzs.build()
    test()
    print("Done")
