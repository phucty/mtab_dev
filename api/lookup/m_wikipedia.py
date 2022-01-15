import requests
from collections import defaultdict
import re
from api import m_f

import m_config as cf
import urllib
from api.utilities import m_io as iw
from api.utilities import m_utils as ul
from multiprocessing.pool import Pool
from tqdm import *
from contextlib import closing


class LookupWikipedia(object):
    def __init__(self):
        self.URL = "https://en.wikipedia.org/w/api.php?"
        self._data = None

    @staticmethod
    def to_en_url(url):
        url_lang = url.split("https://")[1]
        url_title = url_lang.split(".wikipedia.org/wiki/")[1]
        url_lang = url_lang.split(".wikipedia.org")[0]

        q_domain = (
            "https://%s.wikipedia.org/w/api.php?"
            "action=query"
            "&prop=langlinks"
            "&lllang=en"
            "&format=json"
            "&llprop=url"
            "&titles=%s" % (url_lang, url_title)
        )
        try:
            result = None
            responds = requests.get(q_domain).json()
            if responds.get("query") and responds["query"].get("pages"):
                for res_page in responds["query"]["pages"].values():
                    res_url = res_page["langlinks"][0]["url"]
                    res_title = res_page["langlinks"][0]["*"]
                    result = [res_url, res_title]
            return result
        except Exception as message:
            iw.print_status(message)
            return None

    @staticmethod
    def _open_search(query_string, max_hits=cf.LIMIT_SEARCH, lang="en"):
        query_domain = "https://%s.wikipedia.org/w/api.php?" % lang
        query_params = {
            "action": "opensearch",
            "search": query_string,
            "limit": max_hits,
            "redirects": "resolve",
            "format": "json",
        }
        responds = requests.get(query_domain, params=query_params).json()
        try:
            responds = [
                [responds[3][i], responds[1][i]] for i in range(len(responds[1]))
            ]
            return responds
        except Exception as message:
            iw.print_status(message)
            return []

    @staticmethod
    def _search(query_string, max_hits=cf.LIMIT_SEARCH, lang="en"):
        query_domain = (
            "https://%s.wikipedia.org/w/api.php?"
            "action=query"
            "&format=json"
            "&generator=search"
            "&prop=info"
            "&inprop=url"
            "&gsrlimit=%d"
            "&gsrsearch=%s" % (lang, max_hits, query_string)
        )
        results = []
        try:
            responds = requests.get(query_domain).json()
            if responds.get("query") and responds["query"].get("pages"):
                for respond in responds["query"]["pages"].values():
                    page_index = respond["index"]
                    page_id = respond["fullurl"]
                    page_label = respond["title"]
                    results.append([page_index, page_id, page_label])
        except Exception as message:
            iw.print_status(message)
            pass
        if len(results):
            results.sort(key=lambda x: x[0])
            results = [[r[1], r[2]] for r in results]
        return results

    def search_entity(self, query_string, max_hits=cf.LIMIT_SEARCH, lang=cf.LANG):
        if not query_string:
            return []

        res_en = self._open_search(query_string, max_hits, "en")

        # Open search on other languages
        res = []
        if lang and lang != "en":
            res_others = self._open_search(query_string, max_hits, lang)
            # Redirect to English
            for res_url, _ in res_others:
                en_redirect = self.to_en_url(res_url)
                if en_redirect:
                    res.append(en_redirect)
        res = res_en + [l for l in res if l not in res_en]

        # Search all text of wikipedia title
        if len(res) == 0:
            res_en = self._search(query_string, max_hits, "en")
            if lang and lang != "en":
                res_others = self._search(query_string, max_hits, lang)
                res = []
                for res_url, _ in res_others:
                    en_redirect = self.to_en_url(res_url)
                    if en_redirect:
                        res.append(en_redirect)
            res = res_en + [l for l in res if l not in res_en]

        responds = []
        for res_url, res_label in res:
            r_url = f.wiki_mapper().url_to_id(res_url)
            if not r_url:
                r_url = f.wiki_mapper().url_to_id(urllib.parse.unquote(res_url))
            if r_url:
                responds.append([r_url, res_label])

        iw.print_status("Query: %s" % query_string, is_screen=False)
        iw.print_status(
            "Results: %d\n    %s"
            % (
                len(responds),
                "\n    ".join(
                    [
                        "%d. %s - %s" % (q_i, q_id, q_label)
                        for q_i, (q_id, q_label) in enumerate(responds[:3])
                    ]
                ),
            ),
            is_screen=False,
        )
        return responds

    def search(self, org_string, max_hits=cf.LIMIT_SEARCH, lang="en"):
        res = []
        query_string = ul.normalize_text(org_string)
        # query_string = re.sub(r"\((.*)\)", "", org_string)
        # query_string = re.sub(r"\[(.*h1)]", "", query_string).strip()  # r"\[(.*h1)\]"
        # res = self.search_a_query(query_string, max_hits, lang)
        # if not len(res) and "(" in org_string:
        #     new_query_string = re.sub(r"\((.*)\)", "", org_string).strip()
        #     if new_query_string != org_string:
        #         res = self.search_a_query(new_query_string, max_hits, lang)
        #
        # if not len(res) and "[" in org_string:
        #     new_query_string = re.sub(r"\([.*]\)", "", org_string).strip()
        #     if new_query_string != org_string:
        #         res = self.search_a_query(new_query_string, max_hits, lang)
        #
        # if not len(res) and "(" in org_string:
        #     new_query_string = re.search(r"\((.*)\)", org_string)
        #     if new_query_string:
        #         new_query_string = new_query_string.group(1)
        #         if new_query_string != org_string:
        #             res = self.search_a_query(new_query_string, max_hits, lang)

        if not len(res):
            new_query_string = f.correct_spell().word_seg(query_string)
            if new_query_string != org_string:
                res = self.search_entity(new_query_string, max_hits, lang)

        if not len(res):
            new_query_string = f.correct_spell().check(query_string, distance=1)
            if new_query_string != org_string:
                res = self.search_entity(new_query_string, max_hits, lang)

        res = [wd for wd, _ in res]
        return res

    def items(self):
        for key, value in self._data.items():
            yield key, value

    def get(self, query_text):
        return self._data.get(query_text)

    def update(self, query_text, responds):
        self._data[query_text] = responds

    def save(self):
        iw.save_obj_pkl(self.dir_file, self._data)


def pool_query_lk(lk_query):
    lk_responds = f.lk_wikipedia().search(lk_query)
    return lk_query, lk_responds


def query_lk(n_cpu):
    f.lk_wikipedia()
    lk_queries = set()
    for lk_query in iw.load_obj_pkl(st.DIR_LK_QUERIES_GT):
        if not f.lk_wikipedia().get(lk_query):
            lk_queries.add(lk_query)

    lk_queries = list(lk_queries)
    count = 0
    with tqdm(total=len(lk_queries)) as p_bar:
        if n_cpu == 1:
            for lk_query in lk_queries:
                key, responds = pool_query_lk(lk_query)
                p_bar.update()
                f.lk_wikipedia().update(key, responds)
                if len(responds):
                    count += 1
                    p_bar.set_description("Success: %d" % count)
                    if count % 20000 == 0:
                        f.lk_wikipedia().save()
            f.lk_wikipedia().save()
        else:
            with closing(Pool(processes=n_cpu)) as p:
                for key, responds in p.imap_unordered(pool_query_lk, lk_queries):
                    p_bar.update()
                    f.lk_wikipedia().update(key, responds)
                    if len(responds):
                        count += 1
                        p_bar.set_description("Success: %d" % count)
                        if count % 20000 == 0:
                            f.lk_wikipedia().save()
                f.lk_wikipedia().save()


if __name__ == "__main__":
    m_f.init()
    m_f.m_corrector()
    searcher = LookupWikipedia()
    temp = searcher.search("DJ Sorryyouwastedyourmoneytobehere")
    temp = searcher.search("Kanawha Couqty")
    query_lk(n_cpu=5)

    # query = 'berlin'
    # query = "H. Riipinen"
    # query = "\"Costa Rica\" 1–0 1–0 Friendly"
    # query = "\"Argentina\" 2008 1 0"
    # query = "Venezuela 3–0 4–2 1989 Copa América"
    # query = "Venezuela"
    # query = "Ligaw na Bulaklak"
    # query = 'Paris-Charles de Gaulle Airport Terminal 1'
    # print("Query: %s" % query)
    # for r_i, (r_url, r_label) in enumerate(f.lk_wikipedia().get(query)):
    #     print("%3d. %s - %s" % (r_i+1, r_url, r_label))

    # query = 'berl'
    # print("Query: %s" % query)
    # for r_i, r in enumerate(f.wikipedia_lookup().get(query)):
    #     print("%3d. %s" % (r_i+1, r))
    #     for t in r.types:
    #         print("     %s" % t)
