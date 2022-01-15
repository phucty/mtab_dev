import re
from collections import defaultdict

import numpy

from api.utilities import m_utils as ul
from api import m_f
import m_config as cf
from api.utilities import m_io as iw
from time import time
from api.utilities import m_sim
from api.lookup.m_entity_bm25 import ESearch


def search(
    query, attr=None, lang="en", mode="a", limit=0, expensive=False, fuzzy=False
):
    is_wd_id = ul.is_wd_item(query)
    # check it is multilingual
    # query_lang = m_f.lang_pre().predict(query)
    # if query_lang != "en":
    #     lang = "all"
    if not ul.isEnglish(query):
        lang = "all"
    # iw.print_status(f"Lang: {lang}")

    # Attribute handling
    if attr:
        # Todo: Implement entity search
        debug = 1

    responds_label_e, responds_label_f, responds_label = (
        defaultdict(float),
        defaultdict(float),
        None,
    )

    def get_wd_score(_responds_label):
        _responds = defaultdict(float)
        for _r_i, (_respond, _res_s) in enumerate(_responds_label):
            if lang == "en":
                _respond_wds = m_f.m_item_labels().get_wd_qid_en(
                    _respond, page_rank=True
                )
                if not _respond_wds:
                    _respond_wds = m_f.m_item_labels().get_wd_qid_all(
                        _respond, page_rank=True
                    )
            else:
                _respond_wds = m_f.m_item_labels().get_wd_qid_all(
                    _respond, page_rank=True
                )
            if not _respond_wds:
                continue

            for _res_wd, _prank in _respond_wds:
                wd_type = m_f.m_items().get_instance_of(_res_wd)
                if wd_type and "Q4167410" in wd_type:
                    continue
                wd_wp = m_f.m_items().get_wikipedia_title(_res_wd)
                if wd_wp and "(disambiguation)" in wd_wp:
                    continue
                if lang == "en":
                    # _responds[_res_wd] = max(_responds[_res_wd], _res_s * 0.7 + _prank * 0.3)

                    main_label = m_f.m_items().get_label(_res_wd)
                    main_label_sim = 0
                    if main_label:
                        main_label_sim = m_sim.sim_fuzz_ratio(main_label, query)

                    label_en = m_f.m_items().get_labels(_res_wd)
                    label_en_sim = 0
                    if label_en:
                        label_en_closest = m_sim.get_closest(query, label_en)
                        if label_en_closest:
                            c_label, label_en_sim = label_en_closest

                    _responds[_res_wd] = max(
                        _responds[_res_wd],
                        _res_s * 0.4
                        + _prank * 0.3
                        + main_label_sim * 0.001
                        + label_en_sim * 0.3,
                    )
                else:
                    label_all = m_f.m_items().get_labels(_res_wd, multilingual=True)
                    label_all_sim = 0
                    if label_all:
                        label_all_closest = m_sim.get_closest(query, label_all)
                        if label_all_closest:
                            c_label, label_all_sim = label_all_closest

                    _responds[_res_wd] = max(
                        _responds[_res_wd],
                        _res_s * 0.4 + _prank * 0.3 + label_all_sim * 0.3,
                    )
                # if limit and len(_responds) > limit:
                #     break
            if limit and len(_responds) > limit:
                break
        return _responds

    if is_wd_id:
        responds = {query.upper(): 1}
    else:
        m_search_e = ESearch()
        # Normalize query
        if query and query[0] == '"':
            query = query.replace('"', "")

        replace_strs = [
            "_ap_aquaregia",
            "_gr_aquaregia mg/kg",
            "_gr_aquaregia",
            "_gr_gemas_aquaregia",
        ]
        for replace_str in replace_strs:
            start = query.lower().find(replace_str)
            if start >= 0:
                query = query[:start] + query[start + len(replace_str) :]
        if "_" in query:
            tmp = query.split("_")
            if tmp and len(tmp) > 1 and len(tmp[0]) < len(tmp[1]):
                query = "_".join(tmp[1:])

        query = re.sub(r"(?<=[.,])(?=[^\s])", r" ", query)

        if mode == "b":
            responds_label = m_search_e.search_wd(
                query, limit=limit, lang=lang, fuzzy=fuzzy
            )
        elif mode == "f":
            responds_label = m_f.m_search_f().search_wd(
                query, limit=limit, lang=lang, expensive=expensive
            )
        else:
            responds_label_e = m_search_e.search_wd(
                query, limit=limit, lang=lang, fuzzy=fuzzy
            )
            # max_score_e = 0
            # if responds_label_e:
            #     max_score_e = responds_label_e[0][1]
            # if max_score_e < 30:
            #     responds_label_f = m_f.m_search_f().search_wd(query, limit=limit, lang=lang, expensive=True)
            # else:
            #     responds_label_f = m_f.m_search_f().search_wd(query, limit=limit, lang=lang, expensive=False)
            responds_label_f = m_f.m_search_f().search_wd(
                query, limit=limit, lang=lang, expensive=expensive
            )
            responds_label = ul.merge_ranking(
                [responds_label_e, responds_label_f], weight=[0.9, 1], is_sorted=True
            )

        responds = defaultdict(float)

        if responds_label:
            responds = get_wd_score(responds_label)

        # for r_i, (respond, res_s) in enumerate(responds_label):
        #     respond_wds = entity_labels.get_words(respond, page_rank=True)
        #
        #     if not respond_wds:
        #         continue
        #     for res_wd, prank in respond_wds:
        #         responds[res_wd] = max(responds[res_wd], res_s * 0.7 + prank * 0.3)

    if not responds:
        return [], None, None
    if limit == 0:
        limit = len(responds)
    # responds = ul.cal_p_from_ranking(responds, is_sort=True)
    responds = sorted(responds.items(), key=lambda x: x[1], reverse=True)
    responds = responds[:limit]

    if responds_label_e:
        responds_label_e = get_wd_score(responds_label_e)
    else:
        responds_label_e = defaultdict(float)

    if responds_label_f:
        responds_label_f = get_wd_score(responds_label_f)
    else:
        responds_label_f = defaultdict(float)

    return responds, responds_label_e, responds_label_f


def test(lang="en", mode="a", limit=20):
    iw.print_status(f"\nlang={lang} - mode={mode}----------------------------")
    queries = [
        "semtab",
        "WâpYên",
        "日本情報学研究所",
        "Picubah Street",
        "Sarah Mclauphlan",
        "Matthew Macaunhay",
        "Matthew Macanhey",
        "Mark Mcgr",
        "New York",
        "Tokyo Olempic",
        "Hadeki Tjkeda",
        "M. Nykänen",
        "武田英明",
        "Град Скопјее",
        "Préfecture de Kanagawa",
        "Paulys Realenzyklopädie der klassischen Altertumswissenschaft",
        "La gran bretaña",
        "Straßenbahn Haltestelle Wendenschloß",
        "제주 유나이티드 FC",
        "অ্যাটলেটিকো ডি কলকাতা",
        "New York",
        "V* FH Mon",
        "Expedition 56",
        "music term",
        "chapel",
        "ensemble",
        "commercial art gallery",
        "rural municipality of Estonia",
        "Wikimedia topic category",
        "rest area",
        "borough council",
        "partly free country",
        "wildlife management area",
        "Phoenix",
        "Mitochondrial Uncoupling Proteins",
        "ministry of the State Council",
        "enzyme family",
        "sports festival",
        "scientific article",
        "dasdasj",
        "Sarah Mclaugling",
        "Univerity of Belgrade",
        "RE:Aulon 17",
        "Church of St Adrew",
        "PIR protein, pseudogene",
        "Catholic archbishop",
        "Floridaaa",
        "corona japan",
        "aideakiii akea",
        "Hideaki Takeda",
        "hideaki takeda",
        "hidEAki tAKeda",
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
        "Tb10.NT.103",
        "Apaizac beto expeditin",
        "{irconium-83",
        'assassination of"John F. Kennedy',
        "covid japan",
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
        start = time()
        responds, responds_bm25, responds_fuzzy = search(
            query, lang=lang, mode=mode, expensive=True
        )
        iw.print_status(
            f"About {len(responds)} results ({(time() - start):.5f} seconds)"
        )
        responds = m_f.m_items().get_search_info(responds[:10])
        if responds:
            c_ok += 1
        for i, respond in enumerate(responds):
            r_score = respond["score"]
            r_score_bm25 = 0
            if responds_bm25:
                r_score_bm25 = responds_bm25.get(respond["id"], 0)
            r_score_fuzzy = 0
            if responds_fuzzy:
                r_score_fuzzy = responds_fuzzy.get(respond["id"], 0)
            r_label = respond["label"]
            r_wd = respond["wd"]
            r_des = respond["des"]
            iw.print_status(
                f"{i + 1:2d}. "
                f"{r_score * 100:5.2f}|{r_score_bm25 * 100:5.2f}|{r_score_fuzzy * 100:5.2f}| "
                f"{r_wd} | [{r_label}] - {r_des}"
            )
    iw.print_status(f"{c_ok}/{len(queries)} - Run time {time() - start_1:.10f} seconds")


if __name__ == "__main__":
    m_f.init()

    # test(lang="en", mode="b")
    # test(lang="en", mode="f")
    test(lang="en", mode="f")
    #
    # test(lang="all", mode="a")
    # test(lang="all", mode="b")
    # test(lang="all", mode="f")
