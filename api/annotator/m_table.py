import os
from api import m_f
from api.utilities import m_io as iw
from api.utilities import m_sim as sim
from api.utilities import m_utils as ul
import m_config as cf
from collections import defaultdict
from time import time
from api.lookup import m_entity_search


class InputTable(object):
    def __init__(self, dir_data, table_id, tar_cea, tar_cta, tar_cpa):
        self.dir_data = dir_data
        self._tar_cea = tar_cea
        self._tar_cta = tar_cta
        self._tar_cpa = tar_cpa

        self.table_id = table_id
        self.n_col = 0
        self.n_row = 0
        self.n_cel = 0
        self.n_mis = 0

        if tar_cea:
            self.headers = tar_cea.headers()
        else:
            self.headers = [0]
        if tar_cpa:
            self.core_attribute = tar_cpa.core_attribute()
        elif tar_cta:
            self.core_attribute = tar_cta.core_attribute()

        if self.core_attribute is None:
            self.core_attribute = 0

        self.cells = []
        self._preprocess()
        # self.langs = []
        # self.data_types = []
        # self.entity_types = []

    def _preprocess(self):
        # Load csv data
        self.cells = [line for line in iw.load_object_csv(self.dir_data)]

        # row
        self.n_row = len(self.cells)
        # remove empty line at the end of data file
        while not self.cells[self.n_row - 1] and self.n_row > 0:
            self.n_row -= 1

        # parse other information
        # self.langs = [[None for _ in r] for r in self.cells]
        # self.data_types = [[defaultdict(int) for _ in r] for r in self.cells]
        # self.entity_types = [[defaultdict(int) for _ in r] for r in self.cells]

        # col
        for row_id, r in enumerate(self.cells):
            if len(r) > self.n_col:
                self.n_col = len(r)
            for col_id, c_value_org in enumerate(r):
                if len(c_value_org) > 0:
                    self.n_cel += 1

        self.n_mis = self.n_row * self.n_col - self.n_cel

    def _lk_cell(self, limit=20):
        entity_cans = [[[] for _ in range(self.n_col)] for _ in range(self.n_row)]
        for r_i, r_obj in enumerate(self.cells):
            for c_i, c_obj in enumerate(r_obj):
                if r_i in self.headers or len(r_obj) < (self.n_col / 2):
                    continue
                if self._tar_cea.is_tar(r_i, c_i):
                    responds, responds_bm25, responds_fuzz = m_entity_search.search(
                        c_obj, limit=limit, mode="a"
                    )
                    # responds_bm25 = m_f.m_search_e().search_wd(c_obj, limit=limit)
                    # responds_fuzz = m_f.m_search_f().search_wd(c_obj, limit=limit)
                    # responds = ul.merge_ranking([responds_fuzz, responds_bm25], weight=[1, 0.9], is_sorted=True)
                    # responds = responds_bm25
                    if responds:
                        entity_cans[r_i][c_i] = responds

        return entity_cans

    def _lk_stat(self, lk_e_responds):
        entity_cans = [[[] for _ in range(self.n_col)] for _ in range(self.n_row)]
        for r_i, r_obj in enumerate(lk_e_responds):
            core_obj = r_obj[self.core_attribute]

            for c_i, c_obj in enumerate(r_obj):
                if not c_obj or c_i == self.core_attribute:
                    continue
                # responds = m_f.m_search_s.search_wd(core_obj, c_obj)
                responds = None
                entity_cans[r_i][c_i] = responds
        return entity_cans

    def annotate(self, limit=10):
        res_cea, res_cpa, res_cta = [], defaultdict(), []
        log_m = f"Table {self.table_id}: Size: {self.n_row}x{self.n_col} - {self.n_cel} cells"
        log_m += iw.print_table(self.cells, self.n_col)
        start = time()

        # Aggregation of CEA
        m_p = [
            [
                {
                    "lk_e": defaultdict(float),
                    "lk_s": defaultdict(float),
                    "lk_s_type": defaultdict(float),
                    "prop": defaultdict(float),
                    "value": defaultdict(float),
                    "final": defaultdict(float),
                    "type_all": defaultdict(float),
                    "type_direct": defaultdict(float),
                    "surface": defaultdict(float),
                }
                for _ in range(self.n_col)
            ]
            if r_i not in self.headers
            else []
            for r_i in range(self.n_row)
        ]

        # entity lookup
        cans_e = self._lk_cell(limit=limit)

        # statement lookup
        # two cell search and cell 1 search given cell 2
        cans_s = self._lk_stat(cans_e)

        for r_i, r_obj in enumerate(cans_e):
            if r_i in self.headers:
                continue
            for c_i, c_obj in enumerate(r_obj):
                # Aggregate one cell lookup
                m_p[r_i][c_i]["lk_e"] = {k: v for k, v in c_obj}

                # Aggregate two cell lookup
                # Cell 1 in core attribute: aggregation of all cell 2
                # Cell 2 in other columns:
                if cans_s[r_i][c_i]:
                    m_p[r_i][c_i]["lk_s"] = {k: v for k, v in cans_s[r_i][c_i]}

        value_mat_errors = set()

        p_cans = [defaultdict(float) for _ in range(self.n_col)]
        c2_ps = defaultdict(set)
        c2_ps_inv = defaultdict(set)
        res_cea_dict = defaultdict()

        is_step_2 = True
        # if table_obj["n_col"] < 2:
        # is_step_2 = False
        log_m_cpa = ""
        # Iterate 2 steps
        for step_i in range(2):
            if step_i == 1 and not is_step_2:
                continue
            # if step_i == 1:
            #     log_m += "\nStep 2"
            # Value matching and CPA
            for r_i, r_obj in enumerate(m_p):
                if r_i in self.headers or (step_i == 1 and r_i not in value_mat_errors):
                    continue
                # get cell text and cea gt
                row_values = [
                    [c_obj, 1 if self._tar_cea.is_tar(r_i, c_i) else 0]
                    for c_i, c_obj in enumerate(self.cells[r_i])
                    if c_i != self.core_attribute
                ]
                if step_i == 0:
                    e_candidates = sorted(
                        {
                            **r_obj[self.core_attribute]["lk_s"],
                            **r_obj[self.core_attribute]["lk_e"],
                        }.items(),
                        key=lambda x: x[1],
                        reverse=True,
                    )
                else:
                    old_cans = set(
                        {
                            **r_obj[self.core_attribute]["lk_s"],
                            **r_obj[self.core_attribute]["lk_e"],
                        }.keys()
                    )
                    e_candidates = r_obj[self.core_attribute]["lk_s_type"]
                    e_candidates = {
                        wd: wd_s
                        for wd, wd_s in e_candidates.items()
                        if wd not in old_cans
                    }
                    if len(m_p[r_i][self.core_attribute]["value"]):
                        e_candidates.update(m_p[r_i][self.core_attribute]["value"])
                    e_candidates = sorted(
                        e_candidates.items(), key=lambda x: x[1], reverse=True
                    )

                m_match_v_res = []
                is_ok = False
                for wd_i, (wd_id, _) in enumerate(e_candidates):
                    avg_sim, best_match, best_str = ul.get_value_matching(
                        row_values, wd_id
                    )
                    if avg_sim > 0:
                        m_match_v_res.append([wd_id, avg_sim, best_match, best_str])
                    if avg_sim == 1:
                        is_ok = True
                    if wd_i > 10 and is_ok:
                        break

                if not len(m_match_v_res):
                    value_mat_errors.add(r_i)
                else:
                    is_lk_type = False
                    m_match_v_res = sorted(
                        m_match_v_res, key=lambda x: x[1], reverse=True
                    )
                    m_match_v_res = [
                        item
                        for item in m_match_v_res
                        if item[1] > m_match_v_res[0][1] * 0.9
                    ]

                    if m_match_v_res[0][1] < 0.85:
                        value_mat_errors.add(r_i)
                        is_lk_type = True

                    is_error = False
                    for wd_rank, (wd_id, avg_sim, best_match, best_str) in enumerate(
                        m_match_v_res
                    ):
                        wd_labels = m_f.m_items().get_labels(wd_id)
                        stat_sim = 0
                        if wd_labels:
                            stat_sim = max(
                                [
                                    sim.sim_string_fuzz(
                                        self.cells[r_i][self.core_attribute],
                                        _v_text,
                                        is_lower=False,
                                    )
                                    for _v_text in wd_labels
                                ]
                            )
                            if stat_sim < 0.65 and avg_sim < 0.65:
                                continue

                        m_p[r_i][self.core_attribute]["value"][wd_id] = avg_sim
                        # log_m += "\n"
                        # log_m += f"{stat_sim:.2f}:{wd_id}[{str(m_f.m_items().get_label(wd_id))}] - {avg_sim:.2f}:"
                        # log_m += "[%s] | " % ", ".join(["%.2f" % (s_states[0][1])
                        #                                 if len(s_states) else "   0" for s_states in best_match])
                        # log_m += ", ".join([f"{str(e_value)}" for e_value in best_str])
                        if best_match:
                            if wd_rank < 3 and step_i == 1 and is_lk_type:
                                # sum([1 for tmp_col_scores in best_match if not len(tmp_col_scores)]) > 0:
                                # log_mis_score = [max([s_state for s_state, _ in s_states]) if len(s_states) else 0
                                #                  for s_states in best_match]

                                is_error = True

                            for c_prop_i in range(len(row_values)):
                                for stat_obj, col_score in best_match[c_prop_i]:
                                    if isinstance(stat_obj, str):
                                        e_prop, e_v_id = stat_obj, None
                                    else:
                                        e_prop, e_v_id = stat_obj
                                    if not col_score:
                                        continue
                                    if (
                                        wd_rank == 0
                                        and ul.is_wd_item(e_prop)
                                        and avg_sim > 0.8
                                    ):
                                        p_cans[c_prop_i + 1][e_prop] += (
                                            col_score * avg_sim
                                        )

                                    if (
                                        e_v_id
                                        and ul.is_wd_item(e_v_id)
                                        and col_score > 0.65
                                    ):
                                        c2_ps[(r_i, c_prop_i + 1, e_v_id)].add(
                                            (c_prop_i + 1, e_prop)
                                        )
                                        c2_ps_inv[(c_prop_i + 1, e_prop)].add(
                                            (r_i, c_prop_i + 1, e_v_id)
                                        )
                                        m_p[r_i][c_prop_i + 1]["value"][e_v_id] = max(
                                            col_score * avg_sim,
                                            m_p[r_i][c_prop_i + 1]["value"][e_v_id],
                                        )

                    # if is_error and step_i == 1 and is_lk_type:  # \
                    # sum([1 for tmp_col_scores in best_match if not len(tmp_col_scores)]) > 0:
                    # log_m += "\nMiss Value: %d. %s\n" % (r_i, str(self.cells[r_i]))
                    # else:
                    #     log_m += "\nOK Value: %d. %s\n" % (r_i, str(self.cells[r_i]))

            # prop two cells
            # c_prop_2cell = [defaultdict(float) for _ in range(table_obj["n_col"])]
            # for r_i, r_obj in enumerate(m_p):
            #     if r_i in self.headers:
            #         continue
            #     for c_i, c_obj in enumerate(r_obj):
            #         for prop, prop_score in c_obj["prop"].items():
            #             c_prop_2cell[c_i][prop] += prop_score

            log_m_cpa = "\nCPA:"
            props_list = set()
            cell2_in_props_scores = defaultdict(float)
            for i, c_prop_res in enumerate(p_cans):
                if (
                    i != self.core_attribute
                ):  # self._tar_cpa and self._tar_cpa.is_tar(self.core_attribute, i):
                    # c_prop_res = ul.cal_p_from_ranking(c_prop_res)
                    # if not len(c_prop_res):
                    # for prop_id, prop_score in ul.cal_p_from_ranking(c_prop_2cell[i]).items():
                    #     if c_prop_res.get(prop_id):
                    #         c_prop_res[prop_id] += prop_score
                    #     else:
                    #         c_prop_res[prop_id] = prop_score
                    c_prop_res = ul.cal_p_from_ranking(c_prop_res, is_sort=True)
                    # else:
                    #     c_prop_res = ul.cal_p_from_ranking(c_prop_res, is_sort=True)

                    # if not len(c_prop_res):
                    #     res_cea_prop_cans = defaultdict(float)
                    #     for (res_cea_r, res_cea_c), res_cea_wd in res_cea_dict.items():
                    #         if res_cea_c == i:
                    #             core_e_wd = res_cea_dict.get((res_cea_r, 0))
                    #             core_e_info = m_f.m_wiki_items().get_statements(core_e_wd)
                    #             if core_e_info and len(core_e_info):
                    #                 for prop_id, wd_id, wd_label in core_e_info:
                    #                     if wd_id == res_cea_wd:
                    #                         res_cea_prop_cans[prop_id] += 1
                    #     c_prop_res = ul.get_e_probs(list(res_cea_prop_cans.items()), mode_score="score", is_sort=True)

                    if len(c_prop_res):
                        for prop_id, prop_score in c_prop_res:
                            cell2_in_props_scores[(i, prop_id)] = prop_score

                    if not len(c_prop_res):
                        log_m_cpa += "\n[0][%d] - ErrorCPA: Missing" % i
                        continue

                    log_m_cpa += "\n[0][%d]" % i
                    for prop_i, (prop_id, prop_score) in enumerate(c_prop_res[:5]):
                        wd_label = str(m_f.m_items().get_label(prop_id))
                        log_m_cpa += "\n    %d\t%.5f\t%s | %s" % (
                            prop_i + 1,
                            prop_score,
                            prop_id,
                            wd_label,
                        )

                    # Get top 1
                    c_prop_res = [
                        c_prop for c_prop in c_prop_res if c_prop[0] not in props_list
                    ]
                    same_score = [
                        c_prop[0]
                        for c_prop in c_prop_res
                        if c_prop[1] == c_prop_res[0][1]
                    ]
                    res_cpa[(0, i)] = same_score
                    # final_p = None

                    # if len(same_score) == 1:
                    #     final_p = same_score[0]
                    # elif len(same_score) > 1:
                    #     set_same_score = set(same_score) - props_list
                    #     # if len(set_same_score) > 1:
                    #     #     log_m += "\nErrorCPA: Duplicate %s" % set(set_same_score)
                    #     # continue
                    #     if len(set_same_score) > 1:
                    #         if "P31" in set_same_score or "P279" in set_same_score or "P361" in set_same_score:
                    #             if "P31" in same_score:
                    #                 final_p = "P31"
                    #             elif "P279" in same_score:
                    #                 final_p = "P279"
                    #             else:
                    #                 final_p = "P361"
                    #         else:
                    #             log_m_cpa += "\n[0][%d] - ErrorCPA: Duplicate" % i
                    #             set_same_score = {tmp_id: m_f.m_items().get_popularity(tmp_id) for tmp_id in
                    #                               set_same_score}
                    #             set_same_score = sorted(set_same_score.items(), key=lambda x: x[1], reverse=True)
                    #             final_p = set_same_score[0][0]
                    #
                    #             # for s_score in same_score:
                    #             #     if s_score not in props_list:
                    #             #         final_p = s_score
                    #             #         break
                    #     elif len(set_same_score) == 1:
                    #         final_p = list(set_same_score)[0]
                    #
                    # if final_p:
                    #     # cell2_in_props_scores[(i, final_p)] = 1.
                    #     # log_m += "\nSelect %s" % final_p
                    #     res_cpa[(0, i)] = final_p
                    #     props_list.add(final_p)

            # CEA
            for r_i, r_obj in enumerate(m_p):
                if r_i in self.headers:
                    continue
                for c_i, c_obj in enumerate(r_obj):
                    lk_scores = m_p[r_i][c_i]["lk_e"].copy()
                    if not len(lk_scores):
                        lk_scores = defaultdict(float)
                    # lk_scores = defaultdict(float)

                    # if table_obj["n_col"] < 2:
                    value_score = ul.cal_p_from_ranking(m_p[r_i][c_i]["type_direct"])
                    for wd_id, wd_score in value_score.items():
                        if lk_scores.get(wd_id):
                            lk_scores[wd_id] += wd_score
                        else:
                            lk_scores[wd_id] = wd_score

                    # value_score = ul.cal_p_from_ranking(m_p[r_i][c_i]["value"])
                    for wd_id, wd_score in m_p[r_i][c_i]["value"].items():
                        if lk_scores.get(wd_id):
                            lk_scores[wd_id] += wd_score
                        else:
                            lk_scores[wd_id] = wd_score

                    lk_2cell_score = ul.cal_p_from_ranking(m_p[r_i][c_i]["lk_s"])
                    for wd_id, wd_score in lk_2cell_score.items():
                        if lk_scores.get(wd_id):
                            lk_scores[wd_id] += wd_score
                        else:
                            lk_scores[wd_id] = wd_score

                    # Vote property
                    for wd_id in lk_scores.keys():
                        wd_prop_list = c2_ps.get((r_i, c_i, wd_id))
                        if wd_prop_list:
                            wd_score_max = 0
                            for wd_prop in wd_prop_list:
                                wd_score_max = max(
                                    wd_score_max, cell2_in_props_scores.get(wd_prop, 0)
                                )
                            if wd_score_max:
                                lk_scores[wd_id] += wd_score_max

                    surface_score = defaultdict(float)
                    for wd_id in lk_scores.keys():
                        wd_labels = m_f.m_items().get_labels(wd_id)
                        if wd_labels:
                            avg_sim = max(
                                [
                                    sim.sim_string_fuzz(
                                        self.cells[r_i][c_i], _v_text, is_lower=False
                                    )
                                    for _v_text in wd_labels
                                ]
                            )
                            if avg_sim > 0.5:
                                surface_score[wd_id] = avg_sim
                    for wd_id, wd_score in surface_score.items():
                        lk_scores[wd_id] += wd_score

                    m_p[r_i][c_i]["final"] = ul.cal_p_from_ranking(
                        lk_scores, is_sort=True
                    )

            # Combine score vote for CTA
            c_types_direct = [defaultdict(float) for _ in range(self.n_col)]
            c_types_direct_p = [defaultdict(float) for _ in range(self.n_col)]
            c_types_direct_wd = defaultdict(set)
            count_perfect = 0
            alpha = 0.0
            while alpha >= 0:
                for r_i, r_obj in enumerate(m_p):
                    if r_i in self.headers:
                        continue
                    for c_i, c_obj in enumerate(r_obj):
                        if not len(c_obj["final"]):
                            continue

                        for wd_id, wd_p in c_obj["final"][:3]:
                            wd_types = c_types_direct_wd.get(wd_id)
                            if wd_types is None:
                                wd_types = m_f.m_items().get_types_specific_dbpedia(wd_id)
                                c_types_direct_wd[wd_id] = wd_types
                            if not wd_types:
                                continue
                            for e_type in wd_types:
                                c_types_direct_p[c_i][e_type] += wd_p

                        wd_id = c_obj["final"][0][0]
                        wd_p = c_obj["final"][0][1]
                        if wd_p >= alpha:
                            count_perfect += 1
                            tmp_types = m_f.m_items().get_types_specific_dbpedia(wd_id)
                            if not tmp_types:
                                continue
                            for e_type in tmp_types:
                                c_types_direct[c_i][e_type] += 1.0

                            # for e_type in f.dump_wikidata().get_type_all(wd_id):
                            #     c_types[e_type] += 1.

                            # if e_types_direct:
                            #     count_perfect += 1
                            #     for e_type in e_types_direct:
                            #         # Direct type
                            #         c_types_direct[e_type] += 1.
                            #
                            #         # All types
                            #         c_types[e_type] += 1.
                            #
                            #         # parents_c_types = f.dump_wikidata().get_parents(e_type)
                            #         # c_types_parents[e_type] = parents_c_types
                            #         # for parent_c_type, _ in parents_c_types.items():
                            #         #     c_types[parent_c_type] += 1.
                            #         for parent_c_type in f.dump_wikidata().get_type_all(e_type):
                            #             c_types[parent_c_type] += 1.
                alpha = alpha - 0.33

            c_types_direct_p = [
                sorted(i.items(), key=lambda x: x[1], reverse=True)[:10]
                for i in c_types_direct_p
            ]
            c_types_direct_p = [ul.cal_p_from_ranking(i) for i in c_types_direct_p]
            for r_i, r_obj in enumerate(m_p):
                if r_i in self.headers:
                    continue
                for c_i, c_obj in enumerate(r_obj):
                    for wd_id in {
                        **m_p[r_i][c_i]["lk_e"],
                        **m_p[r_i][c_i]["lk_s"],
                    }.keys():
                        max_p = 0
                        tmp_c_types = c_types_direct_wd.get(wd_id)
                        if not tmp_c_types:
                            continue
                        for wd_type in c_types_direct_wd.get(wd_id):
                            if isinstance(c_types_direct_p[c_i], dict):
                                tmp_p = c_types_direct_p[c_i].get(wd_type)
                                if tmp_p and tmp_p > max_p:
                                    max_p = tmp_p
                        if max_p:
                            m_p[r_i][c_i]["type_direct"][wd_id] = max_p

            if step_i == 0:
                c1_c2_type_cans = [
                    max(c_cans.values()) if len(c_cans) else 0
                    for c_cans in c_types_direct
                ]
                c1_c2_type_cans = [
                    [
                        wd_id
                        for wd_id, wd_s in c_cans.items()
                        if wd_s >= c1_c2_type_cans[c_i] * 0.98
                    ]
                    for c_i, c_cans in enumerate(c_types_direct)
                ]
                for r_i, r_obj in enumerate(m_p):
                    if r_i in self.headers or r_i not in value_mat_errors:
                        continue
                    c_value = self.cells[r_i][self.core_attribute]
                    c1_cans_types = set()
                    for c2_wd in c1_c2_type_cans[self.core_attribute]:
                        # Todo implement 2cell type lookup
                        # responds_c1 = f.lk_fz_res_2cell_types().get((c_value, c2_wd))
                        responds_c1 = None
                        if responds_c1 and len(responds_c1):
                            responds_c1 = set(responds_c1[:100])
                        else:
                            responds_c1 = set()
                        c1_cans_types.update(responds_c1)

                    # c1_cans_types = ul.get_c1_cans(c1_c2_type_cans[c_i], c_value)
                    if len(c1_cans_types):
                        is_step_2 = True
                        m_p[r_i][self.core_attribute]["lk_s_type"] = ul.get_ranking_pr(
                            list(c1_cans_types),
                            mode_pr=cf.EnumPr.AVG,
                            mode_score=cf.EnumRank.EQUAL,
                        )

        log_m += "\n\nCEA:"
        c_v_cea = [defaultdict() for _ in range(self.n_col)]
        for r_i, r_obj in enumerate(m_p):
            if r_i in self.headers:
                continue
            for c_i, c_obj in enumerate(r_obj):
                if not self._tar_cea.is_tar(r_i, c_i):
                    continue
                if not len(c_obj["final"]):
                    log_m += "\n[%d][%d] - ErrorCEA: Missing %s" % (
                        r_i,
                        c_i,
                        self.cells[r_i][c_i],
                    )
                    continue

                # Get top 1
                # wd_labels = m_f.m_wiki_items().get_label_all(c_obj["final"][0][0])

                # if len(wd_labels):  # and not st.RES_HIST.get(self.cells[r_i][c_i])
                # surface_cea = max([sim.sim_string_fuzz(self.cells[r_i][c_i], _v_text, is_lower=False)
                #                    for _v_text in wd_labels])

                # if surface_cea >= 0.5 or st.RES_HIST.get(self.cells[r_i][c_i]):
                # res_cea.append([self.table_id, r_i, c_i, c_obj["final"][0][0]])
                # res_cea_dict[(r_i, c_i)] = c_obj["final"][0][0]
                # else:
                # res_cea.append([self.table_id, r_i, c_i, c_obj["final"][0][0]])
                res_cea_dict[(r_i, c_i)] = c_obj["final"][0][0]

                if c_v_cea[c_i].get(self.cells[r_i][c_i]) is None:
                    c_v_cea[c_i][self.cells[r_i][c_i]] = defaultdict(float)
                if res_cea_dict.get((r_i, c_i)):
                    c_v_cea[c_i][self.cells[r_i][c_i]][res_cea_dict[(r_i, c_i)]] += 1

                log_m += "\n[%d][%d] - %s" % (r_i, c_i, self.cells[r_i][c_i])
                for cea_i, (cea_id, cea_score) in enumerate(c_obj["final"][:3]):
                    wd_label = str(m_f.m_items().get_label(cea_id))
                    log_m += "\n    %d\t%.5f\t%s | %s" % (
                        cea_i + 1,
                        cea_score,
                        cea_id,
                        wd_label,
                    )

        for r_i, c_i in self._tar_cea.tars():
            wd_id = res_cea_dict.get((r_i, c_i))
            # if c_i != self.core_attribute:
            #     is_get_fix = True
            #     if wd_id:
            #         wd_labels = m_f.m_wiki_items().get_label_all(wd_id)
            #         wd_score = max([sim.sim_string_fuzz(self.cells[r_i][c_i], _v_text)
            #                         for _v_text in wd_labels])
            #         if wd_score > 0.8:
            #             is_get_fix = False
            #
            #     if is_get_fix:
            #         new_id = None
            #         if c_v_cea[c_i].get(self.cells[r_i][c_i]):
            #             new_id = ul.get_most_popular_wd(c_v_cea[c_i][self.cells[r_i][c_i]])
            #
            #         if new_id and (not wd_id or wd_id != new_id):
            #             wd_id = new_id
            #             wd_labels = m_f.m_wiki_items().get_labels(wd_id)
            #             log_m += "\n Fix CEA: [%d][%d] %s | %s - %s" % (r_i, c_i, wd_id, str(wd_labels),
            #                                                             self.cells[r_i][c_i])
            if wd_id:
                res_cea.append([self.table_id, r_i, c_i, wd_id])
            else:
                log_m += "\nErrorCEA: Remove [%d][%d] - %s" % (
                    r_i,
                    c_i,
                    self.cells[r_i][c_i],
                )
                # log_m += "\nErrorCEA: Remove - %s" % self.cells[r_i][c_i]

        # one_more_step = True
        # alpha = 0
        # while one_more_step and count_perfect > 0:
        #     tmp_c_type = sorted([[c_type, c_score]
        #                         for c_type, c_score in c_types.items() if c_score >= (count_perfect - alpha)],
        #                         key=lambda x: x[1])
        #     if len(tmp_c_type):
        #         one_more_step = False
        #         c_types = tmp_c_type
        #     alpha += 1
        # c_types_direct = ul.cal_p_from_ranking(c_types_direct, is_sort=True)
        # tmp_c_types = f.dump_wikidata().filter_dbpedia_type(c_types)
        # if len(tmp_c_types):
        #     c_types = tmp_c_types
        #     c_types = sorted(c_types.items(), key=lambda x: x[1], reverse=True)

        # Combine score vote for CTA
        c_types_direct = [defaultdict(float) for _ in range(self.n_col)]
        for _, r_i, c_i, cea_wd in res_cea:
            e_types = m_f.m_items().get_types_specific_dbpedia(cea_wd)
            if not e_types:
                continue
            for e_type in e_types:
                c_types_direct[c_i][e_type] += 1.0

        c_types_direct = [
            sorted(c_obj.items(), key=lambda x: x[1], reverse=True)
            for c_obj in c_types_direct
        ]

        log_m += "\nCTA:"
        for c_i in self._tar_cta.tars():
            if not len(c_types_direct[c_i]):
                log_m += "\n[%d] - ErrorCTA: Missing" % c_i
                continue
            c_types_direct_dup = [
                c_type[0]
                for c_type in c_types_direct[c_i]
                if c_type[1] == c_types_direct[c_i][0][1]
            ]

            # if len(c_types_direct_dup) > 1:
            #     log_m += "\nErrorCTA: Duplicate"
            #     c_types_direct_dup = {c_type: m_f.m_wiki_items().get_popularity(c_type) for c_type in c_types_direct_dup}
            #     c_types_direct_dup = sorted(c_types_direct_dup.items(), key=lambda x: x[1], reverse=True)
            #     cta_final = c_types_direct_dup[0][0]
            # else:
            #     cta_final = c_types_direct_dup[0]
            # c_types_direct_dup = {c_type: m_f.m_wiki_items().get_popularity(c_type) for c_type in c_types_direct_dup}
            # c_types_direct_dup = sorted(c_types_direct_dup.items(), key=lambda x:x[1], reverse=True)
            cta_final = c_types_direct_dup
            # cta_final = m_f.m_items().get_lowest_types(c_types_direct_dup)

            if cta_final:
                res_cta.append([self.table_id, c_i, cta_final])
                log_m += "\n[%d] - Select: %s[%s]" % (
                    c_i,
                    cta_final,
                    str(m_f.m_items().get_label(cta_final)),
                )
            else:
                log_m += "\nErrorCTA: Duplicate Ignore"

            c_types_direct[c_i] = ul.cal_p_from_ranking(
                c_types_direct[c_i], is_sort=True
            )

            for cta_i, (cta_id, cta_score) in enumerate(c_types_direct[c_i][:3]):
                log_m += "\n    %d\t%.5f\t%s | %s " % (
                    cta_i + 1,
                    cta_score,
                    cta_id,
                    str(m_f.m_items().get_label(cta_id)),
                )
        res_cpa = [
            [self.table_id, r_i, c_i, wd_p] for (r_i, c_i), wd_p in res_cpa.items()
        ]

        log_m += log_m_cpa

        return res_cea, res_cta, res_cpa, log_m, time() - start
