from time import time

import m_config as cf
import numpy as np
import os
import re
from api import m_f
from api.annotator import m_input
from api.lookup import m_entity_search
from api.utilities import m_io as iw
from api.utilities import m_sim as sim
from api.utilities import m_utils as ul
from collections import defaultdict
from contextlib import closing
from multiprocessing.pool import Pool
from tqdm import tqdm


def cal_similarity(query, item_labels, max_limit=1):
    if not query:
        return 0

    if query in item_labels:
        return 1

    max_sim = 0
    for item_label in item_labels:
        max_sim = max(max_sim, sim.sim_string_fuzz(query, item_label, is_lower=False))
        if max_sim == max_limit:
            return max_sim

    return max_sim


def norm_table_cell(cell_value):
    cell_value = cell_value.replace("T00:00:00Z", "")
    cell_value = cell_value.replace("/", "-")
    cell_value = re.sub("\[(.*)\]", "", cell_value)
    return cell_value


def get_max_similarity(cell_value, wd_id):
    cell_value = norm_table_cell(cell_value)

    if not cell_value or cell_value.lower() in cf.NONE_CELLS:
        return 0

    is_multilingual = ul.isEnglish(cell_value)
    wd_labels = m_f.m_items().get_labels(wd_id, multilingual=is_multilingual)
    if cell_value in wd_labels:
        return 1

    return cal_similarity(cell_value, wd_labels)


def get_value_matching(
    row_values,
    wd_id,
    is_target_prediction=False,
    core_attribute=0,
    is_multilingual=False,
    cpa_tar=None,
):
    arg_sim, col_sims, col_best_str = 0, None, None
    if not row_values:
        return 0, None, None
    statements = m_f.m_items().get_statement_values(wd_id, multilingual=is_multilingual)
    if not statements:
        return 0, None, None

    # col_sims = [defaultdict(float) for _ in range(len(row_values))]
    # col_sims_inv = [defaultdict(float) for _ in range(len(row_values))]
    col_best_str = [None for _ in range(len(row_values))]
    col_best_sim = [0 for _ in range(len(row_values))]

    def update_search_space(_search_space):
        augment_space = defaultdict(set)
        for v_text, e_values in _search_space.items():
            if "-00-00" in v_text:
                augment_space[v_text.replace("-00-00", "")] = e_values
            if "(" in v_text:
                remove_1 = v_text.replace("(", "").replace(")", "")
                remove_2 = re.sub("\((.*)\)", "", v_text)
                augment_space[remove_1] = e_values
                augment_space[remove_2] = e_values
        _search_space.update(augment_space)
        return _search_space

    statements["entity"] = update_search_space(statements["entity"])
    statements["text"] = update_search_space(statements["text"])

    def find_best_cans(is_inverse=False):
        col_sims = [defaultdict(float) for _ in range(len(row_values))]
        c_valid_cells = 0
        for cell_i, (cell_value, is_entity) in enumerate(row_values):
            cell_value = norm_table_cell(cell_value)
            # cell_value = cell_value.replace("-01-01", "")
            if (
                not cell_value
                or cell_value.lower() in cf.NONE_CELLS
                or cell_i == core_attribute
            ):
                continue

            is_num = ul.convert_num(cell_value)
            # Check index column
            if (
                is_num
                and is_num.is_integer()
                and 0 <= is_num < 1e6
                and cell_i < core_attribute
            ):
                continue

            c_valid_cells += 1
            if is_num is None:
                cell_texts = {cell_value}
                if "-01-01" in cell_value:
                    cell_texts.add(cell_value.replace("-01-01", ""))
                if is_inverse:
                    is_entity = 0 if is_entity == 1 else 1

                if is_entity == 1:
                    search_space = statements["entity"]
                else:
                    search_space = statements["text"]

                l_e_values = defaultdict()
                for _cell_text in cell_texts:
                    e_values = search_space.get(_cell_text)
                    if e_values:
                        l_e_values[_cell_text] = e_values

                if l_e_values:
                    for v_text, e_values in l_e_values.items():
                        for e_value in e_values:
                            col_sims[cell_i][e_value] = 1
                        if col_best_sim[cell_i] < 1:
                            col_best_sim[cell_i] = 1
                            col_best_str[cell_i] = v_text
                else:
                    cell_texts_len = np.mean(
                        [len(_cell_text) for _cell_text in cell_texts]
                    )
                    edit_distance_ratio = 10
                    # v_texts = sorted(search_space.keys(), key=lambda x: abs(cell_texts_len - len(x)))
                    v_texts = [
                        v_text
                        for v_text in search_space
                        if cell_texts_len - 10 < len(v_text) < cell_texts_len + 10
                    ]
                    for v_text in v_texts:
                        e_values = search_space[v_text]
                        if not v_text:
                            continue
                        v_max_score = cal_similarity(v_text, cell_texts)
                        # v_max_score = max(
                        #     [
                        #         sim.sim_string_fuzz(_cell_text, v_text, is_lower=False)
                        #         for _cell_text in cell_texts
                        #     ]
                        # )
                        for e_value in e_values:
                            if v_max_score > 0.8:
                                col_sims[cell_i][e_value] = max(
                                    v_max_score, col_sims[cell_i][e_value]
                                )
                        if col_best_sim[cell_i] < v_max_score:
                            col_best_sim[cell_i] = v_max_score
                            col_best_str[cell_i] = v_text
            else:
                # string similarity
                e_values = statements["num"].get(cell_value)
                if e_values:
                    for e_value in e_values:
                        col_sims[cell_i][e_value] = 1
                    if col_best_sim[cell_i] < 1:
                        col_best_sim[cell_i] = 1
                        col_best_str[cell_i] = cell_value

                if not len(col_sims[cell_i]):
                    for v_text, e_values in statements["num"].items():
                        tmp_score = sim.sim_percentage_change(cell_value, v_text)

                        if tmp_score > cf.min_sim_value:
                            for e_value in e_values:
                                col_sims[cell_i][e_value] = max(
                                    tmp_score, col_sims[cell_i][e_value]
                                )
                        # else:
                        #     # detect unit change
                        #     if cpa_tar and cpa_tar.is_tar(core_attribute, cell_i):
                        #         tmp_score = sim.sim_fuzz_partial_ratio(
                        #             v_text, cell_value
                        #         )
                        #         for e_value in e_values:
                        #             col_sims[cell_i][e_value] = max(
                        #                 tmp_score, col_sims[cell_i][e_value]
                        #             )

                        if col_best_sim[cell_i] < tmp_score:
                            col_best_sim[cell_i] = tmp_score
                            col_best_str[cell_i] = v_text

                if not len(col_sims[cell_i]):
                    e_values = statements["text"].get(cell_value)
                    if e_values:
                        for e_value in e_values:
                            col_sims[cell_i][e_value] = 1
                        if col_best_sim[cell_i] < 1:
                            col_best_sim[cell_i] = 1
                            col_best_str[cell_i] = cell_value
                    else:
                        for v_text, e_values in statements["text"].items():
                            if (
                                len(cell_value) - 10
                                < len(v_text)
                                < len(cell_value) + 10
                            ):
                                continue

                            tmp_score = sim.sim_string_fuzz(cell_value, v_text)
                            if tmp_score > 0.8:
                                for e_value in e_values:
                                    col_sims[cell_i][e_value] = max(
                                        tmp_score, col_sims[cell_i][e_value]
                                    )
                            if col_best_sim[cell_i] < tmp_score:
                                col_best_sim[cell_i] = tmp_score
                                col_best_str[cell_i] = v_text

        col_sims = [
            sorted(col_i.items(), key=lambda x: x[1], reverse=True)
            for col_i in col_sims
        ]
        return col_sims, c_valid_cells

    col_sims, c_valid_cells = find_best_cans(is_inverse=False)
    if is_target_prediction:
        col_sims_inv, c_valid_cells = find_best_cans(is_inverse=True)
        col_sims = [{p: s for p, s in col_i} for col_i in col_sims]
        for col_i, col_obj in enumerate(col_sims_inv):
            for p, s in col_obj:
                col_sims[col_i][p] = max(s, col_sims[col_i].get(p, 0))
        col_sims = [
            sorted(col_i.items(), key=lambda x: x[1], reverse=True)
            for col_i in col_sims
        ]
    # col_max_sim = [col_score[0][1] if len(col_score) else 0 for col_score in col_sims]
    # Select the best match
    best_match = [[] for _ in range(len(row_values))]
    for col_i, col_stats in enumerate(col_sims):
        for stat, score in col_stats:
            if score < col_best_sim[col_i]:
                break

            best_match[col_i].append((stat, score))

    # avg_sim = sum(col_best_sim) / len(col_sims)
    c_valid_cells = 1 if not c_valid_cells else c_valid_cells
    avg_sim = sum([s[0][1] for s in best_match if s]) / c_valid_cells

    return avg_sim, best_match, col_best_str


def generate_candidates(table_obj, limit=cf.LIMIT_GEN_CAN, mode="a"):
    lk_results = defaultdict()

    cans = [[[] for _ in r] for r in table_obj["cell"]["values"]]

    for r_i, r_obj in enumerate(table_obj["cell"]["values"]):
        for c_i, c_obj in enumerate(r_obj):

            if (
                r_i in table_obj["headers"]
                or not c_obj
                or c_obj.lower() in cf.NONE_CELLS
            ):
                continue

            # if c_i != table_obj["core_attribute"]:
            #     continue

            # if c_i == table_obj["core_attribute"]:
            #     mode = "a"
            # else:
            #     mode = "b"

            if table_obj["tar"]["cea"] and table_obj["tar"]["cea"].is_tar(r_i, c_i):
                responds = lk_results.get(c_obj)
                if responds is None:
                    responds, _, _ = m_entity_search.search(
                        c_obj, limit=limit, mode=mode, expensive=False
                    )
                    lk_results[c_obj] = responds
                if responds:
                    cans[r_i][c_i] = responds
    return cans


def run(table_obj, limit=cf.LIMIT_GEN_CAN, search_mode="a"):
    res_cea, res_cpa, res_cta = [], defaultdict(), []
    log_m = "Table %s: Size: %dx%d - %d cells" % (
        table_obj["name"],
        table_obj["stats"]["row"],
        table_obj["stats"]["col"],
        table_obj["stats"]["cell"],
    )
    log_m += "\n" + iw.print_table(
        table_obj["cell"]["values"], table_obj["stats"]["col"]
    )
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
            for _ in range(table_obj["stats"]["col"])
        ]
        if r_i not in table_obj["headers"]
        else []
        for r_i in range(table_obj["stats"]["row"])
    ]

    # entity lookup
    cans_e = generate_candidates(table_obj, limit=limit, mode=search_mode)

    # statement lookup
    # two cell search and cell 1 search given cell 2

    for r_i, r_obj in enumerate(cans_e):
        if r_i in table_obj["headers"]:
            continue
        for c_i, c_obj in enumerate(r_obj):
            # Aggregate one cell lookup
            m_p[r_i][c_i]["lk_e"] = {k: v for k, v in c_obj}

            # Aggregate two cell lookup
            # Cell 1 in core attribute: aggregation of all cell 2
            # Cell 2 in other columns:
            # if cans_s[r_i][c_i]:
            #     m_p[r_i][c_i]["lk_s"] = {k: v for k, v in cans_s[r_i][c_i]}

    value_mat_errors = set()

    p_cans = [defaultdict(float) for _ in range(table_obj["stats"]["col"])]
    p_cans_count = [defaultdict(int) for _ in range(table_obj["stats"]["col"])]
    c2_ps = defaultdict(set)
    c2_ps_inv = defaultdict(set)
    res_cea_dict = defaultdict()

    is_step_2 = True
    # if table_obj["n_col"] < 2:
    # is_step_2 = False
    log_m_cpa = ""
    matching_results = [defaultdict() for _ in range(table_obj["stats"]["row"])]
    remove_list = set()
    # Iterate 2 steps
    for step_i in range(2):
        if step_i == 1 and not is_step_2:
            continue
        if step_i == 1:
            log_m += "\nStep 2"
        # Value matching and CPA
        for r_i, r_obj in enumerate(m_p):
            if r_i in table_obj["headers"] or (
                step_i == 1 and r_i not in value_mat_errors
            ):
                continue
            # get cell text and cea gt
            # row_values = [
            #     [c_obj, 1 if table_obj["tar"]["cea"].is_tar(r_i, c_i) else 0]
            #     for c_i, c_obj in enumerate(table_obj["cell"]["values"][r_i])
            #     if c_i != table_obj["core_attribute"]
            # ]
            row_values = [
                [
                    c_obj,
                    1
                    if table_obj["tar"]["cea"]
                    and table_obj["tar"]["cea"].is_tar(r_i, c_i)
                    else 0,
                ]
                for c_i, c_obj in enumerate(table_obj["cell"]["values"][r_i])
            ]
            is_multilingual = any([not ul.isEnglish(c) for c, _ in row_values])

            if step_i == 0:
                e_candidates = sorted(
                    {
                        **r_obj[table_obj["core_attribute"]]["lk_s"],
                        **r_obj[table_obj["core_attribute"]]["lk_e"],
                    }.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )

            else:
                old_cans = set(
                    {
                        **r_obj[table_obj["core_attribute"]]["lk_s"],
                        **r_obj[table_obj["core_attribute"]]["lk_e"],
                    }.keys()
                )
                e_candidates = r_obj[table_obj["core_attribute"]]["lk_s_type"]
                e_candidates = {
                    wd: wd_s for wd, wd_s in e_candidates.items() if wd not in old_cans
                }
                if len(m_p[r_i][table_obj["core_attribute"]]["value"]):
                    e_candidates.update(m_p[r_i][table_obj["core_attribute"]]["value"])
                e_candidates = sorted(
                    e_candidates.items(), key=lambda x: x[1], reverse=True
                )

            if matching_results[r_i]:
                e_candidates = [
                    [wd_id, wd_score]
                    for wd_id, wd_score in e_candidates
                    if wd_id not in matching_results[r_i]
                ]

            m_match_v_res = []
            is_ok = False
            for wd_i, (wd_id, _) in enumerate(e_candidates):
                avg_sim, best_match, best_str = get_value_matching(
                    row_values,
                    wd_id,
                    is_target_prediction=table_obj["tar"].get("is_predicted"),
                    core_attribute=table_obj["core_attribute"],
                    is_multilingual=is_multilingual,
                    cpa_tar=table_obj["tar"]["cpa"],
                )
                matching_results[r_i][wd_id] = [avg_sim, best_match, best_str]

                if avg_sim > 0:
                    m_match_v_res.append([wd_id, avg_sim, best_match, best_str])
                # if avg_sim == 1:
                #     is_ok = True
                # if wd_i > 10 and is_ok:
                #     break

            if not len(m_match_v_res):
                value_mat_errors.add(r_i)
            else:
                is_lk_type = False
                m_match_v_res = sorted(m_match_v_res, key=lambda x: x[1], reverse=True)
                if (
                    sum([1 for item in m_match_v_res if item[1] == m_match_v_res[0][1]])
                    > 1
                ):
                    max_score = 0
                    count_max = 0
                    for wd_id, _, _, _ in m_match_v_res:
                        wd_labels = m_f.m_items().get_labels(
                            wd_id, multilingual=is_multilingual
                        )
                        stat_sim = 0
                        if wd_labels:
                            stat_sim = cal_similarity(
                                table_obj["cell"]["values"][r_i][
                                    table_obj["core_attribute"]
                                ],
                                wd_labels,
                            )

                        if stat_sim > max_score:
                            max_score = stat_sim
                            count_max = 1
                        elif stat_sim == max_score:
                            count_max += 1

                    # if count_max > 2:
                    #     remove_list.add(r_i)

                m_match_v_res = [
                    item
                    for item in m_match_v_res
                    if item[1] > m_match_v_res[0][1] * 0.9
                ]

                if m_match_v_res[0][1] < 0.8:
                    value_mat_errors.add(r_i)
                    is_lk_type = True

                is_error = False
                for wd_rank, (wd_id, avg_sim, best_match, best_str) in enumerate(
                    m_match_v_res
                ):
                    wd_labels = m_f.m_items().get_labels(
                        wd_id, multilingual=is_multilingual
                    )
                    stat_sim = 0
                    if wd_labels:
                        stat_sim = cal_similarity(
                            table_obj["cell"]["values"][r_i][
                                table_obj["core_attribute"]
                            ],
                            wd_labels,
                        )
                        # stat_sim = max(
                        #     [
                        #         sim.sim_string_fuzz(
                        #             table_obj["cell"]["values"][r_i][
                        #                 table_obj["core_attribute"]
                        #             ],
                        #             _v_text,
                        #             is_lower=False,
                        #         )
                        #         for _v_text in wd_labels
                        #     ]
                        # )
                        if (
                            not table_obj["tar"].get("is_predicted")
                            and stat_sim < 0.65
                            and avg_sim < 0.65
                        ):
                            continue

                    m_p[r_i][table_obj["core_attribute"]]["value"][wd_id] = avg_sim
                    log_m += "\n"
                    log_m += f"{stat_sim:.2f}:{wd_id}[{str(m_f.m_items().get_label(wd_id))}] - {avg_sim:.2f}:"
                    log_m += "[%s] | " % ", ".join(
                        [
                            "%.2f" % (s_states[0][1]) if len(s_states) else "   0"
                            for s_states in best_match
                        ]
                    )
                    log_m += ", ".join([f"{str(e_value)}" for e_value in best_str])
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
                                if wd_rank == 0 and ul.is_wd_item(e_prop):
                                    if (
                                        table_obj["tar"].get("is_predicted")
                                        and col_score > 0.95
                                    ) or (
                                        not table_obj["tar"].get("is_predicted")
                                        and avg_sim > 0.85
                                    ):
                                        p_cans[c_prop_i][e_prop] += col_score * avg_sim
                                        p_cans_count[c_prop_i][e_prop] += 1

                                if (
                                    e_v_id
                                    and ul.is_wd_item(e_v_id)
                                    and r_i not in remove_list
                                ):
                                    if table_obj["tar"].get("is_predicted") or (
                                        not table_obj["tar"].get("is_predicted")
                                        and col_score > 0.65
                                    ):
                                        c2_ps[(r_i, c_prop_i, e_v_id)].add(
                                            (c_prop_i, e_prop)
                                        )
                                        c2_ps_inv[(c_prop_i, e_prop)].add(
                                            (r_i, c_prop_i, e_v_id)
                                        )
                                        m_p[r_i][c_prop_i]["value"][e_v_id] = max(
                                            col_score * avg_sim,
                                            m_p[r_i][c_prop_i]["value"][e_v_id],
                                        )

                if is_error and step_i == 1 and is_lk_type:  # \
                    # sum([1 for tmp_col_scores in best_match if not len(tmp_col_scores)]) > 0:
                    log_m += "\nMiss Value: %d. %s\n" % (
                        r_i,
                        str(table_obj["cell"]["values"][r_i]),
                    )
                else:
                    log_m += "\nOK Value: %d. %s\n" % (
                        r_i,
                        str(table_obj["cell"]["values"][r_i]),
                    )

        # prop two cells
        # c_prop_2cell = [defaultdict(float) for _ in range(table_obj["n_col"])]
        # for r_i, r_obj in enumerate(m_p):
        #     if r_i in table_obj["headers"]:
        #         continue
        #     for c_i, c_obj in enumerate(r_obj):
        #         for prop, prop_score in c_obj["prop"].items():
        #             c_prop_2cell[c_i][prop] += prop_score

        # p_cans = [
        #     {p_id: p_s for p_id, p_s in col_p.items() if p_cans_count[col_i][p_id] > table_obj["stats"]["row"] * 0.3}
        #     for col_i, col_p in enumerate(p_cans)
        # ]

        log_m_cpa = "\nCPA:"
        props_list = set()
        cell2_in_props_scores = defaultdict(float)
        cpa_tars = [] if not table_obj["tar"]["cpa"] else table_obj["tar"]["cpa"].tars()
        for col1, col2 in cpa_tars:
            is_inverse_prop = True if col2 == table_obj["core_attribute"] else False
            col_tar = col1 if is_inverse_prop else col2
            c_prop_res = p_cans[col_tar]

            # for i, c_prop_res in enumerate(p_cans):
            c_prop_res = {
                p_id: p_s
                for p_id, p_s in c_prop_res.items()
                if p_cans_count[col_tar][p_id] > table_obj["stats"]["row"] * 0.05
            }

            if (
                col_tar != table_obj["core_attribute"]
            ):  # table_obj["tar"]["cpa"] and table_obj["tar"]["cpa"].is_tar(table_obj["core_attribute"], i):
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
                        cell2_in_props_scores[(col_tar, prop_id)] = prop_score

                if not len(c_prop_res):
                    log_m_cpa += "\n[0][%d] - ErrorCPA: Missing" % col_tar
                    continue

                log_m_cpa += "\n[0][%d]" % col_tar
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
                    c_prop[0] for c_prop in c_prop_res if c_prop[1] == c_prop_res[0][1]
                ]
                if not is_inverse_prop:
                    res_cpa[(col1, col2)] = same_score
                else:
                    inverse_props = set()
                    for same_score_i in same_score:
                        inverse_prop = m_f.m_items().get_tail_obj_with_relation(
                            same_score_i, "P1696"
                        )
                        if inverse_prop:
                            inverse_props.update(inverse_prop)
                    res_cpa[(col1, col2)] = list(inverse_props)
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
            if r_i in table_obj["headers"]:
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
                surface_score_label = defaultdict(float)
                wd_candidates = set(lk_scores.keys())

                wd_candidates.update(m_p[r_i][c_i]["value"].keys())
                cell_value = norm_table_cell(table_obj["cell"]["values"][r_i][c_i])

                is_multilingual = ul.isEnglish(cell_value)
                for wd_id in wd_candidates:
                    avg_sim = 0
                    wd_labels = m_f.m_items().get_labels(wd_id, is_multilingual)

                    if wd_labels:
                        if cell_value in wd_labels:
                            avg_sim = 1
                        else:
                            avg_sim = cal_similarity(cell_value, wd_labels)

                    if avg_sim > 0.5:
                        surface_score[wd_id] = avg_sim

                    # if wd_labels:
                    #     avg_sim = max(
                    #         [
                    #             sim.sim_string_fuzz(
                    #                 table_obj["cell"]["values"][r_i][c_i],
                    #                 _v_text,
                    #                 is_lower=False,
                    #             )
                    #             for _v_text in wd_labels
                    #         ]
                    #     )
                    #     if avg_sim > 0.5:
                    #         surface_score[wd_id] = avg_sim

                    wd_label = m_f.m_items().get_label(wd_id)
                    if wd_label and wd_label == cell_value:
                        surface_score[wd_id] += 1
                        surface_score_label[wd_id] += 1

                for wd_id, wd_score in surface_score.items():
                    if lk_scores.get(wd_id):
                        lk_scores[wd_id] += wd_score
                    else:
                        lk_scores[wd_id] = wd_score

                value_score = ul.cal_p_from_ranking(m_p[r_i][c_i]["value"])
                for wd_id, wd_score in value_score.items():
                    attribute_weight = 5
                    if (
                        c_i != table_obj["core_attribute"]
                        and surface_score.get(wd_id, 0) >= 1
                    ):
                        attribute_weight = 10
                    if lk_scores.get(wd_id):
                        lk_scores[wd_id] += wd_score * attribute_weight
                    else:
                        lk_scores[wd_id] = wd_score * attribute_weight
                m_p[r_i][c_i]["surface"] = surface_score_label
                m_p[r_i][c_i]["final"] = ul.cal_p_from_ranking(lk_scores, is_sort=True)

        # Combine score vote for CTA
        c_types_direct = [defaultdict(float) for _ in range(table_obj["stats"]["col"])]
        c_types_direct_p = [
            defaultdict(float) for _ in range(table_obj["stats"]["col"])
        ]
        c_types_direct_wd = defaultdict(set)
        count_perfect = 0
        alpha = 0.0
        while alpha >= 0:
            for r_i, r_obj in enumerate(m_p):
                if r_i in table_obj["headers"]:
                    continue
                for c_i, c_obj in enumerate(r_obj):
                    if not len(c_obj["final"]):
                        continue

                    for wd_id, wd_p in c_obj["final"][:3]:
                        wd_types = c_types_direct_wd.get(wd_id)
                        if wd_types is None:
                            wd_types = m_f.m_items().get_instance_of(wd_id)
                            c_types_direct_wd[wd_id] = wd_types
                        if not wd_types:
                            continue
                        for e_type in wd_types:
                            c_types_direct_p[c_i][e_type] += wd_p

                    wd_id = c_obj["final"][0][0]
                    wd_p = c_obj["final"][0][1]
                    if wd_p >= alpha:
                        count_perfect += 1
                        tmp_types = m_f.m_items().get_instance_of(wd_id)
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
            if r_i in table_obj["headers"]:
                continue
            for c_i, c_obj in enumerate(r_obj):
                for wd_id in {**m_p[r_i][c_i]["lk_e"], **m_p[r_i][c_i]["lk_s"]}.keys():
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
                max(c_cans.values()) if len(c_cans) else 0 for c_cans in c_types_direct
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
                if r_i in table_obj["headers"] or r_i not in value_mat_errors:
                    continue
                c_value = table_obj["cell"]["values"][r_i][table_obj["core_attribute"]]
                c1_cans_types = set()
                for c2_wd in c1_c2_type_cans[table_obj["core_attribute"]]:
                    # Todo implement 2cell type lookup
                    # responds_c1 = f.lk_fz_res_2cell_types().get((c_value, c2_wd))
                    responds_c1 = None
                    if responds_c1:
                        responds_c1 = set(responds_c1[:100])
                    else:
                        responds_c1 = set()
                    c1_cans_types.update(responds_c1)

                # c1_cans_types = ul.get_c1_cans(c1_c2_type_cans[c_i], c_value)
                if len(c1_cans_types):
                    is_step_2 = True
                    m_p[r_i][table_obj["core_attribute"]][
                        "lk_s_type"
                    ] = ul.get_ranking_pr(
                        list(c1_cans_types),
                        mode_pr=cf.EnumPr.AVG,
                        mode_score=cf.EnumRank.EQUAL,
                    )

    log_m += "\n\nCEA:"
    c_v_cea = [defaultdict() for _ in range(table_obj["stats"]["col"])]
    for r_i, r_obj in enumerate(m_p):
        if r_i in table_obj["headers"]:
            continue
        for c_i, c_obj in enumerate(r_obj):
            if not table_obj["tar"]["cea"] or (
                not table_obj["tar"]["cea"].is_tar(r_i, c_i)
                # and not table_obj["tar"].get("is_predicted")
            ):
                continue
            if not len(c_obj["final"]):
                log_m += "\n[%d][%d] - ErrorCEA: Missing %s" % (
                    r_i,
                    c_i,
                    table_obj["cell"]["values"][r_i][c_i],
                )
                continue

            if r_i in remove_list:
                if c_i == table_obj["core_attribute"]:
                    continue

                wd_label = m_f.m_items().get_label(c_obj["final"][0][0])
                if not wd_label or wd_label != table_obj["cell"]["values"][r_i][c_i]:
                    continue

            # Get top 1
            # wd_labels = m_f.m_wiki_items().get_label_all(c_obj["final"][0][0])

            # if len(wd_labels):  # and not st.RES_HIST.get(table_obj["cell"]["values"][r_i][c_i])
            # surface_cea = max([sim.sim_string_fuzz(table_obj["cell"]["values"][r_i][c_i], _v_text, is_lower=False)
            #                    for _v_text in wd_labels])

            # if surface_cea >= 0.5 or st.RES_HIST.get(table_obj["cell"]["values"][r_i][c_i]):
            # res_cea.append([table_obj["name"], r_i, c_i, c_obj["final"][0][0]])
            # res_cea_dict[(r_i, c_i)] = c_obj["final"][0][0]
            # else:
            # res_cea.append([table_obj["name"], r_i, c_i, c_obj["final"][0][0]])
            res_cea_dict[(r_i, c_i)] = c_obj["final"][0][0]

            if c_v_cea[c_i].get(table_obj["cell"]["values"][r_i][c_i]) is None:
                c_v_cea[c_i][table_obj["cell"]["values"][r_i][c_i]] = defaultdict(float)
            if res_cea_dict.get((r_i, c_i)):
                c_v_cea[c_i][table_obj["cell"]["values"][r_i][c_i]][
                    res_cea_dict[(r_i, c_i)]
                ] += 1

            log_m += "\n[%d][%d] - %s" % (
                r_i,
                c_i,
                table_obj["cell"]["values"][r_i][c_i],
            )
            for cea_i, (cea_id, cea_score) in enumerate(c_obj["final"][:3]):
                wd_label = str(m_f.m_items().get_label(cea_id))
                log_m += "\n    %d\t%.5f\t%s | %s" % (
                    cea_i + 1,
                    cea_score,
                    cea_id,
                    wd_label,
                )

    # for r_i, c_i in table_obj["tar"]["cea"].tars():
    if table_obj["tar"].get("is_predicted"):
        cea_tars = list(res_cea_dict.keys())
    else:
        cea_tars = [[r_i, c_i] for r_i, c_i in table_obj["tar"]["cea"].tars()]
    for r_i, c_i in cea_tars:
        wd_id = res_cea_dict.get((r_i, c_i))
        # if c_i != table_obj["core_attribute"]:
        #     is_get_fix = True
        #     if wd_id:
        #         wd_labels = m_f.m_wiki_items().get_label_all(wd_id)
        #         wd_score = max([sim.sim_string_fuzz(table_obj["cell"]["values"][r_i][c_i], _v_text)
        #                         for _v_text in wd_labels])
        #         if wd_score > 0.8:
        #             is_get_fix = False
        #
        #     if is_get_fix:
        #         new_id = None
        #         if c_v_cea[c_i].get(table_obj["cell"]["values"][r_i][c_i]):
        #             new_id = ul.get_most_popular_wd(c_v_cea[c_i][table_obj["cell"]["values"][r_i][c_i]])
        #
        #         if new_id and (not wd_id or wd_id != new_id):
        #             wd_id = new_id
        #             wd_labels = m_f.m_wiki_items().get_labels(wd_id)
        #             log_m += "\n Fix CEA: [%d][%d] %s | %s - %s" % (r_i, c_i, wd_id, str(wd_labels),
        #                                                             table_obj["cell"]["values"][r_i][c_i])
        if wd_id:
            res_cea.append([table_obj["name"], r_i, c_i, wd_id])
        else:
            log_m += "\nErrorCEA: Remove [%d][%d] - %s" % (
                r_i,
                c_i,
                table_obj["cell"]["values"][r_i][c_i],
            )
            # log_m += "\nErrorCEA: Remove - %s" % table_obj["cell"]["values"][r_i][c_i]

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
    c_types_direct = [defaultdict(float) for _ in range(table_obj["stats"]["col"])]
    remove_col_type = set()
    if table_obj["tar"].get("is_predicted"):
        col_e_count = defaultdict(int)
        for _, r_i, c_i, cea_wd in res_cea:
            col_e_count[c_i] += 1
        for c_i, count in col_e_count.items():
            if count < col_e_count[table_obj["core_attribute"]] * 0.5:
                remove_col_type.add(c_i)

    for _, r_i, c_i, cea_wd in res_cea:
        if c_i in remove_col_type:
            continue
        e_types = m_f.m_items().get_instance_of(cea_wd)
        if not e_types:
            continue
        # if isinstance(m_p[r_i][c_i]["final"], list):
        #     m_p[r_i][c_i]["final"] = {k: v for k, v in m_p[r_i][c_i]["final"]}
        # if len(m_p[r_i][c_i]["final"]) > 1:
        #     change = m_p[r_i][c_i]["final"][0][1] - m_p[r_i][c_i]["final"][1][1]
        # elif m_p[r_i][c_i]["final"] == 1:
        #     change = m_p[r_i][c_i]["final"][0][1]
        # else:
        #     change = 0

        for e_type in e_types:
            if ul.is_wd_item(e_type):
                # c_types_direct[c_i][e_type] += change
                # c_types_direct[c_i][e_type] += m_p[r_i][c_i]["final"].get(cea_wd, 0)
                c_types_direct[c_i][e_type] += 1.0

    c_types_direct = [
        sorted(c_obj.items(), key=lambda x: x[1], reverse=True)
        for c_obj in c_types_direct
    ]

    log_m += "\nCTA:"
    for c_i in table_obj["tar"]["cta"].tars():
        if not len(c_types_direct[c_i]):
            log_m += "\n[%d] - ErrorCTA: Missing" % c_i
            continue
        c_types_direct_dup = [
            c_type[0]
            for c_type in c_types_direct[c_i]
            if c_type[0] and c_type[1] == c_types_direct[c_i][0][1]
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
        # cta_final = c_types_direct_dup
        cta_final = m_f.m_items().get_lowest_types(c_types_direct_dup)

        if cta_final:
            if len(cta_final) > 1:
                log_m += f"\n CTA: lowest common {cta_final}"
                supertypes = [
                    m_f.m_items().get_p279_all_distances(type_) for type_ in cta_final
                ]
                common_supertypes = set.intersection(
                    *([set(type_.keys()) for type_ in supertypes])
                ).difference(cf.WD_TOP)
                # log_m += "\n CTA: lowest common"
                if common_supertypes:
                    common_supertypes = {
                        type_: sum([s_type_.get(type_, 0) for s_type_ in supertypes])
                        for type_ in common_supertypes
                    }
                    common_supertypes = sorted(
                        common_supertypes.items(), key=lambda x: x[1]
                    )
                    log_m += f"\n common_supertypes {common_supertypes}"

                    cta_final = [common_supertypes[0][0]]
            res_cta.append([table_obj["name"], c_i, cta_final])
            log_m += "\n[%d] - Select: %s[%s]" % (
                c_i,
                cta_final,
                str(m_f.m_items().get_label(cta_final[0])),
            )
        else:
            log_m += "\nErrorCTA: Missing"

        c_types_direct[c_i] = ul.cal_p_from_ranking(c_types_direct[c_i], is_sort=True)

        for cta_i, (cta_id, cta_score) in enumerate(c_types_direct[c_i][:3]):
            log_m += "\n    %d\t%.5f\t%s | %s " % (
                cta_i + 1,
                cta_score,
                cta_id,
                str(m_f.m_items().get_label(cta_id)),
            )
    res_cpa = [
        [table_obj["name"], r_i, c_i, wd_p] for (r_i, c_i), wd_p in res_cpa.items()
    ]

    log_m += log_m_cpa
    table_obj.update(
        {
            "res_cea": res_cea,
            "res_cta": res_cta,
            "res_cpa": res_cpa,
            "log": log_m,
            "run_time": time() - start,
        }
    )

    return table_obj


def annotate(dir_tables, dir_cea, dir_cta, dir_cpa, n_cpu=1, is_screen=False):
    dir_csvs = sorted(iw.get_files_from_dir(dir_tables, extension="csv"))
    iw.print_status("Total: %d tables" % (len(dir_csvs)))
    tar_cea, n_cea = m_input.parse_target_cea(dir_cea)
    tar_cta, n_cta = m_input.parse_target_cta(dir_cta)
    tar_cpa, n_cpa = m_input.parse_target_cpa(dir_cpa)

    is_log = True
    cea, cta, cpa, new_lk = [], [], [], set()
    dir_csvs = dir_csvs[:1]

    with tqdm(total=len(dir_csvs)) as p_bar:

        def update_p_bar():
            p_bar.update()
            if is_log:
                iw.print_status(output_args["log"], is_screen=is_screen)
                run_time = output_args["run_time"]
                iw.print_status(
                    f"Table annotation time: ({run_time:.2f} seconds)",
                    is_screen=is_screen,
                )
            cea.extend(output_args["res_cea"])
            cta.extend(output_args["res_cta"])
            cpa.extend(output_args["res_cpa"])
            # p_bar.set_description(f"")

        args = []
        for dir_csv in dir_csvs:
            table_id = os.path.splitext(os.path.basename(dir_csv))[0]
            args_obj = {
                "dir_csv": dir_csv,
                "table_id": table_id,
                "tar_cea": tar_cea.get(table_id),
                "tar_cta": tar_cta.get(table_id),
                "tar_cpa": tar_cpa.get(table_id),
            }
            args.append(args_obj)

        if n_cpu == 1:
            for input_args in args:
                output_args = pool_table_annotation(input_args)
                update_p_bar()

        else:
            with closing(Pool(processes=n_cpu)) as p:
                for output_args in p.imap_unordered(pool_table_annotation, args):
                    update_p_bar()

    return cea, cta, cpa


if __name__ == "__main__":
    m_f.init()
    annotate(
        dir_tables=cf.DIR_SAMPLE_TABLES,
        dir_cea=cf.DIR_SAMPLE_TAR_CEA,
        dir_cta=cf.DIR_SAMPLE_TAR_CTA,
        dir_cpa=cf.DIR_SAMPLE_TAR_CPA,
        is_screen=True,
    )
