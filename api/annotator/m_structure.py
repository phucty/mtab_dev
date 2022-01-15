import m_config as cf
import numpy as np
from collections import defaultdict
from api import m_f
from api.utilities import m_utils as ul
from api.annotator.m_input import TargetCTA, TargetCEA, TargetCPA


def parse_data_type(table_obj):
    # Get basic stats
    n_row = len(table_obj["cell"]["values"])
    n_col = 0
    n_miss = 0
    while len(table_obj["cell"]["values"][n_row - 1]) == 0 and n_row > 0:
        n_row -= 1

    # cell_dtype_duck = [[defaultdict(int) for _ in r] for r in table_obj["cell"]["values"]]
    cell_dtype_spacy = [
        [defaultdict(int) for _ in r] for r in table_obj["cell"]["values"]
    ]
    col_dtype_spacy = defaultdict()
    col_dtype_duck = defaultdict()

    for row_id, r in enumerate(table_obj["cell"]["values"]):
        if len(r) > n_col:
            n_col = len(r)
        for col_id, c_value in enumerate(r):
            if not c_value or c_value.lower() in cf.NONE_CELLS:
                n_miss += 1
            if ul.isEnglish(c_value):
                dtype_cell = m_f.m_spacy().get_type(c_value)
            else:
                dtype_cell = m_f.m_spacy().get_type(c_value, lang="all")
            # cell_dtype_duck[row_id][col_id] = f.duck().get_most_type(c_value)
            cell_dtype_spacy[row_id][col_id] = dtype_cell

            if col_dtype_duck.get(col_id, None) is None:
                col_dtype_duck[col_id] = defaultdict(int)
            if col_dtype_spacy.get(col_id, None) is None:
                col_dtype_spacy[col_id] = defaultdict(int)

            # col_dtype_duck[col_id][cell_dtype_duck[row_id][col_id]] += 1
            col_dtype_spacy[col_id][dtype_cell] += 1
    table_obj["cell"]["dtype_spacy"] = cell_dtype_spacy

    # col_dtype_duck = [
    #     sorted(col_dtype_duck.get(i, defaultdict(int)).items(), key=lambda x: x[1], reverse=True) for i in range(n_col)
    # ]
    col_dtype_spacy = [
        sorted(
            col_dtype_spacy.get(i, defaultdict(int)).items(),
            key=lambda x: x[1],
            reverse=True,
        )
        for i in range(n_col)
    ]
    # col_dtype_spacy = [cf.DataType.NUM if not len(c) else c[0][0] for c in col_dtype_spacy]

    table_obj.update(
        {
            "col": {
                # "dtype_duck": col_dtype_duck,
                "dtype_spacy": col_dtype_spacy
            },
            "stats": {
                "row": n_row,
                "col": n_col,
                "miss": n_miss,
                "cell": n_row * n_col - n_miss,
                "r_cell": (n_row * n_col - n_miss) / (n_row * n_col)
                if (n_row * n_col)
                else 0,
            },
        }
    )


def predict_headers(table_obj, n_first_rows=cf.HEADER_FIRST_ROW):
    if n_first_rows >= len(table_obj["cell"]["values"]):
        n_first_rows = 0

    data_len = []
    i_data_len = 3
    while not len(data_len) and i_data_len > 0:
        data_len = [
            sum([len(c) for c in r if len(r) >= (table_obj["stats"]["col"] / 2)])
            for r in table_obj["cell"]["values"][i_data_len:]
            if len(r)
        ]
        i_data_len -= 1

    q1, q3 = np.quantile(data_len, [0.01, 0.99])
    # len_rows = [sum([len(c) for c in r]) for r in table_dump["cell_norm_values"][n_first_rows:] if len(r)]
    # q1 = min(len_rows)
    # q3 = max(len_rows)
    # cut_off = (q3 - q1) * 1.5
    # lower, upper = q1 - cut_off, q3 + cut_off
    # avg_len_rows = np.median([sum([len(c) for c in r]) for r in table_dump["cell_values"] if len(r)])
    col_dtype_spacy = [
        cf.DataType.NONE if not c else c[0][0] for c in table_obj["col"]["dtype_spacy"]
    ]
    titles = []
    headers = []
    for row_i in range(3):
        if (
            len(table_obj["cell"]["values"][row_i]) < 2
            and table_obj["stats"]["col"] >= 2
        ):
            titles.append(row_i)
        else:
            # row_d_type = [cf.DataType.NUM if t != cf.DataType.TEXT else cf.DataType.TEXT for t in table["dtype_ducks"][row_i]]
            # row_e_type = [cf.DataType.NUM if t in cf.NUM_SPACY else cf.DataType.TEXT for t in table["dtype_spacys"][row_i]]
            row_e_type = table_obj["cell"]["dtype_spacy"][row_i]
            row_len = sum([len(c) for c in table_obj["cell"]["values"][row_i]])
            is_semtab_header = False
            if (
                sum([1 for cell in table_obj["cell"]["values"][row_i] if "col" in cell])
                >= table_obj["stats"]["col"] // 2
            ):
                is_semtab_header = True

            is_header = False
            if (
                is_semtab_header
                or row_e_type != col_dtype_spacy
                or (
                    sum(row_e_type) == len(row_e_type)
                    and len(data_len) > 2
                    and (row_len < q1 or row_len > q3)
                )
            ):  # or
                is_header = True
            # if not is_all_text_top_dtype_duck:
            #     if row_dtype_duck != top_dtype_duck:
            #         is_header = True
            # else:
            #     if row_dtype_spacy != top_dtype_spacy:
            #         is_header = True
            if is_header:
                headers.append(row_i)
            else:
                break
            if len(headers) < 3:
                # try one more
                while True:
                    if (row_i + 1) >= len(table_obj["cell"]["values"]):
                        break
                    current_row = table_obj["cell"]["values"][row_i]
                    next_row = table_obj["cell"]["values"][row_i + 1]
                    overlapping_cells = set(current_row) & set(next_row)
                    overlapping_cells = {oc for oc in overlapping_cells if len(oc)}
                    if (
                        len(next_row) >= len(current_row)
                        and len(overlapping_cells) >= len(current_row) // 2
                    ):
                        headers.append(row_i + 1)
                        row_i = row_i + 1
                    else:
                        break

                while True:
                    if (row_i + 1) >= len(table_obj["cell"]["values"]):
                        break
                    next_row = table_obj["cell"]["values"][row_i + 1]
                    if len(next_row) <= (table_obj["stats"]["col"] / 2):
                        headers.append(row_i + 1)
                        row_i = row_i + 1
                    else:
                        break
                break

    table_obj["headers"] = titles + headers
    # return table_obj


def select_dtype_column(table_obj):
    # Clean column data types
    temp_dtype = [
        {i: s for i, s in c if i != cf.DataType.NONE}
        for c in table_obj["col"]["dtype_spacy"]
    ]
    for r in table_obj["headers"]:
        if table_obj["cell"]["dtype_spacy"][r]:
            for c_i, c in enumerate(table_obj["cell"]["dtype_spacy"][r]):
                if temp_dtype[c_i].get(c):
                    temp_dtype[c_i][c] -= 1
    temp_dtype = [
        sorted(c.items(), key=lambda x: x[1], reverse=True) for c in temp_dtype
    ]
    col_dtype_spacy = []
    for c in temp_dtype:
        if not len(c):
            col_dtype_spacy.append(cf.DataType.NUM)
        else:
            if c[0][0] == cf.DataType.NONE and len(c) > 1 and c[1][1]:
                col_dtype_spacy.append(c[1][0])
            else:
                col_dtype_spacy.append(c[0][0])
    table_obj["col"]["dtype_spacy"] = col_dtype_spacy


def predict_core_attribute(table_obj, tar_cpa, tar_cta):
    # Predict core attribute
    core_attribute = []
    if tar_cpa:
        core_attribute.append([tar_cpa.core_attribute(), 10])
    # elif tar_cta:
    #     core_attribute.append([tar_cta.core_attribute(), 8])

    else:
        values_in_cols = defaultdict(list)
        for row_id, r in enumerate(table_obj["cell"]["values"]):
            if row_id in table_obj["headers"] or len(r) < (
                table_obj["stats"]["col"] / 2
            ):
                continue
            for col_id, c in enumerate(r):
                if 0 < len(c) <= 200:
                    values_in_cols[col_id].append(c)

        avg_len_cols = {
            col_i: (
                sum([len(c_value) for c_value in c_values])
                * 1.0
                / len(values_in_cols[col_i])
            )
            for col_i, c_values in values_in_cols.items()
        }

        for col_id, cell_values in values_in_cols.items():
            # len_set_values = len({" ".join([w for w in cell_value if w.isalpha()]) for cell_value in cell_values})
            len_set_values = len(set(cell_values))
            len_mis_values = (
                table_obj["stats"]["row"] - len(cell_values) - len(table_obj["headers"])
            )
            uniqueness = (len_set_values - len_mis_values) * 1.0 / len(cell_values)
            if table_obj["col"]["dtype_spacy"][col_id]:
                if (
                    3 <= avg_len_cols[col_id] <= 200
                    and table_obj["col"]["dtype_spacy"][col_id] == cf.DataType.TEXT
                ):  #   and uniqueness >= 0.15 \
                    core_attribute.append([col_id, uniqueness])
        core_attribute.sort(key=lambda x: x[1], reverse=True)

    # Get top 1
    if len(core_attribute):
        table_obj["core_attribute"] = core_attribute[0][0]
    else:
        table_obj["core_attribute"] = 0
    return table_obj


def predict_targets(
    table_obj,
    tar_cea=None,
    tar_cta=None,
    tar_cpa=None,
    limit_cea=cf.LIMIT_CEA_TAR,
    predict_target=False,
):
    is_predicted = False
    if tar_cea is None and tar_cta is None and tar_cpa is None:
        predict_target = True

    if predict_target:
        if not tar_cta:
            tar_cta = TargetCTA(table_obj["name"])
            for i, dtype in enumerate(table_obj["col"]["dtype_spacy"]):
                if dtype == cf.DataType.TEXT:
                    tar_cta.add(i, None)

        if not tar_cpa:
            tar_cpa = TargetCPA(table_obj["name"])
            for i in range(table_obj["stats"]["col"]):
                if i == table_obj["core_attribute"]:
                    continue
                tar_cpa.add(table_obj["core_attribute"], i, None)

        if not tar_cea:
            tar_cea = TargetCEA(table_obj["name"])
            for r_i, r_dtype in enumerate(table_obj["cell"]["dtype_spacy"]):
                if (
                    r_i in table_obj["headers"]
                    or len(r_dtype) - 1 < table_obj["core_attribute"]
                ):
                    continue

                if tar_cea.n > limit_cea:
                    break

                c_i, c_dtype = (
                    table_obj["core_attribute"],
                    r_dtype[table_obj["core_attribute"]],
                )
                if (
                    table_obj["col"]["dtype_spacy"][c_i] == cf.DataType.NUM
                    or not table_obj["cell"]["values"][r_i][c_i]
                    or table_obj["cell"]["values"][r_i][c_i].lower() in cf.NONE_CELLS
                ):
                    continue
                tar_cea.add(r_i, c_i, None)
            is_predicted = True
            for r_i, r_dtype in enumerate(table_obj["cell"]["dtype_spacy"]):
                if r_i in table_obj["headers"]:
                    continue

                if tar_cea.n > limit_cea:
                    break

                for c_i, c_dtype in enumerate(r_dtype):
                    if c_i == table_obj["core_attribute"]:
                        continue
                    if (
                        table_obj["col"]["dtype_spacy"][c_i] == cf.DataType.NUM
                        or not table_obj["cell"]["values"][r_i][c_i]
                        or table_obj["cell"]["values"][r_i][c_i].lower()
                        in cf.NONE_CELLS
                    ):
                        continue
                    tar_cea.add(r_i, c_i, None)

    table_obj.update(
        {
            "tar": {
                "cea": tar_cea,
                "cta": tar_cta,
                "cpa": tar_cpa,
                "is_predicted": is_predicted,
            }
        }
    )


def run(
    table_obj,
    tar_cea=None,
    tar_cta=None,
    tar_cpa=None,
    limit_cea=cf.LIMIT_CEA_TAR,
    predict_target=False,
):
    parse_data_type(table_obj)
    if tar_cea:
        table_obj["headers"] = tar_cea.headers()
        if not table_obj["headers"]:
            predict_headers(table_obj)
    else:
        predict_headers(table_obj)
    select_dtype_column(table_obj)
    predict_core_attribute(table_obj, tar_cpa, tar_cta)
    predict_targets(
        table_obj,
        tar_cea,
        tar_cta,
        tar_cpa,
        limit_cea=limit_cea,
        predict_target=predict_target,
    )
