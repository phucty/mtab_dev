import os

from tqdm import tqdm

import m_config as cf
from api.utilities import m_io as iw
from api import m_f
from api.annotator import m_preprocess, m_input
from api.annotator import m_structure
from api.annotator import m_semantic
from multiprocessing.pool import Pool
from contextlib import closing
from time import time


def run(
    source_type,
    source,
    table_name="",
    predict_target=False,
    tar_cea=None,
    tar_cta=None,
    tar_cpa=None,
    limit_cea=cf.LIMIT_CEA_TAR,
    search_mode="a",
):
    start = time()

    def call_annotation():
        # 1. preprocessing
        table_obj = m_preprocess.run(source_type, source, table_name)
        # 2. Call structure annotation
        m_structure.run(
            table_obj,
            predict_target=predict_target,
            tar_cea=tar_cea,
            tar_cta=tar_cta,
            tar_cpa=tar_cpa,
            limit_cea=limit_cea,
        )
        # 4. Call semantic annotations
        m_semantic.run(table_obj, search_mode=search_mode)
        return table_obj

    table_obj = call_annotation()
    # try:
    #     table_obj = call_annotation()
    # except Exception as message:
    #     iw.print_status(message)
    #     raise ValueError
    return table_obj, time() - start


def pool_table_annotation(args):
    # try:
    table_obj, run_time = run(
        source_type=args["source_type"],
        source=args["source"],
        predict_target=args.get("predict_target", False),
        tar_cea=args["tar_cea"],
        tar_cta=args["tar_cta"],
        tar_cpa=args["tar_cpa"],
        limit_cea=args["limit_cea"],
    )
    return {
        "status": "Success",
        "table_obj": table_obj,
        "run_time": run_time,
    }


# except ValueError:
#     return {
#         "status": "Error",
#         "source": args["source"],
#     }


def annotate_api(
    source_type,
    dir_tables,
    dir_cea=None,
    dir_cta=None,
    dir_cpa=None,
    n_cpu=8,
    is_screen=False,
    limit=0,
    limit_cea=cf.LIMIT_CEA_TAR,
    predict_target=False,
):
    if isinstance(dir_tables, str):
        dir_tables = [dir_tables]
    else:
        dir_tables = sorted(dir_tables)
        if limit:
            dir_tables = dir_tables[:limit]
    iw.print_status("Total: %d tables" % (len(dir_tables)))
    tar_cea, tar_cta, tar_cpa = None, None, None
    if dir_cea:
        tar_cea, n_cea = m_input.parse_target_cea(dir_cea)
    if dir_cta:
        tar_cta, n_cta = m_input.parse_target_cta(dir_cta)
    if dir_cpa:
        tar_cpa, n_cpa = m_input.parse_target_cpa(dir_cpa)
    log_res = ""

    # table_dirs = [table_dir for table_dir in table_dirs if "1C9LFOKN" in table_dir]
    is_log = True
    responds = []

    with tqdm(total=len(dir_tables)) as p_bar:

        def update_p_bar():
            p_bar.update()
            if output_args["status"] == "Error":
                responds.append(output_args)
                return ""

            run_time = output_args["run_time"]
            if is_log:
                iw.print_status(output_args["table_obj"]["log"], is_screen=is_screen)
                iw.print_status(
                    f"Table annotation time: ({run_time:.2f} seconds)",
                    is_screen=is_screen,
                )

            log_res = "\n" + output_args["table_obj"]["log"]
            log_res += f"Table annotation time: ({run_time:.2f} seconds)"
            table_obj = {
                "status": "Success",
                # "file_name": os.path.basename(output_args["table"].dir_data),
                "name": output_args["table_obj"]["name"],
                "table_cells": output_args["table_obj"]["cell"]["values"],
                "run_time": output_args["run_time"],
                "structure": {
                    "encoding": output_args["table_obj"]["encoding"],
                    "table type": "horizontal relational",
                    "rows": output_args["table_obj"]["stats"]["row"],
                    "columns": output_args["table_obj"]["stats"]["col"],
                    "cells": output_args["table_obj"]["stats"]["cell"],
                    "r_cells": output_args["table_obj"]["stats"]["r_cell"],
                    "headers": output_args["table_obj"]["headers"],
                    "core_attribute": output_args["table_obj"]["core_attribute"],
                },
                "semantic": {
                    "cea": [
                        {
                            "target": (r, c),
                            "annotation": m_f.m_items().get_entity_info(a),
                        }
                        for _, r, c, a in output_args["table_obj"]["res_cea"]
                    ],
                    "cta": [
                        {
                            "target": c,
                            "annotation": [
                                m_f.m_items().get_entity_info(_a) for _a in a
                            ],
                        }
                        for _, c, a in output_args["table_obj"]["res_cta"]
                    ],
                    "cpa": [
                        {
                            "target": (c1, c2),
                            "annotation": [
                                m_f.m_items().get_entity_info(_a) for _a in a
                            ],
                        }
                        for _, c1, c2, a in output_args["table_obj"]["res_cpa"]
                    ],
                },
            }
            responds.append(table_obj)
            return log_res

        args = []
        for dir_table in dir_tables:
            table_id = os.path.splitext(os.path.basename(dir_table))[0]
            args_obj = {
                "source_type": source_type,
                "source": dir_table,
                "predict_target": predict_target,
                # "table_id": table_id,
                "tar_cea": tar_cea.get(table_id) if tar_cea else None,
                "tar_cta": tar_cta.get(table_id) if tar_cta else None,
                "tar_cpa": tar_cpa.get(table_id) if tar_cpa else None,
                "limit_cea": limit_cea,
            }
            args.append(args_obj)

        if n_cpu == 1 or len(args) == 1:
            for input_args in args:
                output_args = pool_table_annotation(input_args)
                log_res += update_p_bar()
        else:
            with closing(Pool(processes=n_cpu)) as p:
                for output_args in p.imap_unordered(pool_table_annotation, args):
                    log_res += update_p_bar()

    return responds, log_res


def template_encode_results(
    source_type,
    dir_tables,
    dir_cea=None,
    dir_cta=None,
    dir_cpa=None,
    n_cpu=8,
    is_screen=False,
    limit=cf.LIMIT_CEA_TAR,
    predict_target=False,
):
    _responds, log_res = annotate_api(
        source_type,
        dir_tables,
        dir_cea=dir_cea,
        dir_cta=dir_cta,
        dir_cpa=dir_cpa,
        n_cpu=n_cpu,
        is_screen=is_screen,
        limit=limit,
        predict_target=predict_target,
    )
    responds = []
    for _respond in _responds:
        if _respond["status"] == "Error":
            responds.append(
                {"status": _respond["status"], "source": _respond["source"]}
            )
            continue
        respond = {
            "status": _respond["status"],
            "name": _respond["name"],
            # "table_id": _respond["table_id"],
            "run_time": _respond["run_time"],
            "structure": _respond["structure"],
        }
        cta_dict = {
            _obj["target"]: [
                {"label": _tmp_obj["label"], "url": _tmp_obj}
                for _tmp_obj in _obj["annotation"]
            ]
            for _obj in _respond["semantic"]["cta"]
        }
        respond["cta"] = [
            cta_dict.get(i) for i in range(respond["structure"]["columns"])
        ]
        cpa_dict = {
            (
                _obj["target"][1]
                if _obj["target"][0] == respond["structure"]["core_attribute"]
                else _obj["target"][0]
            ): [
                {
                    "label": _tmp_obj["label"],
                    "url": _tmp_obj,
                    "cell1": _obj["target"][0],
                }
                for _tmp_obj in _obj["annotation"]
            ]
            for _obj in _respond["semantic"]["cpa"]
        }
        respond["cpa"] = [
            cpa_dict.get(i) for i in range(respond["structure"]["columns"])
        ]
        cea_dict = {
            _obj["target"]: {
                "label": _obj["annotation"]["label"],
                "url": _obj["annotation"],
            }
            for _obj in _respond["semantic"]["cea"]
        }
        respond["cea"] = [
            [
                {
                    "value": c_obj,
                    "label": cea_dict.get((r, c), dict()).get("label"),
                    "url": cea_dict.get((r, c), dict()).get("url"),
                }
                for c, c_obj in enumerate(r_obj)
            ]
            for r, r_obj in enumerate(_respond["table_cells"])
        ]
        responds.append(respond)
    return responds, log_res


def test():
    tables = [
        [
            cf.SourceType.TEXT,
            """Table Title
        No	col0	col1	col2	-	col4	col5	-
        No	col0	col1	col2	col3	col4	col5	col6
        1	Newport	31	8	95	2	-	-
        2	Thomas	30	5	98	2	-	-
        3	Haynes	25	8	68	2	-	-
        4	Lampitt	29.4	10	73	3	-	-
        5	Solanki	19	4	76	1	-	-
        6	Weston	1	0	1	0	-	-""",
        ],
        [
            cf.SourceType.TEXT,
            """Table Title
        1	col0	col1	col2	col3	col4	col5	col6
        1	col0	col1	col2	col3	col4	col5	col6
        1	Newport	31	8	95	2	-	-
        2	Thomas	30	5	98	2	-	-
        3	Haynes	25	8	68	2	-	-
        4	Lampitt	29.4	10	73	3	-	-
        5	Solanki	19	4	76	1	-	-
        6	Weston	1	0	1	0	-	-""",
        ],
        [
            cf.SourceType.TEXT,
            """1	Newport	31	8	95	2	-	-
        2	Thomas	30	5	98	2	-	-
        3	Haynes	25	8	68	2	-	-
        4	Lampitt	29.4	10	73	3	-	-
        5	Solanki	19	4	76	1	-	-
        6	Weston	1	0	1	0	-	-""",
        ],
        [
            cf.SourceType.TEXT,
            """Newport	31	8	95	2	-	-
        Thomas	30	5	98	2	-	-
        Haynes	25	8	68	2	-	-
        Lampitt	29.4	10	73	3	-	-
        Solanki	19	4	76	1	-	-
        Weston	1	0	1	0	-	-""",
        ],
        [
            cf.SourceType.TEXT,
            """col0	col1	col2	col3	col4	col5	col6
        Newport	31	8	95	2	-	-
        Thomas	30	5	98	2	-	-
        Haynes	25	8	68	2	-	-
        Lampitt	29.4	10	73	3	-	-
        Solanki	19	4	76	1	-	-
        Weston	1	0	1	0	-	-""",
        ],
        [
            cf.SourceType.TEXT,
            '''"col0","col1","col2","col3","col4","col5","col6"
        "Newport","31","8","95","2","-","-"
        "Thomas","30","5","98","2","-","-"
        "Haynes","25","8","68","2","-","-"
        "Lampitt","29.4","10","73","3","-","-"
        "Solanki","19","4","76","1","-","-"
        "Weston","1","0","1","0","-","-"''',
        ],
        [
            cf.SourceType.TEXT,
            """col0 col1 col2 col3 col4 col5 col6
        Newport 31 8 95 2 - -
        Thomas 30 5 98 2 - -
        Haynes 25 8 68 2 - -
        Lampitt 29.4 10 73 3 - -
        Solanki 19 4 76 1 - -
        Weston 1 0 1 0 - -""",
        ],
        [
            cf.SourceType.TEXT,
            """x,MTabES,BM25,Fuzzy,Wikipedia,Wikidata,BM25Fuzzy
        1,88.80,76.87,89.01,56.9,78.52,78.32
        3,95.30,88.35,95.52,61.57,88.52,87.06
        10,98.21,93.48,98.44,63.57,91.18,93.32
        20,99.03,95.01,99.16,64.23,91.98,95.5
        100,99.75,96.47,99.68,65.21,92.85,97.67
        1000,99.89,97.03,99.77,65.21,92.85,98.48""",
        ],
        [
            cf.SourceType.TEXT,
            '''"col0","col1","col2","col3","col4","col5","col6"
        "Newport","31","8","95","2","-","-"
        "Thomas","30","5","98","2","-","-"
        "Haynes","25","8","68","2","-","-"
        "Lampitt","29.4","10","73","3","-","-"
        "Solanki","19","4","76","1","-","-"
        "Weston","1","0","1","0","-","-"''',
        ],
        [
            cf.SourceType.TEXT,
            """| x    | MTabES | BM25  | Fuzzy | Wikipedia | Wikidata | BM25Fuzzy |
        |------|--------|-------|-------|-----------|----------|-----------|
        | 1    | 81.09  | 38.02 | 81.34 | 55        | 59.86    | 58.73     |
        | 3    | 89.81  | 71.80 | 89.92 | 72.59     | 71.45    | 71.02     |
        | 10   | 95.25  | 80.88 | 95.35 | 77.09     | 75.03    | 82.27     |
        | 20   | 96.49  | 82.41 | 96.48 | 79.94     | 75.62    | 86.29     |
        | 100  | 98.26  | 87.75 | 97.98 | 82.33     | 76.10    | 92.47     |
        | 1000 | 98.85  | 89.90 | 98.10 | 82.33     | 76.10    | 94.31     |""",
        ],
        [cf.SourceType.FILE, f"{cf.DIR_ROOT}/data/tables/0AJSJYAL.csv"],  # 2T table
        [cf.SourceType.FILE, f"{cf.DIR_ROOT}/data/tables/0AJSJYAL.xml"],  # 2T table
        [cf.SourceType.FILE, f"{cf.DIR_ROOT}/data/tables/0AJSJYAL.xls"],  # 2T table
        [cf.SourceType.FILE, f"{cf.DIR_ROOT}/data/tables/0AJSJYAL.xltx"],  # 2T table
        [
            cf.SourceType.FILE,
            f"{cf.DIR_ROOT}/data/tables/tk9003_1.csv",
        ],  # shift jis encoding multiple tables
        [cf.SourceType.FILE, f"{cf.DIR_ROOT}/data/tables/DYOGP7OK.csv"],
        [
            cf.SourceType.FILE,
            f"{cf.DIR_ROOT}/data/semtab2021/hardtables/tables/0GL2QTRA.csv",
        ],
    ]
    m_f.init()
    for source_type, source in tables:
        table_obj, run_time = run(source_type, source)
        # -1: print results
        # -1.1. table type, and source
        if table_obj["validation"]:
            if source_type != cf.SourceType.TEXT:
                print(f"\nType: {source_type} - {source}")
            else:
                print(f"\nType: {source_type}")
            # print("Name: %s" % table_obj["name"])
            print("Headers: %s" % ", ".join([str(i) for i in table_obj["headers"]]))
            print("Core Attribute: %d" % table_obj["core_attribute"])
            # print("Table Size: [%dx%d]" % (table_obj["stats"]["row"], table_obj["stats"]["col"]))
            print(
                "Table Values: %d - %.2f"
                % (table_obj["stats"]["cell"], table_obj["stats"]["r_cell"] * 100)
                + f"%"
            )
            print(table_obj["log"])
        else:
            print("Can not read")


if __name__ == "__main__":
    test()
