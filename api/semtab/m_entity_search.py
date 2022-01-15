import os
from contextlib import closing
from multiprocessing import Pool

from api import m_f
from api.annotator import m_semantic, m_input
import m_config as cf
from api.lookup import m_entity_search
from api.utilities import m_io as iw
from collections import defaultdict
from time import time
from tqdm import tqdm


def pool_entity_search(args):
    table_id = args["table_id"]
    output_args = {
        "table": table_id,
        "n_gt": 0 if not args["gt_cea"] else args["gt_cea"].n,
        "pre_lk": {method: defaultdict() for method in args["methods"]},
        "err_lk": defaultdict(),
    }
    hits = args["hits"]

    for method in args["methods"]:
        output_args[method] = {"corrects": [0 for _ in hits], "responds": 0, "time": 0}

    cells = [line for line in iw.load_object_csv(args["table_dir"])]

    def get_hits(res, ans):
        correct = [0 for _ in hits]
        for i, r in enumerate(res):
            if r in ans:
                for hit_i, hit in enumerate(hits):
                    if i + 1 <= hit:
                        correct[hit_i] += 1
                break
        return correct

    # GT Error
    # https://github.com/sem-tab-challenge/aicrowd-evaluator/blob/master/Evaluator_2020_2T/CEA_Evaluator.py#L38

    # pre_lk = {method: defaultdict() for method in args["methods"]}

    for r_i, c_i in args[
        "gt_cea"
    ].tars():  # Avoid inconsistencies between gt and targets
        cell_value = cells[r_i][c_i]
        gt_value = args["gt_cea"].is_tar(r_i, c_i)

        def call_search_module(responds_type, mode):
            start = time()
            # if m_f.pre_lk()[mode].get(cell_value) is None:
            #     responds, _, _ = m_entity_search.search(cell_value, lang=args["lang"], mode=mode, expensive=args["expensive"])
            #     output_args["pre_lk"][mode][cell_value] = {"responds": responds, "run_time": time() - start}
            #
            #     responds_type["time"] += output_args["pre_lk"][mode][cell_value]["run_time"]
            #     responds = output_args["pre_lk"][mode][cell_value]["responds"]
            # else:
            #     responds_type["time"] += m_f.pre_lk()[mode][cell_value]["run_time"]
            #     responds = m_f.pre_lk()[mode][cell_value]["responds"]

            responds, _, _ = m_entity_search.search(
                cell_value, lang=args["lang"], mode=mode, expensive=args["expensive"]
            )
            responds_type["time"] += time() - start

            if responds:
                responds_type["responds"] += 1

            res_hits = get_hits([r for r, _ in responds], gt_value)

            if sum(res_hits) < 2 and mode == "a" and responds:
                gt_labels = ", ".join(
                    [f"{gt_v}[{m_f.m_items().get_label(gt_v)}]" for gt_v in gt_value]
                )

                if not m_f.err_lk().get(cell_value):
                    output_args["err_lk"][cell_value] = {"gt": set(), "c": 0}

                output_args["err_lk"][cell_value]["gt"].update(gt_value)
                output_args["err_lk"][cell_value]["c"] += 1
                n_error = output_args["err_lk"][cell_value]["c"]
                iw.print_status(
                    f"\nERR: {n_error} - {cell_value} - {gt_labels} - {table_id}[{r_i}][{c_i}]"
                )

            responds_type["corrects"] = [
                responds_type["corrects"][i] + v for i, v in enumerate(res_hits)
            ]

        for method in args["methods"]:
            call_search_module(output_args[method], mode=method)

    return output_args


def eval_semtab_2020_entity_search(
    dataset="R4", n_cpu=1, lang="en", limit=0, expensive=True
):
    dir_tables = f"{cf.DIR_SEMTAB_2}/{dataset}/tables"
    iw.print_status(dir_tables)
    dir_csvs = iw.get_files_from_dir(
        dir_tables, extension="csv", is_sort=True, reverse=False
    )
    iw.print_status("Total: %d tables" % (len(dir_csvs)))

    dir_tar_cea = f"{cf.DIR_SEMTAB_2}/{dataset}/CEA_Targets.csv"
    tar_cea, n_cea = m_input.parse_target_cea(dir_tar_cea)

    dir_gt_cea = f"{cf.DIR_SEMTAB_2}/{dataset}/CEA_GT.csv"
    gt_cea, n_cea = m_input.parse_target_cea(dir_gt_cea)

    methods = ["a", "f"]  # "aggregation", "bm25", "fuzzy" "a", "b",
    hits = [1, 3, 10, 20, 100, 1000]

    # tmp = ["ND6V1CBE", "HBKGJQEY", "OUL0NHS5", "Z4M8AT89", "4EGV2QNO", "XZRRL5AE", "BN8FOG7F", "4FW66XNV", "0AJSJYAL", "B7ILPH0Y", "JMNRCW4E", "XBHX2VRT", "7VVP9YIF", "OKW6UUW5", "7GOG4IKF", "P8B3IAOY", "I847EKDH", "4WMC1B70", "B38A9Q5R", "NV4GY44T", "4FFI99VQ", "3JXMPC7N", "3X6QY8Q0", "P8JQV93S", "U30NUSS7", "A5TITKIN", "ET9REW9Y", "N9SJK368", "HQRATPBV", "YCOUS57M", "F98SUVJH", "0H1C2CNE", "BDH3WFGJ", "MZ0BI8NN", "KOQM4YU9", "VKWTT7F7", "51MYHYDF", "OP0PZIXY", "SHV3ZSSV", "UN0JPGPF", "5OLOTSKC", "WRGS0WCX", "M56G7E6D", "GNDO9OXJ", "S7DQFOD4", "1C7N45JA", "SRVLBA90", "UURPYBGQ", "SFGT3EDA", "7EVLH0DV", "WH6JINCM", "CQH26T15", "X3RACWMT", "VGUZX5R3", "QOL4ZIHL", "E3ZK744Q", "RYTFLT5K", "JA4L7KWX", "71SY0Z5S", "PWNRGOJ5", "L2MCKOWX", "K2VEUQT0", "MM6ZALRS", "FVKKTA8O", "T7RPWH6N", "JZLRN9PL", "VLUAWPBO", "MLRJYJ5K", "9JA43FJL", "N6QAC84T", "QOAVEFGY", "SPPNJXB2", "NDSTZH1I", "GINQPZQC", "5DKX42VB", "EV6LDIB8", "BID0NRU0", "HB00DX4L", "J9EJV2S3", "5RV7WRO1"]

    # tmp = tmp[:70]
    #
    args = []
    if limit == 0:
        limit = len(dir_csvs)
    n_step = len(dir_csvs) // 20
    for dir_csv in dir_csvs[:limit]:
        table_id = os.path.splitext(os.path.basename(dir_csv))[0]
        # if table_id not in tmp:
        #     continue
        if not gt_cea.get(table_id):
            continue
        args_obj = {
            "table_dir": dir_csv,
            "table_id": table_id,
            "tar_cea": tar_cea.get(table_id),
            "gt_cea": gt_cea.get(table_id),
            "hits": hits,
            "lang": lang,
            "expensive": expensive,
            "methods": methods,
        }
        args.append(args_obj)

    p_bar = tqdm(total=len(dir_csvs))

    return_obj = {"n_gt": n_cea, "n_table": len(dir_csvs)}

    for method in methods:
        return_obj[method] = {"corrects": [0 for _ in hits], "responds": 0, "time": 0}
    tmp_n = 0
    n_tab = 0
    tmp_acc_1 = [0, 0, 0]
    tmp_acc_max = [0, 0, 0]
    run_time = [0, 0, 0]
    n_queries = 0

    def update_p_bar(tmp_n, n_queries, n_tab):
        p_bar.update()
        for _method in methods:
            return_obj[_method]["time"] += output_args[_method]["time"]
            return_obj[_method]["responds"] += output_args[_method]["responds"]
            return_obj[_method]["corrects"] = [
                return_obj[_method]["corrects"][i] + v
                for i, v in enumerate(output_args[_method]["corrects"])
            ]
        if output_args.get("a"):
            tmp_acc_1[0] += output_args["a"]["corrects"][0]
            tmp_acc_max[0] += output_args["a"]["corrects"][-1]
            run_time[0] += output_args["a"]["time"]
        if output_args.get("b"):
            tmp_acc_1[1] += output_args["b"]["corrects"][0]
            tmp_acc_max[1] += output_args["b"]["corrects"][-1]
            run_time[1] += output_args["b"]["time"]
        if output_args.get("f"):
            tmp_acc_1[2] += output_args["f"]["corrects"][0]
            tmp_acc_max[2] += output_args["f"]["corrects"][-1]
            run_time[2] += output_args["f"]["time"]
        n_queries += output_args["n_gt"]

        tmp_n += output_args["n_gt"]
        n_tab += 1
        p_bar.set_description(
            f"@1|"
            f"{tmp_acc_1[0] * 100./ tmp_n if tmp_n else 0:.2f}|"
            f"{tmp_acc_1[1] * 100./ tmp_n if tmp_n else 0:.2f}|"
            f"{tmp_acc_1[2] * 100./ tmp_n if tmp_n else 0:.2f}"
            f"| @{hits[-1]}|"
            f"{tmp_acc_max[0] * 100. / tmp_n if tmp_n else 0:.2f}|"
            f"{tmp_acc_max[1] * 100. / tmp_n if tmp_n else 0:.2f}|"
            f"{tmp_acc_max[2] * 100. / tmp_n if tmp_n else 0:.2f}"
            f"| time|"
            f"{n_queries / run_time[0] if run_time[0] else 0:.2f}|"
            f"{n_queries / run_time[1] if run_time[1] else 0:.2f}|"
            f"{n_queries / run_time[2] if run_time[2] else 0:.2f}"
        )
        # Update dict
        # prelk
        # for m, m_obj in output_args["pre_lk"].items():
        #     for k, v in m_obj.items():
        #         if not m_f.pre_lk()[m].get(k) and v["run_time"] > 3:
        #             m_f.pre_lk()[m][k] = v
        # m_f.pre_lk()[m].update(m_obj)

        for _cell_value, m_obj in output_args["err_lk"].items():
            if not m_f.err_lk().get(_cell_value):
                m_f.err_lk()[_cell_value] = m_obj
            else:
                # m_f.err_lk()[_cell_value]["gt"].update(m_obj["gt"])
                m_f.err_lk()[_cell_value]["c"] += m_obj["c"]

        # remove prelk

        if n_tab and n_tab % n_step == 0:
            iw.print_status(
                f"\nM\t   @\tF1  \tP   \tR   \tAcc \tCorrect \tAnswers \tGT"
            )
            for method in methods:
                n_responds = return_obj[method]["responds"]
                for hit_i, hit in enumerate(hits):
                    n_corrects = return_obj[method]["corrects"][hit_i]
                    precision = 0
                    if n_responds:
                        precision = n_corrects * 100.0 / n_responds

                    recall = n_corrects * 100.0 / n_cea
                    f1 = (
                        (2.0 * precision * recall) / (precision + recall)
                        if (precision + recall) > 0
                        else 0.0
                    )
                    acc = n_corrects * 100.0 / n_cea

                    iw.print_status(
                        f"{method}\t{hit:4d}"
                        f"\t{f1:4.2f}\t{precision:4.2f}\t{recall:4.2f}"
                        f"\t{acc:4.2f}\t{n_corrects:7d}\t{n_responds:7d}\t{n_cea:7d}"
                    )

        return tmp_n, n_queries, n_tab

    if n_cpu == 1:
        for input_args in args:
            output_args = pool_entity_search(input_args)
            tmp_n, n_queries, n_tab = update_p_bar(tmp_n, n_queries, n_tab)
    else:
        with closing(Pool(processes=n_cpu)) as p:
            for output_args in p.imap_unordered(pool_entity_search, args):
                tmp_n, n_queries, n_tab = update_p_bar(tmp_n, n_queries, n_tab)

    iw.print_status(f"\nM\t   @\tF1  \tP   \tR   \tAcc \tCorrect \tAnswers \tGT")
    for method in methods:
        n_responds = return_obj[method]["responds"]
        for hit_i, hit in enumerate(hits):
            n_corrects = return_obj[method]["corrects"][hit_i]
            precision = 0
            if n_responds:
                precision = n_corrects * 100.0 / n_responds

            recall = n_corrects * 100.0 / n_cea
            f1 = (
                (2.0 * precision * recall) / (precision + recall)
                if (precision + recall) > 0
                else 0.0
            )
            acc = n_corrects * 100.0 / n_cea

            iw.print_status(
                f"{method}\t{hit:4d}\t"
                f"{f1:4.2f}\t{precision:4.2f}\t{recall:4.2f}\t{acc:4.2f}\t{n_corrects:7d}\t{n_responds:7d}\t{n_cea:7d}"
            )
    iw.print_status("\nErrors")
    errors = sorted(m_f.err_lk().items(), key=lambda x: x[1]["c"], reverse=True)
    for cell_value, gt in errors:
        gt_labels = ", ".join(
            [f"{gt_v}[{m_f.m_items().get_label(gt_v)}]" for gt_v in gt["gt"]]
        )
        n_err = gt["c"]
        iw.print_status(f"ERR: {n_err} - {cell_value} - {gt_labels}")
    iw.print_status(f"Dataset: {dataset}")


def test_gt_2t():
    import pandas as pd

    gt_cell_ent = dict()
    gt_cell_ent2 = dict()
    # The SemTab2020 CEA submission format is tab_id, row_id, col_id, entity
    gt = pd.read_csv(
        f"{cf.DIR_SEMTAB_2}/2T/CEA_GT.csv",
        delimiter=",",
        names=["tab_id", "row_id", "col_id", "entity"],
        dtype={"tab_id": str, "row_id": str, "col_id": str, "entity": str},
        keep_default_na=False,
    )
    gt2 = gt[~(gt["tab_id"].isin(["24W5SSRB", "3LG8J4MX"]) & (gt["col_id"] == "2"))]

    for index, row in gt.iterrows():
        cell = (row["tab_id"], row["row_id"], row["col_id"])
        gt_cell_ent[cell] = row["entity"].lower().split()

    for index, row in gt2.iterrows():
        cell = (row["tab_id"], row["row_id"], row["col_id"])
        gt_cell_ent2[cell] = row["entity"].lower().split()


if __name__ == "__main__":
    # iw.prepare_input_tables(cf.DIR_SAMPLE_ZIP)
    m_f.init()
    # test_gt_2t()
    eval_semtab_2020_entity_search(dataset="2T", n_cpu=1)
