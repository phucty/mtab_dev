import os
from collections import defaultdict
from contextlib import closing
from datetime import timedelta
from time import time, sleep
from multiprocessing import Pool

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from api import m_setting as st
from api.utilities import m_iw


class MTab(object):
    def __init__(self):
        self.F_MTAB = f"{st.DOMAIN}/api/v1.1/mtab"

        self.session = requests.Session()
        retries = Retry(
            total=st.LIMIT_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def _request(self, func_name, query_args, retries=3, message=""):
        responds = defaultdict()
        if retries == 0:
            print(message)
            return responds
        try:
            # _responds = requests.post(func_name, json=query_args, timeout=self.TIME_OUT)
            _responds = self.session.post(
                func_name, json=query_args, timeout=st.LIMIT_TIME_OUT
            )
            if _responds.status_code == 200:
                responds = _responds.json()
                if not responds or (
                    responds.get("status") == "Error" and not responds.get("message")
                ):
                    sleep(300)
                    return self._request(
                        func_name,
                        query_args,
                        retries - 1,
                        message=f"Error: Retry {retries-1}",
                    )
        except Exception as message:
            if func_name == self.F_MTAB and query_args.get("table_name"):
                args_info = func_name + ": " + query_args.get("table_name")
            else:
                args_info = func_name
            sleep(300)
            return self._request(
                func_name, query_args, retries - 1, message=f"\n{message} - {args_info}"
            )
        return responds

    def get_table_annotation(
        self,
        table_content,
        table_name="",
        predict_target=False,
        tar_cea=None,
        tar_cta=None,
        tar_cpa=None,
        search_mode="a",
    ):
        query_args = {
            "table_name": table_name,
            "table": table_content,
            "predict_target": predict_target,
            "tar_cea": tar_cea,
            "tar_cta": tar_cta,
            "tar_cpa": tar_cpa,
            "search_mode": search_mode,
        }
        responds = self._request(self.F_MTAB, query_args)
        return responds


def pool_table_annotation(args):
    responds = {}
    try:
        mtab_api = MTab()
        responds = mtab_api.get_table_annotation(
            args["table_content"],
            table_name=args.get("table_id"),
            tar_cea=args.get("tar_cea"),
            tar_cta=args.get("tar_cta"),
            tar_cpa=args.get("tar_cpa"),
            search_mode=args.get("search_mode"),
        )
    except Exception as message:
        print(message)
        responds.update({"status": "Error", "message": message})

    # overloading - try 10 times, 5s / a sleep
    if (not responds or responds["status"] == "Error") and args.get("sleep", 0) < 50:
        sleep(5)
        args.update({"sleep": args.get("sleep", 0) + 5})
        print(args.get("sleep", 0))
        return pool_table_annotation(args)

    return responds


def m_call_run_semtab(
    challenge="semtab2021", data_name="hardtables", n_thread=1, search_mode="a", limit=0
):
    start = time()

    dir_folder_tables = st.dir_tables.format(challenge=challenge, data_name=data_name)

    # Load tables
    dir_tables = m_iw.get_files_from_dir(
        dir_folder_tables,
        is_sort=True,
        reverse=True,
    )
    # Test a table 00CU42PU
    # dir_tables = [
    #     dir_table
    #     for dir_table in dir_tables
    #     if "1d09a099d3964602aca9425adcde89cd" in dir_table
    # ]
    # 5F93F9 00CU42PU

    if limit:
        dir_tables = dir_tables[:limit]

    # Matching target
    tar_cea, tar_cta, tar_cpa = defaultdict(list), defaultdict(list), defaultdict(list)

    # Load targets
    dir_tar_cea = st.dir_cea_tar.format(challenge=challenge, data_name=data_name)
    dir_tar_cta = st.dir_cta_tar.format(challenge=challenge, data_name=data_name)
    dir_tar_cpa = st.dir_cpa_tar.format(challenge=challenge, data_name=data_name)
    # Load target cea
    for line in m_iw.load_object_csv(dir_tar_cea):
        table_id, row_i, col_i = line[:3]
        tar_cea[table_id].append([row_i, col_i])

    # Load target cta
    for line in m_iw.load_object_csv(dir_tar_cta):
        table_id, col_i = line[:2]
        tar_cta[table_id].append(col_i)

    # Load target cpa
    for line in m_iw.load_object_csv(dir_tar_cpa):
        table_id, col_i1, col_i2 = line[:3]
        tar_cpa[table_id].append([col_i1, col_i2])

    # Create input args
    args = []
    for dir_table in dir_tables:
        table_id = os.path.splitext(os.path.basename(dir_table))[0]
        table_content = m_iw.load_object_csv(dir_table)
        args_obj = {
            "table_content": table_content,
            "table_id": table_id,
            "tar_cea": tar_cea.get(table_id),
            "tar_cta": tar_cta.get(table_id),
            "tar_cpa": tar_cpa.get(table_id),
            "search_mode": search_mode,
        }
        args.append(args_obj)

    # Call MTab
    res_cea, res_cta, res_cpa = [], [], []
    with tqdm(total=len(dir_tables)) as p_bar:
        # if n_thread == 1:
        #     for arg in args:
        #         output_args = pool_table_annotation(arg)
        with closing(Pool(processes=n_thread)) as p:
            for output_args in p.imap_unordered(pool_table_annotation, args):
                p_bar.update()
                if not output_args or output_args["status"] == "Error":
                    if output_args.get("message"):
                        print(output_args.get("message"))
                    else:
                        print(
                            "Error: Could not get POST input, please retry again. (The server is overloading now)"
                        )
                    continue
                if output_args.get("semantic"):
                    if output_args["semantic"].get("cea"):
                        # Why c, r: Inconsistencies of SemTab 2021
                        res_cea.extend(
                            [output_args["table_name"], r, c, a]
                            for r, c, a in output_args["semantic"]["cea"]
                        )
                    if output_args["semantic"].get("cta"):
                        res_cta.extend(
                            [
                                output_args["table_name"],
                                c,
                                a[0]  # Randomly pick the first item (as SemTab 2021).
                                # " ".join(a),
                            ]
                            for c, a in output_args["semantic"]["cta"]
                        )
                    if output_args["semantic"].get("cpa"):
                        res_cpa.extend(
                            [
                                output_args["table_name"],
                                c1,
                                c2,
                                a[0]  # Randomly pick the first item (as SemTab 2021).
                                # " ".join(a),
                            ]
                            for c1, c2, a in output_args["semantic"]["cpa"]
                        )

    # Why: easy compare the results
    res_cea.sort(key=lambda x: x[0])
    res_cta.sort(key=lambda x: x[0])
    res_cpa.sort(key=lambda x: x[0])

    # Save annotation files
    domain = "online" if st.DOMAIN == st.DOMAIN_ONLINE else "local"

    dir_cea_res = st.dir_cea_res.format(
        challenge=challenge, data_name=data_name, source=domain
    )
    dir_cta_res = st.dir_cta_res.format(
        challenge=challenge, data_name=data_name, source=domain
    )
    dir_cpa_res = st.dir_cpa_res.format(
        challenge=challenge, data_name=data_name, source=domain
    )

    m_iw.save_object_csv(dir_cea_res, res_cea)
    m_iw.save_object_csv(dir_cta_res, res_cta)
    m_iw.save_object_csv(dir_cpa_res, res_cpa)

    print(f"{str(timedelta(seconds=round(time() - start)))}")


if __name__ == "__main__":
    m_call_run_semtab(
        challenge="semtab2021",
        data_name="HardTableR3",  # HardTableR3
        n_thread=1,
        limit=0,
        search_mode="a",
    )
    # m_call_run_semtab(
    #     challenge="semtab2021", data_name="biotables", n_thread=8, limit=0
    # )
