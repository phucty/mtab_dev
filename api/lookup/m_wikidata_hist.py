import sys

import requests
from utilities import io_worker as iw
import re
import setting as st
from collections import defaultdict, Counter
from multiprocessing.pool import Pool
from contextlib import closing
from tqdm import *
import f
import utils as ul


class Wikidata_Hist(object):
    def __init__(self):
        self.URL = "https://www.wikidata.org/w/api.php"
        self.HEADER = {"Accept": "application/json"}
        self.dir_file = st.DIR_WIKIDATA_HIST
        self._data = None
        self.load_data()

    def load_data(self):
        try:
            self._data = iw.load_obj_pkl(self.dir_file, is_message=True)
        except Exception as message:
            iw.print_status(message)
            self._data = defaultdict()

    def run_analysis(self):
        count = 0
        count_1 = 0
        count_2 = 0
        for key, values in self._data.items():
            count += 1
            if len(values):
                count_1 += 1
                for value in values:
                    if value[1] and value[1] == "P31":
                        count_2 += 1
                        break
        print("Change: %d/%d = %.2f" % (count_1, count, count_1 * 100.0 / count))
        print("P31: %d/%d = %.2f" % (count_2, count, count_2 * 100.0 / count))

    def get_previous_label(self, wd_id, change_obj):
        timestamp = change_obj[0]
        new_label = change_obj[1]
        args = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": wd_id,
            "rvlimit": "500",
            "rvprop": "timestamp|comment",
            "rvdir": "older",
            "rvstart": timestamp,
        }
        label = None
        try:
            responds = requests.get(self.URL, params=args).json(encoding="utf8")
            for respond in responds["query"]["pages"].values():
                if respond.get("revisions"):
                    for revision in respond["revisions"]:
                        for add_pattern in [
                            "wbsetlabel-set:1|en",
                            "wbsetlabel-add:1|en",
                        ]:
                            if add_pattern in revision["comment"]:
                                label = re.sub(
                                    r"/\*(.*)\*/", "", revision["comment"]
                                ).strip()
                                # revision["comment"].replace(add_pattern, "").strip()
                                if len(label) and label != new_label:
                                    return label
        except Exception as e:
            iw.print_status("%s. %s" % (e, wd_id))
        return label

    def get_previous_props(self, wd_id, prop_list):
        args = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": wd_id,
            "rvlimit": "500",
            "rvprop": "timestamp|comment",
            "rvdir": "older",
            "rvstart": "2020-03-01T00:00:00Z",
        }
        props_results = defaultdict()
        try:
            responds = requests.get(self.URL, params=args).json(encoding="utf8")
            for respond in responds["query"]["pages"].values():
                if respond.get("revisions"):
                    for revision in respond["revisions"]:
                        prop_id = re.search(
                            "\[\[Property:(.+?)\]\]", revision["comment"]
                        )
                        if prop_id:
                            prop_id = prop_id.group(1)
                            v_id = re.search("\[\[Q(.+?)\]\]", revision["comment"])
                            if v_id:
                                v_id = "Q" + v_id.group(1)
                            else:
                                v_id = revision["comment"].split("]]:")
                                if len(v_id):
                                    v_id = v_id[-1]
                                else:
                                    v_id = ""
                            if (
                                len(prop_id)
                                and len(v_id)
                                and prop_id in prop_list
                                and not props_results.get(prop_id)
                            ):
                                props_results[prop_id] = v_id
        except Exception as e:
            iw.print_status("%s. %s" % (e, wd_id))
        return props_results

    def get_previous_update(self, wd_id, org_prop_id, timestamp):
        args = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": wd_id,
            "rvlimit": "100",
            "rvprop": "timestamp|comment",
            "rvdir": "older",
            "rvstart": timestamp,
        }
        res_v_id = None
        try:
            responds = requests.get(self.URL, params=args).json(encoding="utf8")
            for respond in responds["query"]["pages"].values():
                if respond.get("revisions"):
                    for revision_i, revision in enumerate(respond["revisions"]):
                        if revision_i == 0:
                            continue
                        prop_id = re.search(
                            "\[\[Property:(.+?)\]\]", revision["comment"]
                        )
                        if prop_id:
                            prop_id = prop_id.group(1)
                            v_id = re.search("\[\[Q(.+?)\]\]", revision["comment"])
                            if v_id:
                                v_id = "Q" + v_id.group(1)
                            else:
                                v_id = revision["comment"].split("]]:")
                                if len(v_id):
                                    v_id = v_id[-1]
                                else:
                                    v_id = ""
                            if len(prop_id) and len(v_id) and prop_id == org_prop_id:
                                res_v_id = v_id
                                break

        except Exception as e:
            iw.print_status("%s. %s" % (e, wd_id))
        return res_v_id

    def run_service(self, wd_id):
        action_list = []  # 1 is add, -1 is remove, property_id, value
        if not len(wd_id):
            return action_list
        headers = {
            "user-agent": "%s.%s 13 Aug" % (sys.version_info[0], sys.version_info[1])
        }
        args = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": wd_id,
            "rvlimit": "500",
            "rvprop": "timestamp|comment",
            "rvdir": "older",
            "rvend": "2020-03-05T00:00:00Z",
            "rvstart": "2020-06-30T00:00:00Z",
        }
        responds = requests.get(self.URL, params=args).json(encoding="utf8")
        change_label = []
        # update_claim = []
        for respond in responds["query"]["pages"].values():
            if respond.get("revisions"):
                for revision in respond["revisions"]:
                    try:
                        if "wbsetlabel-set:1|en" in revision["comment"]:
                            label = (
                                revision["comment"]
                                .replace("/* wbsetlabel-set:1|en */", "")
                                .strip()
                            )
                            change_label.append([revision["timestamp"], label])
                        else:
                            if "wbeditentity-update" in revision["comment"]:
                                action = 5  # Property
                                pattern_search = re.findall(
                                    "\[\[Property:(.+?)\]\]", revision["comment"]
                                )
                                if len(pattern_search) == 2:
                                    action_list.append(
                                        [action, pattern_search[0], pattern_search[1]]
                                    )
                                else:
                                    action = 4  # Entitty
                                    pattern_search = re.findall(
                                        "\[\[Q(.+?)\]\]", revision["comment"]
                                    )
                                    if len(pattern_search) == 2:
                                        action_list.append(
                                            [
                                                action,
                                                "Q" + pattern_search[0],
                                                "Q" + pattern_search[1],
                                            ]
                                        )
                                    else:
                                        pattern_search = re.findall(
                                            "\[\[(.+?)\]\]", revision["comment"]
                                        )
                                        if len(pattern_search) == 2:
                                            from_wd = pattern_search[0].replace(
                                                "Property:", ""
                                            )
                                            to_wd = pattern_search[1].replace(
                                                "Property:", ""
                                            )
                                            if ul.is_wd_item(from_wd) and ul.is_wd_item(
                                                to_wd
                                            ):
                                                action_list.append(
                                                    [action, from_wd, to_wd]
                                                )
                                        # elif len(pattern_search) == 1:
                                        #     from_wd = pattern_search[0].replace("Property:", "")
                                        #     if ul.is_Wikidata_item(from_wd):
                                        #         to_wd = revision["comment"].split("%s]]" % from_wd)[1]
                                        #         action_list.append([action, from_wd, to_wd])
                            else:
                                if (
                                    "wbremoveclaims-remove" in revision["comment"]
                                    or "wbsetclaimvalue" in revision["comment"]
                                ):
                                    action = 1
                                elif (
                                    "wbsetclaim-create" in revision["comment"]
                                    or "wbcreateclaim-create" in revision["comment"]
                                ):
                                    action = -1
                                elif "wbsetclaim-update" in revision["comment"]:
                                    action = 3
                                else:
                                    action = 0

                                prop_id = re.search(
                                    "\[\[Property:(.+?)\]\]", revision["comment"]
                                )
                                if prop_id:
                                    prop_id = prop_id.group(1)
                                    v_id = re.search(
                                        "\[\[Q(.+?)\]\]", revision["comment"]
                                    )
                                    if v_id:
                                        v_id = "Q" + v_id.group(1)
                                    else:
                                        v_id = revision["comment"].split("]]:")
                                        if len(v_id):
                                            v_id = v_id[-1]
                                        else:
                                            v_id = ""

                                    if action == 3:
                                        v_id_update = self.get_previous_update(
                                            wd_id, prop_id, revision["timestamp"]
                                        )
                                        if v_id_update:
                                            action_list.append([-1, prop_id, v_id])
                                            action_list.append(
                                                [1, prop_id, v_id_update]
                                            )
                                        else:
                                            action_list.append([action, prop_id, v_id])
                                        # update_claim.append(prop_id)
                                    else:
                                        action_list.append([action, prop_id, v_id])
                    except Exception as e:
                        iw.print_status("%s. %s" % (e, wd_id))
        # Find previous label
        if len(change_label):
            old_label = self.get_previous_label(wd_id, change_label[-1])
            action_list.append([2, change_label[-1][1], old_label])

        # if len(update_claim):
        #     prop_results = self.get_previous_props(wd_id, update_claim)
        #     for prop_id, v_id in prop_results.items():
        #         action_list.append([1, prop_id, v_id])

        return action_list

    def update(self, query_text, responds):
        self._data[query_text] = responds

    def get(self, wd_id):
        return self._data.get(wd_id)

    def save(self):
        iw.save_obj_pkl(self.dir_file, self._data)

    def items(self):
        for key, value in self._data.items():
            yield key, value

    def size(self):
        return len(self._data)

    def search(self, wd_id):
        responds = self.get(wd_id)
        if responds is None:
            responds = self.run_service(wd_id)
            self._data[wd_id] = responds
        return responds


def pool_get_wikidata_hist(wd_id):
    responds = []
    try:
        responds = f.wikidata_hist().run_service(wd_id)
    except Exception as message:
        iw.print_status(message)
    return wd_id, responds


def get_wikidata_hist(n_cpu):
    cea_res = iw.load_object_csv(st.DIR_RESULTS + "cea.csv")
    # res_file = iw.load_obj_pkl(st.DIR_WIKIDATA_HIST, is_message=True)
    # lk_queries = set(res_file.keys())
    lk_queries = set()
    for table_id, row_i, col_i, wd_id in cea_res:
        # if wd_id.replace(st.WD, "") == "Q52336979": # col_i == "0" and  and
        # if col_i == "0":
        lk_queries.add(wd_id.replace(st.WD, ""))
    #
    # for wd_id in iw.load_obj_pkl(st.DIR_LK_QUERIES_GT_WD):
    #     lk_queries.add(wd_id.replace(st.WD, ""))
    #
    # two_cells = iw.load_obj_pkl(st.DIR_LK_RES_FZ_2CELLS_CELL2)
    # for responds in two_cells.values():
    #     for respond in responds:
    #         lk_queries.add(respond["core"])
    #         lk_queries.update(respond["other"])
    #
    # two_cells = iw.load_obj_pkl(st.DIR_LK_RES_FZ_2CELLS_2CELLS)
    # for responds in two_cells.values():
    #     for respond in responds:
    #         lk_queries.add(respond["core"])
    #         lk_queries.update(respond["other"])
    #
    lk_queries = [
        lk_query for lk_query in lk_queries if f.wikidata_hist().get(lk_query) is None
    ]
    # lk_queries = [lk_query for lk_query, lk_res in f.wikidata_hist().items() if len(lk_res)]
    lk_queries = sorted(list(lk_queries), reverse=False)

    iw.print_status(f.wikidata_hist().size())
    count = 0
    count_p31 = 0
    with tqdm(total=len(lk_queries)) as p_bar:

        def update_bar(count, count_p31):
            p_bar.update()
            f.wikidata_hist().update(key, respond)
            if len(respond):
                # f.wikidata_hist().update(key, respond)
                count += 1
                if sum([1 for _, p, _ in respond if p == "P31"]) > 0:
                    count_p31 += 1
                p_bar.set_description("Success: %d - P31:%d" % (count, count_p31))
                if count % 10000 == 0:
                    f.wikidata_hist().save()
                    iw.print_status(f.wikidata_hist().size())
            return count, count_p31

        if n_cpu == 1:
            for lk_query in lk_queries:
                key, respond = pool_get_wikidata_hist(lk_query)
                count, count_p31 = update_bar(count, count_p31)
        else:
            with closing(Pool(processes=n_cpu)) as p:
                for key, respond in p.imap_unordered(
                    pool_get_wikidata_hist, lk_queries
                ):
                    count, count_p31 = update_bar(count, count_p31)
        f.wikidata_hist().save()


def print_popular():
    cea_res = defaultdict(list)
    cea_res_0 = defaultdict()
    cea = []
    for table_id, row_i, col_i, wd_id in iw.load_object_csv(st.DIR_RESULTS + "cea.csv"):
        row_i = int(row_i)
        col_i = int(col_i)
        if col_i != 0:
            cea_res[wd_id].append((table_id, row_i, col_i))
            cea.append([table_id, row_i, col_i, wd_id])
        else:
            cea_res_0[(table_id, row_i, col_i)] = wd_id

    cea_res = sorted(cea_res.items(), key=lambda x: len(x[1]), reverse=True)
    for wd_id, table_records in cea_res:
        iw.print_status(
            "WD: %d - %s[%s]"
            % (
                len(table_records),
                wd_id,
                f.wikidata_info().get_labels(wd_id, default_return=wd_id),
            ),
            is_screen=False,
        )
        current_cursor = ("", "")
        for table_id, row_i, col_i in table_records:
            if current_cursor != (table_id, row_i):
                current_cursor = (table_id, row_i)
                wd_id_tmp = cea_res_0.get((table_id, row_i, 0), "")
                iw.print_status(
                    "  %s\t%d\t%d\t%s[%s]"
                    % (
                        table_id,
                        row_i,
                        0,
                        wd_id_tmp,
                        f.wikidata_info().get_labels(
                            wd_id_tmp, default_return=wd_id_tmp
                        ),
                    ),
                    is_screen=False,
                )
            # iw.print_status("  %s\t%d\t%d" % (table_id, row_i, col_i), is_screen=False)
    iw.save_object_csv(st.DIR_RESULTS + "cea_test.csv", cea)


if __name__ == "__main__":
    # tmp = Wikidata_Hist()
    # tmp.run_service("Q85872031")
    f.init()
    # print_popular()
    # f.wikidata_hist().run_service('Q23775474')
    # f.wikidata_hist().run_service('Q7928869')
    # f.wikidata_hist().run_service('Q18975243')
    # f.wikidata_hist().run_service('Q52336979')
    # f.wikidata_hist().run_service("Q7892805")
    # f.wikidata_hist().run_analysis()
    get_wikidata_hist(10)
