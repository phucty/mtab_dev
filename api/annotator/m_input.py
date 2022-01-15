from collections import defaultdict
from enum import Enum
from api.utilities import m_io as iw
import m_config as cf


class EnumTar(Enum):
    ERR_INDEX = -1
    OK = 0
    DUPLICATE = 1


class TargetObj(object):
    def __init__(self, table_name):
        self._table_name = table_name
        self._target = defaultdict()
        self._n = 0

    @property
    def n(self):
        return self._n


class TargetCEA(TargetObj):
    def __init__(self, table_name):
        super().__init__(table_name)
        self._cols = set()

    def headers(self):
        results = []
        n_rows = self.rows()
        if n_rows:
            n_rows = int(n_rows[0])
            if n_rows:
                results = [i for i in range(n_rows)]
        return results

    def rows(self):
        return sorted(self._target.keys())

    def cols(self):
        return sorted(list(self._cols))

    def add(self, row, col, result=None):
        row = int(row)
        col = int(col)
        if row < 0 or col < 0:
            return EnumTar.ERR_INDEX

        self._cols.add(col)
        if self._target.get(row):
            if self._target[row].get(col):
                return EnumTar.DUPLICATE
        else:
            self._target[row] = defaultdict()

        if result is not None:
            self._target[row][col] = result
        else:
            self._target[row][col] = True
        self._n += 1
        return EnumTar.OK

    def is_tar(self, row, col):
        row = int(row)
        col = int(col)
        if row < 0 or col < 0:
            return None
        row_obj = self._target.get(row)
        if not row_obj:
            return None

        col_obj = row_obj.get(col)
        return col_obj

    def tars(self):
        for row, cols in self._target.items():
            for col in cols.keys():
                yield row, col

    def items(self):
        for row, cols in self._target.items():
            for col, wd_id in cols.items():
                yield row, col, wd_id


class TargetCTA(TargetObj):
    def __init__(self, table_name):
        super().__init__(table_name)

    def core_attribute(self):
        if self._target:
            return min(self._target.keys())

    def cols(self):
        return sorted(self._target.keys())

    def add(self, col, result=None):
        col = int(col)
        if col < 0:
            return EnumTar.ERR_INDEX

        if result is not None:
            self._target[col] = result
        else:
            self._target[col] = True
        self._n += 1
        return EnumTar.OK

    def is_tar(self, col):
        col = int(col)
        if col < 0:
            return None
        col_obj = self._target.get(col)
        return col_obj

    def tars(self):
        for col in self._target:
            yield col


class TargetCPA(TargetObj):
    def __init__(self, table_name):
        super().__init__(table_name)
        self._cols = set()

    def cols(self):
        return sorted(list(self._cols))

    def core_attribute(self):
        if self._target:
            tmp = sorted(self._target.items(), key=lambda x: len(x[1]), reverse=True)
            return tmp[0][0]
            # return list(self._target.keys())[0]
        else:
            return None

    def add(self, col1, col2, result=None):
        col1 = int(col1)
        col2 = int(col2)
        if col1 < 0 or col2 < 0:
            return EnumTar.ERR_INDEX

        self._cols.add(col1)
        self._cols.add(col2)

        if self._target.get(col1):
            if self._target[col1].get(col2):
                return EnumTar.DUPLICATE
        else:
            self._target[col1] = defaultdict()

        if result is not None:
            self._target[col1][col2] = result
        else:
            self._target[col1][col2] = True

        self._n += 1
        return EnumTar.OK

    def is_tar(self, col1, col2):
        col1 = int(col1)
        col2 = int(col2)
        if col1 < 0 or col2 < 0:
            return None
        col1_obj = self._target.get(col1)
        if not col1_obj:
            return None

        col_obj = col1_obj.get(col2)
        return col_obj

    def tars(self):
        for col1, col2s in self._target.items():
            for col2 in col2s:
                yield col1, col2


def parse_target_cea(csv_file):
    targets = defaultdict()
    n = 0
    for line in iw.load_object_csv(csv_file):
        if len(line) >= 3:
            table_id, row_i, col_i = line[:3]
            gt_cea = None
            if len(line) == 4:
                gt_cea = {l.replace(cf.WD, "") for l in line[3].split(" ")}
                # gt_cea = line[3].replace(cf.WD, "")
            if targets.get(table_id) is None:
                targets[table_id] = TargetCEA(table_id)
            targets[table_id].add(row_i, col_i, gt_cea)
            n += 1
    return targets, n


def update_gt_redirect_cea(dataset):
    redirect_of = iw.load_obj_pkl(f"{cf.DIR_MODELS}/redirect_of.pkl")
    wd_redirects = iw.load_obj_pkl(f"{cf.DIR_MODELS}/wd_redirects.pkl")

    csv_obj = []
    csv_file = f"{cf.DIR_SEMTAB_2}/{dataset}/CEA_GT.csv"

    for line in iw.load_object_csv(csv_file):
        if len(line) >= 3:
            table_id, row_i, col_i = line[:3]
            gt_cea = None
            if len(line) == 4:
                gt_cea = {l.replace(cf.WD, "") for l in line[3].split(" ")}
                # gt_cea = line[3].replace(cf.WD, "")
            if gt_cea:
                wd_id = list(gt_cea)[0]
                gt_cea_redirect = wd_redirects.get(wd_id, wd_id)
                gt_cea.add(gt_cea_redirect)
                gt_cea_redirect_of = redirect_of.get(gt_cea_redirect)
                if gt_cea_redirect_of:
                    gt_cea.update(gt_cea_redirect_of)
            csv_obj.append([table_id, row_i, col_i, " ".join(list(gt_cea))])
        else:
            csv_obj.append(line)
    iw.save_object_csv(csv_file, csv_obj)


def parse_target_cta(csv_file):
    targets = defaultdict()
    n = 0
    for line in iw.load_object_csv(csv_file):
        if len(line) >= 2:
            table_id, col_i = line[:2]
            gt_cta = None
            if len(line) == 3:
                # gt_cta = line[2].replace(cf.WD, "")
                gt_cta = {l.replace(cf.WD, "") for l in line[2].split(" ")}
            if targets.get(table_id) is None:
                targets[table_id] = TargetCTA(table_id)
            targets[table_id].add(col_i, gt_cta)
            n += 1
    return targets, n


def parse_target_cpa(csv_file):
    targets = defaultdict()
    n = 0
    for line in iw.load_object_csv(csv_file):
        if len(line) >= 3:
            table_id, col_i1, col_i2 = line[:3]
            gt_cpa = None
            if len(line) == 4:
                # gt_cpa = line[3].replace(cf.WDT, "")
                gt_cpa = {l.replace(cf.WDT, "") for l in line[3].split(" ")}
            if targets.get(table_id) is None:
                targets[table_id] = TargetCPA(table_id)
            targets[table_id].add(col_i1, col_i2, gt_cpa)
            n += 1
    return targets, n


if __name__ == "__main__":
    update_gt_redirect_cea("R1")
    update_gt_redirect_cea("R2")
    update_gt_redirect_cea("R3")
    update_gt_redirect_cea("R4")
    update_gt_redirect_cea("2T")
    # tar_cea, n = parse_target_cea(cf.DIR_SEMTAB_2_R4_TAR_CEA)
    # tar_cta, n = parse_target_cta(cf.DIR_SEMTAB_2_R4_TAR_CTA)
    # tar_cpa, n = parse_target_cpa(cf.DIR_SEMTAB_2_R4_TAR_CPA)
