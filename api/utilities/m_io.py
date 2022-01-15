from datetime import timedelta

import charamel
import csv
import fnmatch
import gzip
import json
import logging
import math
import os
import pickle
import re
import secrets
import shutil
import sys
import unicodedata
from tabulate import tabulate
import types
import zipfile
import zlib
from collections import defaultdict
from itertools import islice
from time import time
from charamel import Detector


import chardet
import clevercsv
import numpy as np
import petl as petl
import texttable as tt

# import pickle5 as pickle
# from six.moves import cPickle as pickle
from tqdm import tqdm
import bz2
import gzip
import m_config as cf


def prepare_input_tables(upload_dir):
    if not zipfile.is_zipfile(upload_dir):
        return None

    table_input = {"tables": set(), "cea": None, "cta": None, "cpa": None}
    tables_dir = f"{cf.DIR_ROOT}/data/users/uploads/{secrets.token_hex(8)}"
    create_dir(tables_dir)
    create_dir(f"{tables_dir}/tables/")

    with zipfile.ZipFile(upload_dir, "r") as zipf:
        for _f in zipf.namelist():
            if _f.startswith("__MACOSX") or _f.endswith(".DS_Store"):
                continue
            if _f[-3:].lower() in cf.TABLE_EXTENSIONS:
                if "tables/" in _f and _f[-7:-4] not in ["cea", "cta", "cpa"]:
                    tar_file = f"{tables_dir}/tables/{os.path.basename(_f)}"
                    with open(tar_file, "wb") as file_obj:
                        shutil.copyfileobj(zipf.open(_f, "r"), file_obj)
                    table_input["tables"].add(tar_file)

                elif _f[-7:-4] in ["cea", "cta", "cpa"]:
                    tar_file = f"{tables_dir}/{_f[-7:]}"
                    with open(tar_file, "wb") as file_obj:
                        shutil.copyfileobj(zipf.open(_f, "r"), file_obj)
                    table_input[_f[-7:-4]] = tar_file

        if not table_input["tables"]:
            # Remove folder
            shutil.rmtree(tables_dir)
            return None
        # for v in table_input.values():
        #     if not v:
        #         Remove folder
        # shutil.rmtree(tables_dir)
        # return None
    return table_input, tables_dir


def get_size_obj(num, suffix="B"):
    if num == 0:
        return "0"
    magnitude = int(math.floor(math.log(num, 1024)))
    val = num / math.pow(1024, magnitude)
    if magnitude > 7:
        return "{:3.1f}{}{}".format(val, "Yi", suffix)
    return "{:3.1f}{}{}".format(
        val, ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"][magnitude], suffix
    )


def read_line_from_file(file_name):
    reader = None
    if ".bz2" in file_name:
        reader = bz2.BZ2File(file_name)
    elif ".gz" in file_name:
        reader = gzip.open(file_name)
    else:
        reader = open(file_name)
    if reader:
        for line in reader:
            yield line


def delete_folder(folder_dir):
    if os.path.exists(folder_dir):
        shutil.rmtree(folder_dir, ignore_errors=False)
    return True


def delete_file(file_dir):
    if os.path.exists(file_dir):
        os.remove(file_dir)
    return True


def create_dir(file_dir):
    """Create a directory

    Args:
        file_dir (str): file directory
    """
    folder_dir = os.path.dirname(file_dir)
    if not os.path.exists(folder_dir):
        os.makedirs(folder_dir)


def get_size_of_file(num, suffix="B"):
    """Get human friendly file size
    https://gist.github.com/cbwar/d2dfbc19b140bd599daccbe0fe925597#gistcomment-2845059

    Args:
        num (int): Bytes value
        suffix (str, optional): Unit. Defaults to 'B'.

    Returns:
        str: file size0
    """
    if num == 0:
        return "0"
    magnitude = int(math.floor(math.log(num, 1024)))
    val = num / math.pow(1024, magnitude)
    if magnitude > 7:
        return "{:3.1f}{}{}".format(val, "Yi", suffix)
    return "{:3.1f}{}{}".format(
        val, ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"][magnitude], suffix
    )


def save_obj_pkl(file_name, save_object, is_compress=False, is_message=True):
    create_dir(file_name)
    save_file = file_name
    if ".pkl" not in file_name:
        save_file = file_name + ".pkl"
    if is_compress and ".zlib" not in file_name:
        save_file += ".zlib"

    temp_file = save_file + ".temp"

    # Write temp
    with open(temp_file, "wb") as fp:
        if is_compress:
            save_data = zlib.compress(
                pickle.dumps(save_object, pickle.HIGHEST_PROTOCOL)
            )
            fp.write(save_data)
        else:
            pickle.dump(save_object, fp, pickle.HIGHEST_PROTOCOL)

    try:
        if os.path.exists(save_file):
            os.remove(save_file)
    except Exception as message:
        print_status(message)

    os.rename(temp_file, save_file)
    if is_message:
        print_status("Saved: - %d - %s" % (len(save_object), save_file), is_log=False)
    return save_file


def load_obj_pkl(file_name, is_message=False):
    load_obj = None
    if not os.path.exists(file_name) and ".pkl" not in file_name:
        file_name = file_name + ".pkl"

    if not os.path.exists(file_name) and ".zlib" not in file_name:
        file_name = file_name + ".zlib"
    with open(file_name, "rb") as fp:
        if ".zlib" in file_name:
            load_obj = pickle.loads(zlib.decompress(fp.read()))
        else:
            load_obj = pickle.load(fp)
    if is_message and load_obj:
        print_status("%d loaded items - %s" % (len(load_obj), file_name))
    return load_obj


def chunks(data, limit=100000):
    """[summary]

    Args:
        data ([type]): [description]
        limit (int, optional): [description]. Defaults to 100000.

    Yields:
        [type]: [description]
    """
    it = iter(data)
    for _ in range(0, len(data), limit):
        yield {k: data[k] for k in islice(it, limit)}


def save_dict_pkl(
    file_name, save_object_iter, compressed=False, is_message=True, buff_size=1000000
):
    # 1. Create directory
    create_dir(file_name)

    # 2. Add pkl extension to file name
    save_file = file_name
    if ".pkl" not in file_name:
        save_file = file_name + ".pkl"

    # 3. Save to a temp file
    temp_file = save_file + ".temp"

    if compressed:
        fp = gzip.open(temp_file, "wb")
    else:
        fp = open(temp_file, "wb")
    p_bar = tqdm(desc="Saving")
    for save_obj in chunks(save_object_iter, buff_size):
        pickle.dump(save_obj, fp, pickle.HIGHEST_PROTOCOL)
        p_bar.update(len(save_obj))
        if is_message:
            print_status(
                "\nSave | size:%d | %s" % (len(save_object_iter), save_file),
                is_log=False,
            )
    p_bar.close()
    fp.close()

    try:
        if os.path.exists(save_file):
            os.remove(save_file)
    except Exception as message:
        print_status(message)

    os.rename(temp_file, save_file)
    return save_file


def load_dict_pkl(file_name, compressed=False, is_message=False):
    load_obj = defaultdict()
    if not os.path.exists(file_name) and ".pkl" not in file_name:
        file_name = file_name + ".pkl"
    with (gzip.open if compressed else open)(file_name, "rb") as fp:
        with tqdm(desc="Load %s" % os.path.basename(file_name)) as p_bar:
            if is_message and len(load_obj):
                print_status("%d loaded items - %s" % (len(load_obj), file_name))
            while True:
                try:
                    load_items = pickle.load(fp)
                    load_obj.update(load_items)
                    p_bar.update(len(load_items))
                except EOFError:
                    break
    return load_obj


def load_text_obj(file_name):
    with open(file_name, "r") as fr:
        return fr.readlines()
    #     for line in save_object:
    #         line_text = deliminator.join([str(line_obj) for line_obj in line]) + "\n"
    #         fp.write(line_text)
    # if "bz2" in file_name:
    #     fp = bz2.BZ2File(file_name)
    # else:
    #     fp = open(file_name, "r", "utf-8")
    # while True:
    #     line = fp.readline()
    #     if not line:
    #         break
    #     if "bz2" in file_name:
    #         yield line.decode("utf-8")
    #     else:
    #         yield line
    # fp.close()


def save_text_obj(file_name, save_object, deliminator=" ", is_message=True):
    create_dir(file_name)
    save_file = file_name
    if ".txt" not in file_name:
        save_file = file_name + ".txt"

    temp_file = save_file + ".temp"
    # Write temp
    with open(temp_file, "w") as fp:
        for line in save_object:
            line_text = deliminator.join([str(line_obj) for line_obj in line]) + "\n"
            fp.write(line_text)

    try:
        if os.path.exists(save_file):
            os.remove(save_file)
    except Exception as message:
        print_status(message)

    os.rename(temp_file, save_file)
    if is_message:
        file_size = get_size_of_file(os.stat(save_file).st_size)
        print_status(
            "\nSaved: %s - %d - %s" % (file_size, len(save_object), save_file),
            is_log=False,
        )
    return save_file


def save_object_pickle(file_name, save_object, is_print=False):
    create_dir(file_name)
    temp_file = "%s.temp" % file_name
    # Write temp
    with open(temp_file, "wb") as f:
        pickle.dump(save_object, f)
    if os.path.exists(file_name):
        os.remove(file_name)
    os.rename(temp_file, file_name)

    if is_print:
        print_status("Saved: %d" % len(save_object))


def load_object_pickle(file_name, is_message=False):
    load_obj = None
    with open(file_name, "rb") as fp:
        load_obj = pickle.load(fp)
        if is_message and load_obj:
            print_status("Loaded: %d" % len(load_obj))
    return load_obj


def get_encoding(source, method="charamel"):
    from api import m_f

    result = "utf-8"
    if os.path.isfile(source):
        with open(source, "rb") as file_open:
            # Read all content --> make sure about the file encoding
            file_content = file_open.read()

            # predict encoding
            if method == "charamel":
                try:
                    # import charamel

                    # charamel.Detector()
                    # encoding_detector = charamel.Detector()
                    detector = m_f.encoding_detector().detect(file_content)
                    if detector:
                        result = detector.value
                except Exception as message:
                    print_status(message, is_screen=False)
                    pass
            else:
                detector = chardet.detect(file_content)
                if detector["encoding"]:
                    result = detector["encoding"]
    return result


# def get_encoding(source):
#     result = {"encoding": "utf-8"}
#     if os.path.isfile(source):
#         with open(source, "rb") as file_open:
#             # Read all content --> make sure about the file encoding
#             file_content = file_open.read()
#
#             # predict encoding
#             detector = chardet.detect(file_content)
#             if detector['encoding']:
#                 result["encoding"] = detector['encoding']
#
#         # predict deliminator
#         if os.path.splitext(source)[1][1:] == "csv":
#             with open(source, "r", encoding=result["encoding"]) as file_open:
#                 file_content = file_open.read()
#                 result["dialect"] = csv.Sniffer().sniff(file_content)
#     else:
#         try:
#             result["dialect"] = csv.Sniffer().sniff(source)
#         except:
#             pass
#
#     return result


def get_dialect(source, encoding):
    result = None
    if os.path.isfile(source):
        if os.path.splitext(source)[1][1:] == "csv":
            with open(source, "r", encoding=encoding) as file_open:
                source = file_open.read(1000)

    try:
        # quotechar, doublequote, delimiter, skipinitialspace = \
        #     self._guess_quote_and_delimiter(sample, delimiters)
        # if not delimiter:
        #     delimiter, skipinitialspace = self._guess_delimiter(sample,
        #                                                         delimiters)
        #
        # if not delimiter:
        #     raise Error("Could not determine delimiter")
        #
        # class dialect(csv.Dialect):
        #     _name = "sniffed"
        #     lineterminator = '\r\n'
        #     quoting = csv.QUOTE_MINIMAL
        #     # escapechar = ''
        #
        # dialect.doublequote = doublequote
        # dialect.delimiter = delimiter
        # # _csv.reader won't accept a quotechar of ''
        # dialect.quotechar = quotechar or '"'
        # dialect.skipinitialspace = skipinitialspace
        #
        # return dialect
        #
        result = clevercsv.Sniffer().sniff(source).to_csv_dialect()
        #
        if not result.lineterminator:
            result.lineterminator = "\r\n"
        if not result.delimiter:
            result.delimiter = ","
        # if len(result.delimiter) > 1:
        #     return None
        # result = csv.Sniffer().sniff(source)
        # result.delimiter = tmp.delimiter
        # if tmp.quotechar:
        #     result.quotechar = tmp.quotechar
        # if tmp.escapechar:
        #     result.escapechar = tmp.escapechar
        # result.strict = tmp.strict
        # result = csv.Sniffer().sniff(source)
    except Exception as message:
        print_status(message, is_screen=False)
        pass
    return result


def load_object_csv(file_name, encoding=None, retries=2):
    content = []
    try:
        if os.path.exists(file_name):
            if not encoding:
                encoding = get_encoding(file_name)
            with open(file_name, "r", encoding=encoding) as f:
                reader = csv.reader(f, delimiter=",")
                for r in reader:
                    content.append(r)
        return content
    except Exception as message:
        if retries == 0:
            return content
        print_status(message, is_screen=False)
        new_encoding = get_encoding(file_name, method="chardet")
        if new_encoding not in ["utf-8", "utf8"]:
            return load_object_csv(
                file_name, encoding=new_encoding, retries=retries - 1
            )
        else:
            return load_object_csv(file_name, encoding="utf-8", retries=0)

    #
    # try:
    #     if os.path.exists(file_name):
    #         if not encoding:
    #             encoding = get_encoding(file_name)
    #         with open(file_name, "r", encoding=encoding) as f:
    #             # dialect = get_dialect(file_name, file_encoding)
    #             # reader = csv.reader(f, dialect=dialect)
    #             reader = csv.reader(f, delimiter=",")
    #
    #             # dialect = csv.Sniffer().sniff(f.read(), delimiters=';\t,', )
    #             # f.seek(0)
    #             # reader = csv.reader(f, dialect)
    #             # content = []
    #             # for r in reader:
    #             #     temp_c = []
    #             #     for r_i in r:
    #             #         if len(r_i) < 2:
    #             #             continue
    #             #         if r_i[-2:] != '\\"':
    #             #             temp_c.append(r_i[1:-1])
    #             #         else:
    #             #             temp_c.append(r_i[1:])
    #             #     content.append(temp_c)
    #             for r in reader:
    #                 content.append(r)
    #             # content = [r for r in reader]
    #             # content = np.array([r for r in reader])
    #             # m = re.split(r'\n+', f.read())
    #             # for line in m:
    #             #     content.append(re.findall(r'(?<!\\)"(?:\\"|[^"])*(?<!\\)"', line))
    #
    #     # Fix problem of \\"\n in csv
    #     # fix_content = []
    #     # for line in content:
    #     #     is_n_lines = False
    #     #     lines = []
    #     #     line_i = []
    #     #     for c in line:
    #     #         if "\"\n" in c:
    #     #             is_n_lines = True
    #     #             c_split = c.split("\"\n")
    #     #             line_i.append(c_split[0])
    #     #             lines.append(line_i)
    #     #             line_i = [c_split[-1]]
    #     #         else:
    #     #             line_i.append(c)
    #     #     if is_n_lines:
    #     #         lines.append(line_i)
    #     #
    #     #     if is_n_lines:
    #     #         fix_content.extend(lines)
    #     #     fix_content.append(line)
    #     # return content


def save_object_csv(file_name, rows):
    create_dir(file_name)
    temp_file = "%s.temp" % file_name
    with open(temp_file, "w") as f:
        try:
            writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
            for r in rows:
                if (
                    isinstance(r, list)
                    or isinstance(r, tuple)
                    or isinstance(r, np.ndarray)
                ):
                    writer.writerow(r)
                else:
                    writer.writerow([r])
        except Exception as message:
            print(message)
    if os.path.exists(file_name):
        os.remove(file_name)
    os.rename(temp_file, file_name)


def save_dict_pickle(folder_dir, input_dict, split_size=20000):
    count_f = 0
    temp_dict = defaultdict()
    for key, value in input_dict.items():
        temp_dict[key] = value
        if len(temp_dict) == split_size:
            file_dir = "%s/%d.pkl" % (folder_dir, count_f)
            save_object_pickle(file_dir, temp_dict)
            temp_dict = defaultdict()
            count_f += 1

    file_dir = "%s/%d.pkl" % (folder_dir, count_f)
    save_object_pickle(file_dir, temp_dict)
    print_status("Saved: %d" % (len(input_dict)))


def load_dict_pickle(folder_dir):
    output_dict = defaultdict()
    for basename in os.listdir(folder_dir):
        try:
            filename = os.path.join(folder_dir, basename)
            if os.path.isfile(filename):
                temp = load_object_pickle(filename)
                for k, v in temp.items():
                    output_dict[k] = v
                # output_dict = {**output_dict, **temp}
        except Exception as message:
            print_status("Error at: %s - %s" % (basename, message))
    return output_dict


def print_progress(current, total, message="", bar_size=20):
    progress = (current + 1) * 1.0 / total
    block = int(round(bar_size * progress))
    sys.stdout.write(
        "\r%s:[%s][%5.2f%%][%d/%d]"
        % (
            message,
            "=" * block + "-" * (bar_size - block),
            progress * 100,
            current + 1,
            total,
        )
    )
    sys.stdout.flush()


def print_count(message, count):
    text = "\r%s: %d" % (message, count)
    sys.stdout.write(text)
    sys.stdout.flush()


def print_count_2(m1, c1, m2, c2):
    text = "\r%s: %d - %s: %d" % (m1, c1, m2, c2)
    sys.stdout.write(text)
    sys.stdout.flush()


def print_status(message, is_screen=True, is_log=True) -> object:
    if isinstance(message, int):
        message = f"{message:,}"

    if is_screen:
        print(message)
    if is_log:
        logging.info(message)


def print_run_time(start, current, total, message=""):
    end = time()
    duration = end - start
    hours, rem = divmod(duration, 3600)
    minutes, _ = divmod(rem, 60)
    p_message = "[{:0>2}:{:0>2}]".format(int(hours), int(minutes))
    avg_time = duration / (current + 1.0)
    wait_time = (total - current) * avg_time
    hours, rem = divmod(wait_time, 3600)
    minutes, _ = divmod(rem, 60)
    p_message += "[{:0>2}:{:0>2}]".format(int(hours), int(minutes))
    p_message += "[%.2fitem/s] - %s" % (1.0 / avg_time, message)
    print_progress(current, total, p_message)


def print_func_run_time(
    func, *args, func_name=None, message=None, is_screen=True, is_log=True,
):
    if not isinstance(func, types.FunctionType):
        raise TypeError

    if func_name is None:
        func_name = func.__name__

    start = time()
    func(*args)
    run_time = timedelta(seconds=time() - start)
    if message is None:
        message = f"{func_name}: {run_time}"
    else:
        message = f"{func_name}: {message} - {run_time}"

    print_status(message, is_screen=is_screen, is_log=is_log)


def print_table(
    cell_values, n_col, headers=None, max_row=0, is_screen=False, is_get_log=True
):
    if headers is None:
        headers = []
    log_message = ""
    try:
        # max_row = len(cell_values) if max_row <= 0 else max_row
        # tab = tt.Texttable(max_width=0)  # max_width=300
        # tab.add_row([""] + [_r for _r in range(n_col)])
        # for r_i, r in enumerate(cell_values[:max_row]):
        #     if len(r) < n_col:
        #         r += ["" for i in range(n_col - len(r))]
        #     r = [str(r_i)] + r
        #     tab.add_row(r)
        # log_message += tab.draw()
        headers = ["I"] + [i for i in range(n_col)]
        cell_values = [[str(i)] + r for i, r in enumerate(cell_values)]
        log_message += str(tabulate(cell_values, headers=headers, tablefmt="github"))
    except Exception as messageException:
        print_status(messageException)
        pass
    if is_get_log:
        return log_message
    else:
        print_status(log_message, is_screen)


def get_files_from_dir_stream(folder_path, extension="*"):
    for root, _, file_dirs in os.walk(folder_path):
        for file_dir in fnmatch.filter(file_dirs, "*.%s" % extension):
            if ".DS_Store" not in file_dir:
                yield os.path.join(root, file_dir)


def get_files_from_dir_subdir(folder_path, extension="*"):
    all_files = []
    for root, _, file_dirs in os.walk(folder_path):
        for file_dir in fnmatch.filter(file_dirs, "*.%s" % extension):
            if ".DS_Store" not in file_dir:
                all_files.append(os.path.join(root, file_dir))
    return all_files


def get_files_from_dir(
    folder_path, extension="*", limit_reader=-1, is_sort=False, reverse=False
):
    all_file_dirs = get_files_from_dir_subdir(folder_path, extension)

    if is_sort:
        file_with_size = [(f, os.path.getsize(f)) for f in all_file_dirs]
        file_with_size.sort(key=lambda f: f[1], reverse=reverse)
        all_file_dirs = [f for f, _ in file_with_size]
    if limit_reader < 0:

        limit_reader = len(all_file_dirs)
    return all_file_dirs[:limit_reader]


def load_object_json(filename):
    object_list = []
    with open(filename) as f:
        object_list = [json.loads(line) for line in f.readlines()]
    return object_list


def get_valid_filename(any_str):
    any_str = (
        unicodedata.normalize("NFKD", any_str).encode("ascii", "ignore").decode("ascii")
    )
    any_str = re.sub(r"[^\w\s-]", "_", any_str).strip()
    any_str = re.sub(r"[-\s]+", "-", any_str)
    return any_str


def describe(arr):
    print("Descriptive analysis")
    print("Array Len=", len(arr))
    print("Measures of Central Tendency")
    print("Mean =", np.mean(arr))
    print("Median =", np.median(arr))
    print("Measures of Dispersion")
    print("Minimum =", np.amin(arr))
    print("Maximum =", np.amax(arr))
    print("Range =", np.ptp(arr))
    print("Variance =", np.var(arr))
    print("Standard Deviation =", np.std(arr))
