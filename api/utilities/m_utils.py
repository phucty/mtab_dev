import csv
import math
import pickle
import re
import string
import struct
import unicodedata
import urllib
import zlib
from collections import Counter
from collections import defaultdict
from itertools import combinations

import ftfy
import numpy as np
import wikitextparser as wtp
from dateutil.parser import parse, parser
from scipy.special import softmax

import m_config as cf
from api import m_f
from api.utilities import m_io as iw
from api.utilities.parse_number import removeCommasBetweenDigits


def is_byte_obj(obj):
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False
    # try:
    #     obj.decode()
    #     return True
    # except (UnicodeDecodeError, AttributeError):
    #     return False


def is_date_complete(date, must_have_attr=("year", "month")):
    parse_res, _ = parser()._parse(date)
    # If date is invalid `_parse` returns (None,None)
    if parse_res is None:
        return False
    # For a valid date `_result` object is returned. E.g. _parse("Sep 23") returns (_result(month=9, day=23), None)
    for attr in must_have_attr:
        if getattr(parse_res, attr) is None:
            return False
    return True


def is_date(string, fuzzy=False):
    if not string:
        return False
    try:
        parse(string, fuzzy=fuzzy)
        if is_date_complete(string):
            return True
        return False
    except Exception:
        return False


def get_date(string, fuzzy=False):
    if not string or len(string) <= 4:
        return None
    months = [
        "january",
        "jan",
        "february",
        "feb",
        "march",
        "mar",
        "april",
        "apr",
        "may",
        "june",
        "jun",
        "july",
        "jul",
        "august",
        "aug",
        "september",
        "sep",
        "sept",
        "october",
        "oct",
        "november",
        "nov",
        "december",
        "dec",
    ]
    is_ok = False
    for month in months:
        if month in string.lower():
            is_ok = True
            break

    if not is_ok:
        return None

    try:
        if is_date_complete(string):
            return parse(string, fuzzy=fuzzy).strftime("%Y-%m-%d")
        else:
            return None
    except Exception:
        return None


def combination_n_k(n, k):
    f = math.factorial
    if n - k < 0:
        return 0
    return f(n) // f(k) // f(n - k)


def isEnglish(s):
    try:
        s.encode(encoding="utf-8").decode("ascii")
    except UnicodeDecodeError:
        return False
    else:
        return True


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    yield latest_row
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            yield latest_row


#
# def quote_url(input_str):
#     return urllib.parse.quote(input_str,
#                        safe="/:@&+$,-_.!~*'()–"
#                             "%22%25%5C%60%5E%23%3C%3E%5B%5D%7B%7C%7D")
#
#
# def unquote_url(input_str):
#     result = parse.unquote(input_str)
#     result = parse.unquote(result)
#     replace = {"\"": "%22",
#                # "'": "%27",
#                # "%": "%25",
#                "’": "'",
#                "?": "%3F",
#                "\\": "%5C",
#                "`": "%60",
#                "^": "%5E",
#                "#": "%23",
#                "<": "%3C",
#                ">": "%3E",
#                "[": "%5B",
#                "]": "%5D",
#                "{": "%7B",
#                "|": "%7C",
#                "}": "%7D",
#                "&amp;": "&",
#                }
#     for r1, r2 in replace.items():
#         result = result.replace(r1, r2)
#     try:
#         result = result.encode("latin1").decode("utf-8")
#     except ValueError:
#         pass
#     return result


def clean_text_brackets(text):
    if "[[" in text and "]]" in text and "#" in text:

        tmp_text = re.sub("\[\[(.*)\]\]", "", text)
        if tmp_text == text:
            text = ""
        else:
            text = tmp_text.strip()
    if "[[" in text and "#" in text and "]]" not in text:
        text = re.sub("\[\[(.*)", "", text).strip()
    if "]]" in text and "[[" not in text:
        text = re.sub("(.*)\]\]", "", text).strip()

    if "see" in text.lower():
        text = ""

    if "(" in text and not ")" in text:
        text = text.replace("(", "").strip()
    if ")" in text and not "(" in text:
        text = text.replace(")", "").strip()
    return text


# def delete_edits(word, edit_distance, delete_words, max_edit, min_len):
#     edit_distance += 1
#     word_len = len(word)
#     if word_len < min_len:
#         return delete_words
#
#     for i in range(word_len):
#         delete = word[: i] + word[i + 1:]
#         if len(delete) < min_len:
#             continue
#         if delete not in delete_words:
#             delete_words.add(delete)
#             # recursion, if maximum edit distance not yet reached
#             if edit_distance < max_edit:
#                 delete_edits(delete, edit_distance, delete_words, max_edit, min_len)
#     return delete_words
#
#
# def delete_edits_prefix(key, max_edit_dis, prefix_length, min_len=1):
#     hash_set = set()
#     if len(key) > prefix_length:
#         key = key[: prefix_length]
#     hash_set.add(key)
#     return delete_edits(key, 0, hash_set, max_edit_dis, min_len)


def delete_edits_prefix(key, max_edit_dis, prefix_length, min_len=1):
    if len(key) > prefix_length:
        key = key[:prefix_length]
    _min_len = len(key) - max_edit_dis - 1
    if _min_len < min_len:
        _min_len = min_len - 1
    combine = {
        "".join(l) for i in range(_min_len, len(key)) for l in combinations(key, i + 1)
    }
    return combine


def get_ngrams(input_text, n=1):
    def _get_ngrams_line(text):
        text = norm_text(text, punctuations=False)
        if not text:
            return Counter()
        if n == 1:
            _ngrams = Counter(text.split())
        else:  # n == 2
            _ngrams = Counter(
                [" ".join(b) for b in zip(text.split()[:-1], text.split()[1:])]
            )
        return _ngrams

    ngrams = Counter()
    if isinstance(input_text, str):
        ngrams.update(_get_ngrams_line(input_text))
    elif isinstance(input_text, list):
        for text_i in input_text:
            ngrams.update(_get_ngrams_line(text_i))
    return ngrams


def merge_ranking(ranking_lists, weight=None, is_score=True, is_sorted=False):
    item_scores = defaultdict(float)

    if weight is None:
        weight = [1] * len(ranking_lists)

    for i, ranking_i in enumerate(ranking_lists):
        # if get_softmax:
        ranking_pr = get_ranking_pr(
            ranking_i, mode_pr=cf.EnumPr.SOFTMAX, mode_score=cf.EnumRank.RANK
        )
        # else:
        #     ranking_pr = {e_label: e_score for e_label, e_score in ranking_i}
        for e_label, e_score in ranking_pr.items():
            item_scores[e_label] += e_score * weight[i]

    sum_weight = sum([weight[i] for i in range(len(weight)) if ranking_lists[i]])
    item_scores = {k: v / sum_weight for k, v in item_scores.items()}
    # item_scores = cal_p_from_ranking(item_scores)

    if is_score:
        if is_sorted:
            item_scores = sorted(item_scores.items(), key=lambda x: x[1], reverse=True)
    else:
        item_scores = sorted(item_scores.items(), key=lambda x: x[1], reverse=True)
        item_scores = [k for k, _ in item_scores]

    return item_scores


def cal_p_from_ranking(relevant_entities, mode=cf.EnumPr.SOFTMAX, is_sort=False, top=0):
    if not relevant_entities:
        return relevant_entities

    if isinstance(relevant_entities, dict):
        relevant_entities = list(relevant_entities.items())

    if top:
        relevant_entities.sort(key=lambda x: x[1], reverse=True)
        relevant_entities = relevant_entities[:top]

    if mode == cf.EnumPr.SOFTMAX:
        ranking_sm = softmax([[_sim for _, _sim in relevant_entities]])[0]
        relevant_entities = {
            candidate: ranking_sm[i]
            for i, (candidate, _) in enumerate(relevant_entities)
        }
    else:  # mode == cf.P_ENUM.AVG
        sum_candidates = sum(relevant_entities.values())
        relevant_entities = {
            key: score / sum_candidates for key, score in relevant_entities.items()
        }

    if is_sort:
        relevant_entities = sorted(
            relevant_entities.items(), key=lambda x: x[1], reverse=True
        )
    return relevant_entities


def get_ranking_pr(
    relevant_ranking,
    max_rank=cf.LIMIT_SEARCH,
    mode_pr=cf.EnumPr.SOFTMAX,
    mode_score=cf.EnumRank.RANK,
    is_sort=False,
    limit=0,
):
    relevant_cans = defaultdict(float)
    if not relevant_ranking:
        return relevant_cans

    if len(relevant_ranking) > max_rank:
        max_rank = len(relevant_ranking)
    if not limit:
        limit = len(relevant_ranking)

    if limit <= 0:
        limit = len(relevant_ranking)

    for e_rank, e_obj in enumerate(relevant_ranking[:limit]):
        if isinstance(e_obj, str):
            e_uri = e_obj
            e_score = 0
        else:
            e_uri, e_score = e_obj

        if mode_score == cf.EnumRank.RANK:
            relevant_cans[e_uri] = max_rank - e_rank
        elif mode_score == cf.EnumRank.SCORE:
            relevant_cans[e_uri] = e_score
        elif mode_score == cf.EnumRank.EQUAL:
            relevant_cans[e_uri] = 1

    if len(relevant_cans):
        relevant_cans = cal_p_from_ranking(relevant_cans, mode_pr, is_sort)

    return relevant_cans


def parse_triple_line(line, remove_prefix=True):
    if isinstance(line, (bytes, bytearray)):
        line = line.decode(cf.ENCODING)
    triple = line.split(" ", 2)
    # if len(triple) < 2 or len(triple) > 5 \
    #         or not (len(triple) == 4 and triple[-1] == "."):
    #     return None

    tail = triple[2].replace("\n", "")
    tail = tail.strip()
    if tail[-1] == ".":
        tail = tail[:-1]
    tail = tail.strip()

    head = norm_namespace(triple[0], remove_prefix)
    prop = norm_namespace(triple[1], remove_prefix)
    tail = norm_namespace(tail, remove_prefix)
    return head, prop, tail


def norm_namespace(ns, is_remove_prefix=True):
    if ns[0] == "<" and ns[-1] == ">":
        ns = ns[1:-1]
    if is_remove_prefix:
        ns = remove_prefix(ns)
    return ns


def remove_prefix(ns):
    for prefix in cf.PREFIX_LIST:
        ns = ns.replace(prefix, "")
    return ns


def convert_num(text):
    if not text:
        return None
    try:
        text = removeCommasBetweenDigits(text)
        # tmp = representsFloat(text)
        # if not tmp:
        #     return None
        #
        # return parseNumber(text)
        return float(text)
    except ValueError:
        return None


def get_wd_int(wd_id):
    result = None
    if wd_id and len(wd_id) and wd_id[0].lower() in ["p", "q"] and " " not in wd_id:
        result = convert_num(wd_id[1:])
    return result


def is_wd_item(wd_id):
    if get_wd_int(wd_id) is None:
        return False
    else:
        return True


def norm_queries(text, punctuations=False, article=True, lower=True, seg=False):
    normed = norm_text(text, punctuations, article, lower)
    if normed.startswith("re:"):
        normed = normed[3:]
    if seg and not normed.startswith("template:"):
        word_list = [len(w) for w in normed.split() if w]
        if word_list:
            max_words_len = max(word_list)
            if max_words_len >= 40:  # heuristics
                normed = m_f.m_corrector().word_seg(normed)
    return normed


def norm_text(text, punctuations=False, article=True, lower=True):
    text = ftfy.fix_text(text)
    text = "".join(filter(lambda c: unicodedata.category(c) != "Cf", text))
    text = unicodedata.normalize("NFKC", text)  # .replace("\u002D", "")
    if lower:
        text = text.lower()
        # text = re.sub(re.compile(r'\s+'), ' ', text)

    # if not accents:
    #
    # remove accents: https://stackoverflow.com/a/518232
    # text = ''.join(c for c in unicodedata.normalize('NFD', text)
    #                if unicodedata.category(c) != 'Mn')
    # text = unicodedata.normalize('NFC', text)

    # Remove article
    if not article:
        text = re.sub(r"\b(a|an|the|and)\b", " ", text)

    # Remove 3 duplicate character
    # text = "".join(char if text.count(char, 0, i) < 2 else "-" for i, char in enumerate(text)))
    text = re.sub(r"([a-zA-Z])\1\1+", r"\1\1", text)

    # Remove punctuations
    if not punctuations:
        exclude_c = set(string.punctuation)
        tmp_text = "".join(c for c in text if c not in exclude_c)
        if tmp_text:
            text = tmp_text

    # Remove space, enter
    text = " ".join(text.split())
    return text


def norm_strip_text(text):
    text = " ".join(text.split())
    return text


def norm_wikipedia_title(title, unquote=False):
    if not title or not len(title):
        return title
    if len(title) > 1:
        title = (title[0].upper() + title[1:]).replace("_", " ")
    else:
        title = title[0].upper()
    if unquote:
        title = urllib.parse.unquote(title)
    return title


def wiki_plain_text(org_text, next_parse=1):
    try:
        parse_text = wtp.parse(org_text)
        text = parse_text.plain_text(
            replace_tags=False, replace_bolds_and_italics=False
        )
        for t in parse_text.get_tags():
            text = text.replace(t.string, "")
        if "<" in text and next_parse < 3:
            return wiki_plain_text(text, next_parse + 1)
        return text
    except Exception as message:
        print(f"{message}: {org_text}")
        return org_text


def is_redirect_or_disambiguation(wd_id):
    wd_i_redirect = m_f.m_mapper().get_redirect_wikidata(wd_id)
    if wd_i_redirect and wd_i_redirect != wd_id:
        return True
    wd_types = m_f.m_items().get_types_specific_dbpedia(wd_id)
    # Wikimedia disambiguation page
    if wd_types and "Q4167410" in wd_types:
        return True
    return False


def get_dump_obj(k, v, encoder="", integerkey=False, compress=False):
    try:
        if not integerkey:
            if not isinstance(k, str):
                k = str(k)
            k = k.encode(cf.ENCODING)[: cf.LMDB_MAX_KEY]
        else:
            k = struct.pack("I", k)

        if encoder == "pickle":
            v_obj = pickle.dumps(v)
        else:
            v_obj = np.array(list(v), dtype=np.uint32).tobytes()

        if compress:
            v_obj = zlib.compress(v_obj, 9)

        dump_obj = (k, v_obj)
        return dump_obj
    except Exception as message:
        iw.print_status(message)
        return None


def expand_template_non_recursive(temp):
    # # expand children
    # last_span = 0
    # tmp_str = ""
    # for temp_i in temp.templates:
    #     if temp_i.span[0] > last_span:
    #         tmp_str += temp.string[last_span: temp_i.span[0]] + expand_template_non_recursive(temp_i)
    #         last_span = max(last_span, temp.span[1])

    tmp_span = temp.string
    if temp.name.lower() in ["small", "angle bracket", "ipa", "nowrap"]:
        tmp_span = "|".join(temp.string[2:-2].split("|")[1:])
    elif temp.name.lower() in ["bartable"]:
        tmp_split = temp.string[2:-2].split("|")
        if len(tmp_split) > 1:
            tmp_span = tmp_split[1]
    elif temp.name.lower() in ["sortname"]:
        tmp_split = temp.string[2:-2].split("|")
        if len(tmp_split) > 2:
            tmp_span = " ".join(tmp_split[:2])
    elif temp.name.lower() in ["font color"]:
        tmp_span = temp.string[2:-2].split("|")[-1]

    return tmp_span


def expand_template(temp):
    last_span = 0
    tmp_str = ""
    for temp_i in temp.templates:
        if temp_i.span[0] - temp.span[0] >= last_span:
            tmp_str += temp.string[last_span : temp_i.span[0] - temp.span[0]]
            expand_substring = expand_template(temp_i)
            if expand_substring != temp_i.string:
                debug = 1
            tmp_str += expand_substring
            last_span = temp_i.span[1] - temp.span[0]

    tmp_str += temp.string[last_span:]

    tmp_span = tmp_str

    if temp.name.lower() in [
        "small",
        "angle bracket",
        "ipa",
        "nowrap",
        "center",
        "plainlist",
        "included",
        "nom",
    ]:
        tmp_span = "|".join(tmp_str[2:-2].split("|")[1:])
        if temp.name.lower() == "plainlist":
            tmp_span = tmp_span.replace("*", "")
    elif temp.name.lower() in ["bartable"]:
        tmp_split = tmp_str[2:-2].split("|")
        if len(tmp_split) > 1:
            tmp_span = tmp_split[1]
    elif temp.name.lower() in ["sortname"]:
        tmp_split = tmp_str[2:-2].split("|")
        if len(tmp_split) > 3:
            tmp_span = " ".join(tmp_split[1:3])
    elif temp.name.lower() in ["font color"]:
        tmp_span = tmp_str[2:-2].split("|")[-1]
    return tmp_span


def remove_sub_string(input_string, remove_substrings):
    output = input_string
    for sub_string in remove_substrings:
        output = output.replace(sub_string, " ")

    output = " ".join(output.split(" ")).strip()
    return output


def replace_html_tags_2(input_text_org):
    input_text = input_text_org
    for r in ["{{break}}", "<br />", "<br/>", "<br>", "<br", " \n", "\n "]:
        input_text = input_text.replace(r, "\n")
    # Check formatnum
    while "{{formatnum:" in input_text:
        input_text = input_text.replace("{{formatnum:", "", 1)
        input_text = input_text.replace("}}", "", 1)
    # dts: https://simple.wikipedia.org/wiki/Template:Dts
    # font color
    input_text_lower = input_text.lower()
    if "{{font color" in input_text_lower:
        debug = 1
    elif "{{note" in input_text_lower:
        debug = 1
    elif "rp" in input_text_lower:
        debug = 1
    elif "{{small" in input_text_lower:
        debug = 1
    elif "{{bartable" in input_text_lower:
        debug = 1
    elif "{{sortname" in input_text:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:Flag
    elif "{{flag" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:Citation
    elif "{{cite" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:Efn
    elif "{{efn" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:Sfn
    elif "{{sfn" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:ns
    elif "{{ns" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:ref
    elif "{{ref" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:inflation
    elif "{{inflation" in input_text_lower:
        debug = 1
    # Ignore: https://en.wikipedia.org/wiki/Template:inflation
    elif "{{as of" in input_text_lower:
        debug = 1
    elif "{{chset" in input_text_lower:
        debug = 1
    elif "{{abbr" in input_text_lower:
        debug = 1
    elif "{{wikt" in input_text_lower:
        debug = 1
    elif "{{hover" in input_text_lower:
        debug = 1
    elif "{{#tag" in input_text_lower:
        debug = 1
    elif "{{bar" in input_text_lower:
        debug = 1
    elif "{{needs update" in input_text_lower:
        debug = 1
    elif "{{nowrap" in input_text_lower:
        debug = 1  # '|indonesia}}'
    elif "{{simplenuclide2" in input_text_lower:
        debug = 1
    elif "{{coat of arms" in input_text_lower:
        debug = 1
    elif "{{un_population" in input_text_lower:
        debug = 1
    elif "{{sort" in input_text_lower:
        debug = 1
    elif "{{snd" in input_text_lower:
        debug = 1
    elif "{{increase" in input_text_lower:
        debug = 1
    elif "{{decrease" in input_text_lower:
        debug = 1
    elif "{{" in input_text_lower:
        debug = 1

    tmp_parse = wtp.parse(input_text)
    input_text_tmp = ""
    last_span = 0
    for temp in tmp_parse.templates:
        # if not temp.name or temp.span[0] < last_span:
        #     tmp_span = temp.string
        # elif temp.name.lower() in ["small", "angle bracket", "ipa", "nowrap"]:
        #     tmp_span = "|".join(temp.string[2:-2].split("|")[1:])
        # elif temp.name.lower() in ["bartable"]:
        #     tmp_split = temp.string[2:-2].split("|")
        #     if len(tmp_split) > 1:
        #         tmp_span = tmp_split[1]
        #     else:
        #         tmp_span = temp.string
        # elif temp.name.lower() in ["sortname"]:
        #     tmp_split = temp.string[2:-2].split("|")
        #     if len(tmp_split) > 2:
        #         tmp_span = " ".join(tmp_split[:2])
        #     else:
        #         tmp_span = temp.string
        # elif temp.name.lower() in ["font color"]:
        #     tmp_span = temp.string[2:-2].split("|")[-1]
        # else:
        #     tmp_span = temp.string
        if temp.span[0] >= last_span:
            tmp_span = expand_template(temp)
            input_text_tmp += input_text[last_span : temp.span[0]] + tmp_span
            last_span = max(last_span, temp.span[1])

    input_text = input_text_tmp + input_text[last_span:]

    # while (
    #     "{{font color" in input_text
    #     # or "{{flag" in input_text
    #     or "{{small|" in input_text
    #     or "{{bartable" in input_text
    #     or "{{angle bracket" in input_text
    #     or "{{IPA" in input_text
    #     or "{{sortname" in input_text
    #     or "{{nowrap" in input_text
    # ):
    #     start = input_text.find("{{")
    #     end = input_text.find("}}") + 2
    #     if start < 0 or end < 2 or end < start:
    #         break
    #
    #     tmp_str = input_text[start + 2 : end - 2]
    #
    #     # if "|" in input_text[start + 2 : end - 2]:
    #     # Get the remaining part
    #     if (
    #         "small|" in tmp_str
    #         or "angle bracket" in tmp_str
    #         or "IPA" in tmp_str
    #         or "nowrap" in input_text
    #     ):
    #         tmp_span = "|".join(tmp_str.split("|")[1:])
    #
    #     # Get the first |
    #     elif "{{bartable" in tmp_str:
    #         tmp_split = tmp_str.split("|")
    #         if len(tmp_split) > 1:
    #             tmp_span = tmp_split[1]
    #         else:
    #             tmp_span = tmp_str
    #
    #     elif "{{sortname" in tmp_str:
    #         tmp_split = tmp_str.split("|")
    #         if len(tmp_split) > 2:
    #             tmp_span = " ".join(tmp_split[:2])
    #         else:
    #             tmp_span = tmp_str
    #
    #     else:
    #         tmp_span = tmp_str.split("|")[-1]
    #
    #     input_text_tmp = input_text[:start] + tmp_span + input_text[end:]
    #     if len(input_text_tmp) >= len(input_text_lower):
    #         # iw.print_status(input_text_tmp)
    #         break
    #     input_text = input_text_tmp

    input_text = remove_html_tags(input_text, cf.REMOVE_HTML_TAGS)

    # input_text = remove_sub_string(input_text, cf.REMOVE_TAGS)
    return input_text


def remove_html_tags(input_string, remove_tags):
    output = input_string
    for start, end in remove_tags:
        while True:
            i_start = output.find(start)
            if len(output) > i_start >= 0:
                i_end = output.find(end, i_start, len(output))
                if len(output) > i_end >= 0:
                    output = output[:i_start] + output[i_end + (len(end)) :]
                else:
                    break
            else:
                break
    output = " ".join(output.split(" ")).strip()
    return output


def select_oldest_wd(wd_list):
    return sorted(wd_list, key=lambda x: int(x[1:]))[0]
