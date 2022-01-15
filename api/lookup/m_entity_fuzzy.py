import gc
import math
import re
from collections import defaultdict
from time import time

# from contextlib import closing
# from multiprocessing.pool import Pool
# import sys
# import lmdb
import symspellpy.helpers as helpers
from symspellpy.suggest_item import SuggestItem

# from rapidfuzz import fuzz, process
# import pickle
# from api.utilities import m_sim as sim
from symspellpy import SymSpell, Verbosity
from symspellpy.editdistance import EditDistance
from tqdm import tqdm

import m_config as cf
from api import m_f
from api.resources.m_db_item import DBItem, DBItemDefault
from api.utilities import m_io as iw
from api.utilities import m_utils as ul
from api.lookup import m_entity_search
from itertools import accumulate


class FuzzySearch(SymSpell):
    def __init__(
        self, max_dictionary_edit_distance=2, prefix_length=10, count_threshold=1
    ):
        super().__init__(max_dictionary_edit_distance, prefix_length, count_threshold)
        self._max_length = cf.LMDB_MAX_KEY
        self._db_deletes = defaultdict()

    def db_deletes(self, d, pl, lang):
        if not self._db_deletes.get((d, pl, lang)):
            dir_db = f"{cf.DIR_MODELS}/deletes_{lang}_d{d}_pl{pl}.lmdb"
            self._db_deletes[(d, pl, lang)] = DBItemDefault(dir_db)
        return self._db_deletes[(d, pl, lang)]

    def lookup_mtab(
        self,
        phrase,
        verbosity,
        max_edit_distance=2,
        prefix_length=10,
        db=220,
        limit=0,
        expensive=True,
        include_unknown=False,
        ignore_token=None,
        transfer_casing=False,
        lang="en",
    ):
        db_deletes = self.db_deletes(int(db / 100), int(db % 100), lang)

        suggestions = []
        suggestions_2 = []
        phrase_len = len(phrase)

        original_phrase = phrase
        if transfer_casing:
            phrase = phrase.lower()

        def early_exit():
            if include_unknown and not suggestions:
                suggestions.append(SuggestItem(phrase, max_edit_distance + 1, 0))
            return suggestions

        # early exit - word is too big to possibly match any words
        if phrase_len - max_edit_distance > self._max_length:
            return early_exit()

        # quick look for exact match
        if lang == "en":
            find_phrase = m_f.m_item_labels().get_wd_qid_en(
                phrase, page_rank=False, get_qid=False
            )
        else:
            find_phrase = m_f.m_item_labels().get_wd_qid_all(
                phrase, page_rank=False, get_qid=False
            )
        if find_phrase:
            suggestion_count = len(find_phrase)
            if transfer_casing:
                suggestions.append(SuggestItem(original_phrase, 0, suggestion_count))
            else:
                suggestions.append(SuggestItem(phrase, 0, suggestion_count))
            # early exit - return exact match, unless caller wants all matches
            if verbosity != Verbosity.ALL:
                return early_exit()

        if ignore_token is not None and re.match(ignore_token, phrase) is not None:
            suggestion_count = 1
            suggestions.append(SuggestItem(phrase, 0, suggestion_count))
            # early exit - return exact match, unless caller wants all matches
            if verbosity != Verbosity.ALL:
                return early_exit()

        # early termination, if we only want to check if word in dictionary or
        # get its frequency e.g. for word segmentation
        if max_edit_distance == 0:
            return early_exit()

        considered_deletes = set()
        considered_suggestions = set()
        considered_suggestions_wd = set()
        # we considered the phrase already in the 'phrase in self._words' above
        considered_suggestions.add(phrase)

        max_edit_distance_2 = max_edit_distance
        candidate_pointer = 0
        candidates = []

        # add original prefix
        phrase_prefix_len = phrase_len
        if phrase_prefix_len > prefix_length:
            phrase_prefix_len = prefix_length
            candidates.append(phrase[:phrase_prefix_len])
        else:
            candidates.append(phrase)
        distance_comparer = EditDistance(self._distance_algorithm)
        combination_k = list(
            accumulate([ul.combination_n_k(phrase_prefix_len, i) for i in range(6)])
        )

        while candidate_pointer < len(candidates):
            if (limit and len(suggestions) >= limit) or (
                verbosity == Verbosity.ALL and len(suggestions) > cf.LIMIT_SEARCH_ES
            ):
                suggestions.sort()
                tmp_max_dis = suggestions[-1].distance
                suggestions_2.extend(
                    [s for s in suggestions if s.distance == tmp_max_dis]
                )
                suggestions = [s for s in suggestions if s.distance < tmp_max_dis]
                suggestions_len = len(suggestions)
                if (
                    suggestions_len != len(suggestions)
                    and (limit and len(suggestions) >= limit)
                    or (
                        verbosity == Verbosity.ALL
                        and len(suggestions) > cf.LIMIT_SEARCH_ES
                    )
                ):
                    break

            candidate = candidates[candidate_pointer]
            candidate_pointer += 1
            candidate_len = len(candidate)
            len_diff = phrase_prefix_len - candidate_len

            # early termination: if candidate distance is already higher than
            # suggestion distance, then there are no better suggestions to be
            # expected
            if len_diff > max_edit_distance_2:
                # skip to next candidate if Verbosity.ALL, look no
                # further if Verbosity.TOP or CLOSEST (candidates are
                # ordered by delete distance, so none are closer than
                # current)
                if verbosity == Verbosity.ALL:  # pragma: no cover
                    # `max_edit_distance_2`` only updated when
                    # verbosity != ALL. New candidates are generated from
                    # deletes so it keeps getting shorter. This should never
                    # be reached.
                    continue
                break  # pragma: no cover, "peephole" optimization, http://bugs.python.org/issue2506

            dict_suggestions = db_deletes.get_delete_candidates(
                candidate,
                phrase_len=phrase_len,
                max_edit_distance=max_edit_distance_2,
                get_label=True,
            )

            if dict_suggestions:
                if not expensive:
                    if suggestions and candidate_pointer > combination_k[1]:
                        break
                    if (
                        candidate_pointer > combination_k[3]
                        or len(considered_suggestions_wd) > 100000
                    ):
                        break
                for suggestion_id, suggestion in dict_suggestions:
                    if suggestion_id in considered_suggestions_wd:
                        continue
                    considered_suggestions_wd.add(suggestion_id)

                    if suggestion == phrase:
                        continue
                    suggestion_len = len(suggestion)
                    # phrase and suggestion lengths diff > allowed/current best
                    # distance
                    if (
                        abs(suggestion_len - phrase_len) > max_edit_distance_2
                        # suggestion must be for a different delete string, in
                        # same bin only because of hash collision
                        or suggestion_len < candidate_len
                        # if suggestion len = delete len, then it either equals
                        # delete or is in same bin only because of hash collision
                        or (suggestion_len == candidate_len and suggestion != candidate)
                    ):
                        continue  # pragma: no cover, "peephole" optimization, http://bugs.python.org/issue2506
                    suggestion_prefix_len = min(suggestion_len, prefix_length)
                    if (
                        suggestion_prefix_len > phrase_prefix_len
                        and suggestion_prefix_len - candidate_len > max_edit_distance_2
                    ):
                        continue
                    # True Damerau-Levenshtein Edit Distance: adjust distance,
                    # if both distances>0. We allow simultaneous edits (deletes)
                    # of max_edit_distance on on both the dictionary and the
                    # phrase term. For replaces and adjacent transposes the
                    # resulting edit distance stays <= max_edit_distance. For
                    # inserts and deletes the resulting edit distance might
                    # exceed max_edit_distance. To prevent suggestions of a
                    # higher edit distance, we need to calculate the resulting
                    # edit distance, if there are simultaneous edits on both
                    # sides. Example: (bank==bnak and bank==bink, but bank!=kanb
                    # and bank!=xban and bank!=baxn for max_edit_distance=1).
                    # Two deletes on each side of a pair makes them all equal,
                    # but the first two pairs have edit distance=1, the others
                    # edit distance=2.
                    distance = 0
                    min_distance = 0
                    if candidate_len == 0:
                        # suggestions which have no common chars with phrase
                        # (phrase_len<=max_edit_distance &&
                        # suggestion_len<=max_edit_distance)
                        distance = max(phrase_len, suggestion_len)
                        if (
                            distance > max_edit_distance_2
                            or suggestion in considered_suggestions
                        ):
                            continue
                    elif suggestion_len == 1:
                        # This should always be phrase_len - 1? Since
                        # suggestions are generated from deletes of the input
                        # phrase
                        distance = (
                            phrase_len
                            if phrase.index(suggestion[0]) < 0
                            else phrase_len - 1
                        )
                        # `suggestion` only gets added to
                        # `considered_suggestions` when `suggestion_len>1`.
                        # Given the max_dictionary_edit_distance and
                        # prefix_length restrictions, `distance`` should never
                        # be >max_edit_distance_2
                        if (
                            distance > max_edit_distance_2
                            or suggestion in considered_suggestions
                        ):  # pragma: no cover
                            continue
                    # number of edits in prefix ==maxeditdistance AND no
                    # identical suffix, then editdistance>max_edit_distance and
                    # no need for Levenshtein calculation
                    # (phraseLen >= prefixLength) &&
                    # (suggestionLen >= prefixLength)
                    else:
                        # handles the shortcircuit of min_distance assignment
                        # when first boolean expression evaluates to False
                        if prefix_length - max_edit_distance == candidate_len:
                            min_distance = (
                                min(phrase_len, suggestion_len) - prefix_length
                            )
                        else:
                            min_distance = 0
                        # pylint: disable=too-many-boolean-expressions
                        if (
                            prefix_length - max_edit_distance == candidate_len
                            and (
                                min_distance > 1
                                and phrase[phrase_len + 1 - min_distance :]
                                != suggestion[suggestion_len + 1 - min_distance :]
                            )
                            or (
                                min_distance > 0
                                and phrase[phrase_len - min_distance]
                                != suggestion[suggestion_len - min_distance]
                                and (
                                    phrase[phrase_len - min_distance - 1]
                                    != suggestion[suggestion_len - min_distance]
                                    or phrase[phrase_len - min_distance]
                                    != suggestion[suggestion_len - min_distance - 1]
                                )
                            )
                        ):
                            continue
                        # delete_in_suggestion_prefix is somewhat expensive, and
                        # only pays off when verbosity is TOP or CLOSEST
                        if suggestion in considered_suggestions:
                            continue
                        considered_suggestions.add(suggestion)
                        distance = distance_comparer.compare(
                            phrase, suggestion, max_edit_distance_2
                        )
                        if distance < 0:
                            continue
                    # do not process higher distances than those already found,
                    # if verbosity<ALL (note: max_edit_distance_2 will always
                    # equal max_edit_distance when Verbosity.ALL)
                    if distance <= max_edit_distance_2:  # pragma: no branch
                        if lang == "en":
                            find_suggestion = m_f.m_item_labels().get_wd_qid_en(
                                suggestion, page_rank=False, get_qid=False
                            )
                        else:
                            find_suggestion = m_f.m_item_labels().get_wd_qid_all(
                                suggestion, page_rank=False, get_qid=False
                            )
                        if find_suggestion:
                            suggestion_count = len(find_suggestion)
                        else:
                            suggestion_count = 0
                        item = SuggestItem(suggestion, distance, suggestion_count)
                        if suggestions:
                            if verbosity == Verbosity.CLOSEST:
                                # we will calculate DamLev distance only to the
                                # smallest found distance so far
                                if distance < max_edit_distance_2:
                                    suggestions = []
                            elif verbosity == Verbosity.TOP:
                                if (  # pragma: no branch, "peephole" optimization, http://bugs.python.org/issue2506
                                    distance < max_edit_distance_2
                                    or suggestion_count > suggestions[0].count
                                ):
                                    max_edit_distance_2 = distance
                                    suggestions[0] = item
                                continue
                        if verbosity != Verbosity.ALL:
                            max_edit_distance_2 = distance
                        suggestions.append(item)
            # add edits: derive edits (deletes) from candidate (phrase) and add
            # them to candidates list. this is a recursive process until the
            # maximum edit distance has been reached
            if len_diff < max_edit_distance and candidate_len <= prefix_length:
                # do not create edits with edit distance smaller than
                # suggestions already found
                if verbosity != Verbosity.ALL and len_diff >= max_edit_distance_2:
                    continue
                for i in range(candidate_len):
                    delete = candidate[:i] + candidate[i + 1 :]
                    if delete not in considered_deletes:
                        considered_deletes.add(delete)
                        candidates.append(delete)
        if len(suggestions) > 1:
            suggestions.sort()

        if transfer_casing:
            suggestions = [
                SuggestItem(
                    helpers.case_transfer_similar(original_phrase, s.term),
                    s.distance,
                    s.count,
                )
                for s in suggestions
            ]

        early_exit()
        return suggestions

    def search_label(self, input_text, limit=0, lang="en", expensive=False):
        # Try with small distance
        def _search_label(
            text,
            verbosity=Verbosity.ALL,
            max_dis=2,
            prefix_length=10,
            db=220,
            lang="en",
            seg=False,
        ):
            res = []
            # Original text
            # if text:
            #     res = self.lookup_mtab(text, verbosity, max_dis=max_dis,
            #                       prefix_length=prefix_length, transfer_casing=True)

            # Normalize with punctuations
            norm_1 = ul.norm_queries(text, punctuations=False, seg=seg)
            if norm_1:
                res = self.lookup_mtab(
                    norm_1,
                    verbosity,
                    max_edit_distance=max_dis,
                    prefix_length=prefix_length,
                    db=db,
                    lang=lang,
                    limit=limit,
                    expensive=expensive,
                )

            return res

        responds = []

        if not responds:
            responds = _search_label(
                input_text,
                verbosity=Verbosity.CLOSEST,
                max_dis=2,
                prefix_length=14,
                db=214,
                lang="all",
            )

        if not responds and lang == "all":
            responds = _search_label(
                input_text,
                verbosity=Verbosity.CLOSEST,
                max_dis=20,
                prefix_length=14,
                db=214,
                lang="all",
            )

        if not responds and lang == "en":
            responds = _search_label(
                input_text,
                verbosity=Verbosity.CLOSEST,
                max_dis=4,
                prefix_length=10,
                db=410,
                lang="en",
            )

        if not responds and lang == "en":
            responds = _search_label(
                input_text,
                verbosity=Verbosity.CLOSEST,
                max_dis=20,
                prefix_length=10,
                db=410,
                lang="en",
            )

        if not responds:
            return []

        if limit == 0:
            limit = len(responds)
        return responds[:limit]

    def search_wd(self, query_text_org, lang="en", limit=0, expensive=False):
        is_wd_id = ul.is_wd_item(query_text_org)
        if is_wd_id:
            return [(query_text_org.upper(), 1)]

        responds_label = defaultdict(float)

        query_text = query_text_org

        if not responds_label:
            responds_label = self.search_label(
                query_text, limit, lang=lang, expensive=expensive
            )

        if not responds_label:
            if "(" in query_text:
                new_query_string = re.sub(r"\((.*)\)", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label = self.search_label(
                        new_query_string, limit, lang=lang, expensive=expensive
                    )

            if "[" in query_text:
                new_query_string = re.sub(r"\[(.*)\]", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label = self.search_label(
                        new_query_string, limit, lang=lang, expensive=expensive
                    )

        if '("' in query_text_org:
            new_query_string = re.search(r"\(\"(.*)\"\)", query_text_org)
            if new_query_string:
                new_query_string = new_query_string.group(1)
                if new_query_string != query_text_org:
                    extra = self.search_label(
                        new_query_string, limit, lang=lang, expensive=expensive
                    )
                    if extra:
                        responds_label_set = {r.term for r in responds_label}
                        for e_i in extra:
                            if e_i.term not in responds_label_set:
                                responds_label.append(e_i)
                                responds_label_set.add(e_i.term)

        if "[" in query_text_org:
            new_query_string = re.sub(r"\[(.*)\]", "", query_text_org)
            if new_query_string != query_text_org:
                extra = self.search_label(
                    new_query_string, limit, lang=lang, expensive=expensive
                )
                if extra:
                    responds_label_set = {r.term for r in responds_label}
                    for e_i in extra:
                        if e_i.term not in responds_label_set:
                            responds_label.append(e_i)
                            responds_label_set.add(e_i.term)

        if "species:" in query_text_org:
            new_query_string = query_text_org.replace("species:", "")
            new_query_string = new_query_string.replace("sub:", "")
            if new_query_string != query_text_org:
                extra = self.search_label(
                    new_query_string, limit, lang=lang, expensive=expensive
                )
                if extra:
                    responds_label_set = {r.term for r in responds_label}
                    for e_i in extra:
                        if e_i.term not in responds_label_set:
                            responds_label.append(e_i)
                            responds_label_set.add(e_i.term)

        responds = defaultdict(float)
        cur_fre = 0
        cur_fre_score = 0
        r_i = 0
        for respond in responds_label:
            # distance:
            res_s = (cf.MAX_EDIT_DISTANCE - respond.distance) / cf.MAX_EDIT_DISTANCE * 9
            if respond.count != cur_fre:
                cur_fre = respond.count
                cur_fre_score = (len(responds_label) - r_i) / len(responds_label)
                r_i += 1
            res_s += cur_fre_score
            responds[respond.term] = res_s / 10.0
            # respond_wds = self._db_labels.get_words(respond.term, page_rank=True)
            # if not respond_wds:
            #     continue
            # for res_wd, prank in respond_wds:
            #     if responds.get(res_wd):
            #         continue
            #     # prank = self._wiki_items.get_pagerank_score(res_wd)
            #     responds[res_wd] = res_s * 10 + prank * cf.WEIGHT_PAGERANK
        # max_score = max(responds.values())
        # if max_score:
        #     responds = {k: (v / max_score) for k, v in responds.items()}

        if not responds:
            return []
        if limit == 0:
            limit = len(responds)
        responds = sorted(responds.items(), key=lambda x: x[1], reverse=True)
        responds = responds[:limit]
        return responds


def test():
    queries = [
        "Big Blue",
        "China",
        "US",
        "Ameriaca",
        "America",
        "Huyn Naông Cn",
        "tokyo",
        "entialarials",
        "famsie",
        "Communism[citation needed]]",
        "titaniu-75",
        "titanium-75",
        "Q18845165",
        "titanium-4",
        "aideakiii akea",
        "Hideaki Takeda",
        "hideaki takeda",
        "hidEAki tAKeda",
        "Град Скопјее",
        "Oregon, OR",
        "Sarrah Mcgloclyn",
        "* TM-88",
        "Zachary Knight Galifianacisss",
        "Hidzki",
        "H. Tjkeda",
        "Colin Rand Kapernikuss",
        "Chuck Palahichikkk",
        "American rapper",
        "Tokio",
        "Phucc Nguyan",
        "Tatyana Kolotilshchikova",
        "T Kolotilshchikova",
        "T. Kolotilshchikova",
        "R. Millar",
        "Tokyo",
        "tokyo",
        "M. Nykänen",
        "Tb10.NT.103",
        "Apaizac beto expeditin",
        "{irconium-83",
        'assassination of"John F. Kennedy',
        "corona japan",
        "SDS J100759.52+102152.8",
        "2MASS J10540655-0031018",
        "6C 124133+40580",
        "2MASS J0343118+6137172",
        "[RMH2016] 011.243560+41.94550",
        "ruthenium-:8",
        "bismuth-393",
        "rubidium-7",
        "Rubidium-7",
        "rUBIdium-7",
        "rubIDIum-7",
        "rUbidIUm-2",
        "neod{mium-133",
        "neodymiwm-155",
        "SDSS J001733.60+0040306",
        "Abstraction Latin Patricia Phelps de Cisneros Collection",
        "Geometric Abstraction: Latin American Art from the Patricia Phelps de Cisneros Collection",
    ]
    start_1 = time()
    c_ok = 0
    for query in queries:
        iw.print_status("\nQuery: %s" % query)
        start = time()
        # responds = fzs.search_wd(query)
        responds, _, _ = m_entity_search.search(query, mode="f", limit=cf.LIMIT_SEARCH)
        iw.print_status(
            f"About {len(responds)} results ({(time() - start):.5f} seconds)"
        )
        responds = m_f.m_items().get_search_info(responds[:10])
        if responds:
            c_ok += 1
        for i, respond in enumerate(responds):
            r_score = respond["score"]
            r_label = respond["label"]
            r_wd = respond["wd"]
            r_des = respond["des"]
            iw.print_status(f"{i + 1}. " f"{r_score:.5f} - {r_wd}[{r_label}] - {r_des}")

    iw.print_status(f"{c_ok}/{len(queries)} - Run time {time() - start_1:.10f} seconds")


if __name__ == "__main__":
    m_f.init()
    test()
#     # 2-20  37/45 - Run time 0.2413361073 seconds
#     # 2-10  36/45 - Run time 5.8603398800 seconds
#     # 10-10 44/45 - Run time 82.7283601761 seconds
#     # 2-20 2-10 10-10 44/45 - Run time 57.2498681545 seconds
#     # 2-20 2-10 10-10 44/45 - 44/45 - Run time 14.6712858677 seconds
#     # 15.9442179203 # 11.9582469463
#     # fzs.gen_deletes()
#     # db = FuzzySearch()
#     # db.update_db(update_from_id=152590998)
#     # db.size_delete_small()
#     # db.size_delete_large()
#     # db.update_database()
#     # db.copy()
#     # db.build()
#     # fz = FuzzySearch()
#     # tmp = fz.search_label("Tokio")
#     # tmp = fz.search_wd("Tokyo", get_label=True)
#     # db.copy()
#     # db.build_deletes()
#     # db.guess_vocab_id()
#     # db.build(buff_size=50000)
