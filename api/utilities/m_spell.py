# import gc
# import re
# from collections import defaultdict, Counter
# from time import time
#
# import lmdb
# import symspellpy.helpers as helpers
# from symspellpy import SuggestItem
# from symspellpy import SymSpell, Verbosity
# from symspellpy.editdistance import EditDistance
# from tqdm import tqdm
#
# import m_config as cf
# from api.lookup.m_entity_fuzzy import MEntityLabels
# from api.resources.m_db_item import DBItem
# from api.utilities import m_io as iw
# from api.utilities import m_utils as ul
#
#
# class SpellCorrectDB(DBItem):
#     def __init__(self, db_file=cf.DIR_SPELL_DB):
#         super().__init__(db_file=db_file, max_db=6)
#         self._db_vocab = self._env.open_db(b'__vocab__')
#         self._db_vocab_inv = self._env.open_db(b'__vocab_inv_')
#         self._buff_int = defaultdict(int)
#         self._len_vocab = self.size_vocab()
#
#         self._db_metadata = self._env.open_db(b'__metadata__')
#         self._max_length = self.get_value(self._db_metadata, "max_length")
#         if self._max_length is None:
#             self._max_length = 0
#
#         self._db_words = self._env.open_db(b'__words__')
#         self._buff_unigram = Counter()
#
#         self._db_bigrams = self._env.open_db(b'__bigrams__')
#         self._buff_bigrams = Counter()
#
#         self._db_deletes = self._env.open_db(b'__deletes__')
#         self._buff_set = defaultdict(set)
#
#     @property
#     def max_length(self):
#         return self._max_length
#
#     def get_words_from_id(self, word_id):
#         return self.get_value(self._db_vocab_inv, word_id)
#
#     def get_words(self, text):
#         return self.get_value(self._db_words, text)
#
#     def get_deletes(self, text):
#         return self.get_value(self._db_deletes, text)
#
#     def get_bigrams(self, text):
#         return self.get_value(self._db_bigrams, text)
#
#     def copy(self):
#         db_names = {b'__metadata__': b'__metadata__',
#                     b'__vocab__': b'__vocab__',
#                     b'__vocab_inv_': b'__vocab_inv_',
#                     b'__words__': b'__words__',
#                     b'__bigrams__': b'__bigrams__',
#                     b'__deletes__': b'__deletes__',
#                     }
#         map_size = cf.SIZE_10GB * 5  # 25 GB
#         self._copy(db_names, map_size)
#
#     def size_vocab(self):
#         return self._get_size(self._db_vocab_inv)
#
#     def get_vocab_id(self, entity_label, is_add=True):
#         v_id = self._buff_int.get(entity_label)
#         if v_id is None:
#             v_id = self.get_value(self._db_vocab, entity_label)
#         if v_id is None and is_add:
#             v_id = self._len_vocab
#             self._buff_int[entity_label] = v_id
#             self._len_vocab += 1
#             if len(self._buff_int) > 1e6:
#                 self.save_buff_vocab()
#         return v_id
#
#     def save_buff_vocab(self, buff_size=1e6):
#         self._write_bulk_with_buffer(
#             env=self._env,
#             db=self._db_vocab,
#             db_inv=self._db_vocab_inv,
#             data=self._buff_int,
#             buff_size=buff_size,
#             is_message=False
#         )
#         self._buff_int.clear()
#         gc.collect()
#
#     def save_buff_obj(self, db, buff, buff_size=1e6, update_type="int"):
#         self.update_bulk_with_buffer(
#             env=self._env,
#             db=db,
#             data=buff,
#             buff_size=buff_size,
#             is_message=True,
#             update_type=update_type,
#             encoder="pickle"
#         )
#         buff.clear()
#         gc.collect()
#
#     def save_max_length(self):
#         self._write_sample(self._db_metadata, "max_length", self._max_length)
#
#     def update_obj(self, _db, _buff, key, value, buff_size, buff_save, update_type):
#         if update_type == "int":
#             _buff[key] += value
#         else:
#             _buff[key].add(value)
#
#         if len(_buff) >= buff_save:
#             self.save_buff_obj(_db, _buff, buff_size, update_type)
#
#     def _build_words(self, buff_size=cf.LMDB_BUFF_SIZE, buff_save=1e7, update_type="int"):
#         _max_length = self._max_length
#
#         w_items = MEntityLabels()
#         p_bar = tqdm(desc=f"words:{self._get_size(self._db_words)}, "
#                           f"bigrams:{self._get_size(self._db_bigrams)}",
#                      total=w_items.size_vocab())
#
#         for i, label in enumerate(w_items.iter_vocab_keys()):
#             p_bar.update()
#             if i and i % 10000 == 0:
#                 p_bar.set_description(
#                     f"words:{self._get_size(self._db_words) + len(self._buff_unigram)}, "
#                     f"bigrams:{self._get_size(self._db_bigrams) + len(self._buff_bigrams)}, "
#                 )
#                 if _max_length != self._max_length:
#                     self.save_max_length()
#                     _max_length = self._max_length
#                 # break
#             # norm_text = ul.norm_text(label, punctuations=False)
#             # if len(norm_text) != label:
#             #     continue
#
#             unigram = ul.get_ngrams(label, n=1)
#             for word, fre in unigram.items():
#                 self.get_vocab_id(word)
#                 if len(word) > self._max_length:
#                     self._max_length = len(word)
#                 self.update_obj(self._db_words, self._buff_unigram,
#                                 word, fre, buff_size, buff_save, update_type)
#
#             bigrams = ul.get_ngrams(label, n=2)
#             for word, fre in bigrams.items():
#                 self.update_obj(self._db_bigrams, self._buff_bigrams,
#                                 word, fre, buff_size, buff_save, update_type)
#
#         p_bar.close()
#         self.save_buff_obj(self._db_words, self._buff_unigram, buff_size, update_type)
#         self.save_buff_obj(self._db_bigrams, self._buff_bigrams, buff_size, update_type)
#         self.save_buff_vocab(buff_size)
#         if _max_length != self._max_length:
#             self.save_max_length()
#
#     def _build_deletes(self, max_dis=6, predix_len=10, min_len=1,
#                        buff_size=cf.LMDB_BUFF_SIZE, buff_save=1e7, update_type="set"):
#         p_bar = tqdm(desc=f"deletes:{self._get_size(self._db_deletes)}",
#                      total=self._get_size(self._db_words))
#
#         for i, (label, label_id) in enumerate(self._db_iter(self._db_vocab)):
#             p_bar.update()
#             # if i < 9826726:
#             #     continue
#             if i and i % 100 == 0:
#                 p_bar.set_description(
#                     f"deletes:{self._get_size(self._db_deletes)}"
#                     f", buff:{len(self._buff_set)}"
#                 )
#             edits = ul.delete_edits_prefix(label, max_dis, predix_len, min_len)
#             for delete in edits:
#                 if not delete:
#                     continue
#                 self.update_obj(self._db_deletes, self._buff_set, delete,
#                                 label_id, buff_size, buff_save, update_type)
#
#         p_bar.close()
#         self.save_buff_obj(self._db_deletes, self._buff_set, buff_size, update_type)
#
#     def build(self):
#         # self._build_words(buff_size=1e6, buff_save=1e7)
#         self._build_deletes(max_dis=4, predix_len=7,
#                             min_len=1, buff_size=1e6, buff_save=1e7)
#
#
# class SpellCorrect(SymSpell):
#     def __init__(self, max_dictionary_edit_distance=6, prefix_length=10,
#                  count_threshold=1):
#         super().__init__(max_dictionary_edit_distance, prefix_length,
#                          count_threshold)
#         self.db = SpellCorrectDB()
#
#     def lookup(self, phrase, verbosity, max_edit_distance=None,
#                include_unknown=False, ignore_token=None,
#                transfer_casing=False):
#         """Find suggested spellings for a given phrase word.
#
#         Parameters
#         ----------
#         phrase : str
#             The word being spell checked.
#         verbosity : :class:`Verbosity`
#             The value controlling the quantity/closeness of the
#             returned suggestions.
#         max_edit_distance : int, optional
#             The maximum edit distance between phrase and suggested
#             words. Set to :attr:`_max_dictionary_edit_distance` by
#             default
#         include_unknown : bool, optional
#             A flag to determine whether to include phrase word in
#             suggestions, if no words within edit distance found.
#         ignore_token : regex pattern, optional
#             A regex pattern describing what words/phrases to ignore and
#             leave unchanged
#         transfer_casing : bool, optional
#             A flag to determine whether the casing --- i.e., uppercase
#             vs lowercase --- should be carried over from `phrase`.
#
#         Returns
#         -------
#         suggestions : list
#             suggestions is a list of :class:`SuggestItem` objects
#             representing suggested correct spellings for the phrase
#             word, sorted by edit distance, and secondarily by count
#             frequency.
#
#         Raises
#         ------
#         ValueError
#             If `max_edit_distance` is greater than
#             :attr:`_max_dictionary_edit_distance`
#         """
#         if max_edit_distance is None:
#             max_edit_distance = self._max_dictionary_edit_distance
#         if max_edit_distance > self._max_dictionary_edit_distance:
#             raise ValueError("Distance too large")
#         suggestions = list()
#         phrase_len = len(phrase)
#
#         if transfer_casing:
#             original_phrase = phrase
#             phrase = phrase.lower()
#
#         def early_exit():
#             if include_unknown and not suggestions:
#                 suggestions.append(
#                     SuggestItem(phrase, max_edit_distance + 1, 0)
#                 )
#             return suggestions
#         # early exit - word is too big to possibly match any words
#         if phrase_len - max_edit_distance > self.db.max_length:
#             return early_exit()
#
#         # quick look for exact match
#         suggestion_count = 0
#         check_words = self.db.get_words(phrase)
#         if check_words:
#             suggestion_count = check_words
#         # if phrase in self._words:
#         #     suggestion_count = self._words[phrase]
#             if transfer_casing:
#                 suggestions.append(
#                     SuggestItem(original_phrase, 0, suggestion_count)
#                 )
#             else:
#                 suggestions.append(
#                     SuggestItem(phrase, 0, suggestion_count)
#                 )
#             # early exit - return exact match, unless caller wants all matches
#             if verbosity != Verbosity.ALL:
#                 return early_exit()
#
#         if ignore_token is not None and re.match(ignore_token, phrase) is not None:
#             suggestion_count = 1
#             suggestions.append(
#                 SuggestItem(phrase, 0, suggestion_count)
#             )
#             # early exit - return exact match, unless caller wants all matches
#             if verbosity != Verbosity.ALL:
#                 return early_exit()
#
#         # early termination, if we only want to check if word in dictionary or get its frequency e.g. for word segmentation
#         if max_edit_distance == 0:
#             return early_exit()
#
#         considered_deletes = set()
#         considered_suggestions = set()
#         # we considered the phrase already in the 'phrase in self._words' above
#         considered_suggestions.add(phrase)
#
#         max_edit_distance_2 = max_edit_distance
#         candidate_pointer = 0
#         candidates = list()
#
#         # add original prefix
#         phrase_prefix_len = phrase_len
#         if phrase_prefix_len > self._prefix_length:
#             phrase_prefix_len = self._prefix_length
#             candidates.append(phrase[: phrase_prefix_len])
#         else:
#             candidates.append(phrase)
#         distance_comparer = EditDistance(self._distance_algorithm)
#         while candidate_pointer < len(candidates):
#             candidate = candidates[candidate_pointer]
#             candidate_pointer += 1
#             candidate_len = len(candidate)
#             len_diff = phrase_prefix_len - candidate_len
#
#             # early termination: if candidate distance is already higher than suggestion distance, than there are no better suggestions to be expected
#             if len_diff > max_edit_distance_2:
#                 # skip to next candidate if Verbosity.ALL, look no further if Verbosity.TOP or CLOSEST (candidates are ordered by delete distance, so none are closer than current)
#                 if verbosity == Verbosity.ALL:
#                     continue
#                 break
#
#             check_deletes = self.db.get_deletes(candidate)
#             if check_deletes:
#                 dict_suggestions = check_deletes
#             # if candidate in self._deletes:
#             #     dict_suggestions = self._deletes[candidate]
#                 for suggestion_id in dict_suggestions:
#                     suggestion = self.db.get_words_from_id(suggestion_id)
#                     if suggestion == phrase:
#                         continue
#                     suggestion_len = len(suggestion)
#                     # phrase and suggestion lengths diff > allowed/current best distance
#                     if (
#                             abs(suggestion_len - phrase_len) > max_edit_distance_2
#                             # suggestion must be for a different delete string, in same bin only because of hash collision
#                             or suggestion_len < candidate_len
#                             # if suggestion len = delete len, then it either equals delete or is in same bin only because of hash collision
#                             or (
#                                 suggestion_len == candidate_len
#                                 and suggestion != candidate
#                             )
#                     ):
#                         continue
#                     suggestion_prefix_len = min(suggestion_len,
#                                                 self._prefix_length)
#                     if (
#                             suggestion_prefix_len > phrase_prefix_len
#                             and suggestion_prefix_len - candidate_len > max_edit_distance_2
#                     ):
#                         continue
#                     # True Damerau-Levenshtein Edit Distance: adjust distance, if both distances>0 We allow simultaneous edits (deletes) of max_edit_distance on on both the dictionary and the phrase term. For replaces and adjacent transposes the resulting edit distance stays <= max_edit_distance. For inserts and deletes the resulting edit distance might exceed max_edit_distance. To prevent suggestions of a higher edit distance, we need to calculate the resulting edit distance, if there are simultaneous edits on both sides. Example: (bank==bnak and bank==bink, but bank!=kanb and bank!=xban and bank!=baxn for max_edit_distance=1). Two deletes on each side of a pair makes them all equal, but the first two pairs have edit distance=1, the others edit distance=2.
#                     distance = 0
#                     min_distance = 0
#                     if candidate_len == 0:
#                         # suggestions which have no common chars with phrase (phrase_len<=max_edit_distance && suggestion_len<=max_edit_distance)
#                         distance = max(phrase_len, suggestion_len)
#                         if (
#                                 distance > max_edit_distance_2
#                                 or suggestion in considered_suggestions
#                         ):
#                             continue
#                     elif suggestion_len == 1:
#                         distance = phrase_len
#                         if phrase.index(suggestion[0]) < 0:
#                             distance = phrase_len - 1
#                         if (
#                                 distance > max_edit_distance_2
#                                 or suggestion in considered_suggestions
#                         ):
#                             continue
#                     # number of edits in prefix ==maxediddistance AND
#                     # no identical suffix, then
#                     # editdistance>max_edit_distance and no need for
#                     # Levenshtein calculation
#                     # (phraseLen >= prefixLength) &&
#                     # (suggestionLen >= prefixLength)
#                     else:
#                         # handles the shortcircuit of min_distance
#                         # assignment when first boolean expression
#                         # evaluates to False
#                         if self._prefix_length - max_edit_distance == candidate_len:
#                             min_distance = (min(phrase_len, suggestion_len) -
#                                             self._prefix_length)
#                         else:
#                             min_distance = 0
#                         # pylint: disable=C0301,R0916
#                         if (self._prefix_length - max_edit_distance == candidate_len
#                                 and (min_distance > 1
#                                      and phrase[phrase_len + 1 - min_distance :] != suggestion[suggestion_len + 1 - min_distance :])
#                                 or (min_distance > 0
#                                     and phrase[phrase_len - min_distance] != suggestion[suggestion_len - min_distance]
#                                     and (phrase[phrase_len - min_distance - 1] != suggestion[suggestion_len - min_distance]
#                                          or phrase[phrase_len - min_distance] != suggestion[suggestion_len - min_distance - 1]))):
#                             continue
#                         else:
#                             # delete_in_suggestion_prefix is somewhat
#                             # expensive, and only pays off when
#                             # verbosity is TOP or CLOSEST
#                             if suggestion in considered_suggestions:
#                                 continue
#                             considered_suggestions.add(suggestion)
#                             distance = distance_comparer.compare(
#                                 phrase, suggestion, max_edit_distance_2)
#                             if distance < 0:
#                                 continue
#                     # do not process higher distances than those
#                     # already found, if verbosity<ALL (note:
#                     # max_edit_distance_2 will always equal
#                     # max_edit_distance when Verbosity.ALL)
#                     if distance <= max_edit_distance_2:
#                         suggestion_count = self.db.get_words(suggestion)
#                         # suggestion_count = self._words[suggestion]
#                         si = SuggestItem(suggestion, distance, suggestion_count)
#                         if suggestions:
#                             if verbosity == Verbosity.CLOSEST:
#                                 # we will calculate DamLev distance
#                                 # only to the smallest found distance
#                                 # so far
#                                 if distance < max_edit_distance_2:
#                                     suggestions = list()
#                             elif verbosity == Verbosity.TOP:
#                                 if (distance < max_edit_distance_2
#                                         or suggestion_count > suggestions[0].count):
#                                     max_edit_distance_2 = distance
#                                     suggestions[0] = si
#                                 continue
#                         if verbosity != Verbosity.ALL:
#                             max_edit_distance_2 = distance
#                         suggestions.append(si)
#             # add edits: derive edits (deletes) from candidate (phrase)
#             # and add them to candidates list. this is a recursive
#             # process until the maximum edit distance has been reached
#             if (len_diff < max_edit_distance
#                     and candidate_len <= self._prefix_length):
#                 # do not create edits with edit distance smaller than
#                 # suggestions already found
#                 if (verbosity != Verbosity.ALL
#                         and len_diff >= max_edit_distance_2):
#                     continue
#                 for i in range(candidate_len):
#                     delete = candidate[: i] + candidate[i + 1 :]
#                     if delete not in considered_deletes:
#                         considered_deletes.add(delete)
#                         candidates.append(delete)
#         if len(suggestions) > 1:
#             suggestions.sort()
#
#         if transfer_casing:
#             suggestions = [SuggestItem(
#                 helpers.transfer_casing_for_similar_text(original_phrase,
#                                                          s.term),
#                 s.distance, s.count) for s in suggestions]
#
#         early_exit()
#         return suggestions
#
#     def lookup_compound(self, phrase, max_edit_distance,
#                         ignore_non_words=False, transfer_casing=False,
#                         split_phrase_by_space=False,
#                         ignore_term_with_digits=False):
#         phrase = ul.norm_text(phrase, punctuations=False)
#         # Parse input string into single terms
#         term_list_1 = helpers.parse_words(
#             phrase,
#             split_by_space=split_phrase_by_space
#         )
#         # Second list of single terms with preserved cases so we can
#         # ignore acronyms (all cap words)
#         if ignore_non_words:
#             term_list_2 = helpers.parse_words(
#                 phrase,
#                 preserve_case=True,
#                 split_by_space=split_phrase_by_space
#             )
#         suggestions = list()
#         suggestion_parts = list()
#         distance_comparer = EditDistance(self._distance_algorithm)
#
#         # translate every item to its best suggestion, otherwise it
#         # remains unchanged
#         is_last_combi = False
#         for i, __ in enumerate(term_list_1):
#             if ignore_non_words:
#                 if helpers.try_parse_int64(term_list_1[i]) is not None:
#                     suggestion_parts.append(SuggestItem(term_list_1[i], 0, 0))
#                     continue
#                 if helpers.is_acronym(
#                         term_list_2[i],
#                         match_any_term_with_digits=ignore_term_with_digits
#                 ):
#                     suggestion_parts.append(SuggestItem(term_list_2[i], 0, 0))
#                     continue
#             suggestions = self.lookup(
#                 term_list_1[i],
#                 Verbosity.TOP,
#                 max_edit_distance
#             )
#             # combi check, always before split
#             if i > 0 and not is_last_combi:
#                 suggestions_combi = self.lookup(
#                     term_list_1[i - 1] + term_list_1[i],
#                     Verbosity.TOP,
#                     max_edit_distance
#                 )
#                 if suggestions_combi:
#                     best_1 = suggestion_parts[-1]
#                     if suggestions:
#                         best_2 = suggestions[0]
#                     else:
#                         # estimated word occurrence probability
#                         # P=10 / (N * 10^word length l)
#                         best_2 = SuggestItem(
#                             term_list_1[i],
#                             max_edit_distance + 1,
#                             10 // 10 ** len(term_list_1[i])
#                         )
#                     # distance_1=edit distance between 2 split terms and
#                     # their best corrections : als comparative value
#                     # for the combination
#                     distance_1 = best_1.distance + best_2.distance
#                     if (distance_1 >= 0
#                             and (suggestions_combi[0].distance + 1 < distance_1
#                                  or (suggestions_combi[0].distance + 1 == distance_1
#                                      and (suggestions_combi[0].count > best_1.count / self.N * best_2.count)))):
#                         suggestions_combi[0].distance += 1
#                         suggestion_parts[-1] = suggestions_combi[0]
#                         is_last_combi = True
#                         continue
#             is_last_combi = False
#
#             # alway split terms without suggestion / never split terms
#             # with suggestion ed=0 / never split single char terms
#             if suggestions and (suggestions[0].distance == 0
#                                 or len(term_list_1[i]) == 1):
#                 # choose best suggestion
#                 suggestion_parts.append(suggestions[0])
#             else:
#                 # if no perfect suggestion, split word into pairs
#                 suggestion_split_best = None
#                 # add original term
#                 if suggestions:
#                     suggestion_split_best = suggestions[0]
#                 if len(term_list_1[i]) > 1:
#                     for j in range(1, len(term_list_1[i])):
#                         part_1 = term_list_1[i][: j]
#                         part_2 = term_list_1[i][j:]
#                         suggestions_1 = self.lookup(part_1, Verbosity.TOP,
#                                                     max_edit_distance)
#                         if suggestions_1:
#                             suggestions_2 = self.lookup(part_2, Verbosity.TOP,
#                                                         max_edit_distance)
#                             if suggestions_2:
#                                 # select best suggestion for split pair
#                                 tmp_term = (suggestions_1[0].term + " " +
#                                             suggestions_2[0].term)
#                                 tmp_distance = distance_comparer.compare(
#                                     term_list_1[i], tmp_term,
#                                     max_edit_distance)
#                                 if tmp_distance < 0:
#                                     tmp_distance = max_edit_distance + 1
#                                 if suggestion_split_best is not None:
#                                     if tmp_distance > suggestion_split_best.distance:
#                                         continue
#                                     if tmp_distance < suggestion_split_best.distance:
#                                         suggestion_split_best = None
#                                 check_bigrams = self.db.get_bigrams(tmp_term)
#                                 if check_bigrams:
#                                     tmp_count = check_bigrams
#                                 # if tmp_term in self._bigrams:
#                                 #     tmp_count = self._bigrams[tmp_term]
#                                     # increase count, if split
#                                     # corrections are part of or
#                                     # identical to input single term
#                                     # correction exists
#                                     if suggestions:
#                                         best_si = suggestions[0]
#                                         # alternatively remove the
#                                         # single term from
#                                         # suggestion_split, but then
#                                         # other splittings could win
#                                         if suggestions_1[0].term + suggestions_2[0].term == term_list_1[i]:
#                                             # make count bigger than
#                                             # count of single term
#                                             # correction
#                                             tmp_count = max(tmp_count,
#                                                             best_si.count + 2)
#                                         elif (suggestions_1[0].term == best_si.term
#                                               or suggestions_2[0].term == best_si.term):
#                                             # make count bigger than
#                                             # count of single term
#                                             # correction
#                                             tmp_count = max(tmp_count,
#                                                             best_si.count + 1)
#                                     # no single term correction exists
#                                     elif suggestions_1[0].term + suggestions_2[0].term == term_list_1[i]:
#                                         tmp_count = max(
#                                             tmp_count,
#                                             max(suggestions_1[0].count,
#                                                 suggestions_2[0].count) + 2)
#                                 else:
#                                     # The Naive Bayes probability of
#                                     # the word combination is the
#                                     # product of the two word
#                                     # probabilities: P(AB)=P(A)*P(B)
#                                     # use it to estimate the frequency
#                                     # count of the combination, which
#                                     # then is used to rank/select the
#                                     # best splitting variant
#                                     tmp_count = min(
#                                         self.bigram_count_min,
#                                         int(suggestions_1[0].count /
#                                             self.N * suggestions_2[0].count))
#                                 suggestion_split = SuggestItem(
#                                     tmp_term, tmp_distance, tmp_count)
#                                 if (suggestion_split_best is None or
#                                         suggestion_split.count > suggestion_split_best.count):
#                                     suggestion_split_best = suggestion_split
#
#                     if suggestion_split_best is not None:
#                         # select best suggestion for split pair
#                         suggestion_parts.append(suggestion_split_best)
#                         self._replaced_words[term_list_1[i]] = suggestion_split_best
#                     else:
#                         si = SuggestItem(term_list_1[i],
#                                          max_edit_distance + 1,
#                                          int(10 / 10 ** len(term_list_1[i])))
#                         suggestion_parts.append(si)
#                         self._replaced_words[term_list_1[i]] = si
#                 else:
#                     # estimated word occurrence probability
#                     # P=10 / (N * 10^word length l)
#                     si = SuggestItem(term_list_1[i], max_edit_distance + 1,
#                                      int(10 / 10 ** len(term_list_1[i])))
#                     suggestion_parts.append(si)
#                     self._replaced_words[term_list_1[i]] = si
#         joined_term = ""
#         joined_count = self.N
#         for si in suggestion_parts:
#             joined_term += si.term + " "
#             joined_count *= si.count / self.N
#         joined_term = joined_term.rstrip()
#         if transfer_casing:
#             joined_term = helpers.transfer_casing_for_similar_text(phrase,
#                                                                    joined_term)
#         suggestion = SuggestItem(joined_term,
#                                  distance_comparer.compare(
#                                      phrase, joined_term, 2 ** 31 - 1),
#                                  int(joined_count))
#         suggestions_line = list()
#         suggestions_line.append(suggestion)
#
#         return suggestions_line
#
#
# def test():
#     fzs = SpellCorrect()
#     queries = [
#         # "Amazzzzan Prime",
#         # "Град Скопјее",
#         # "Oregon, OR",
#         # "Sarrah Mcgloclyn",
#         # "* TM-88",
#         # "Zachary Knight Galifianacisss",
#         # "Hedeki Tjkeda",
#         # "Colin Rand Kapernikuss",
#         # "Chuck Palahichikkk",
#         # "American rapper",
#         # "Tokio",
#         # "Phucc Nguyan",
#         # "Tatyana Kolotilshchikova",
#         # "T Kolotilshchikova",
#         # "T. Kolotilshchikova",
#         # "R. Millar",
#         # "Tokyo",
#         # "M. Nykänen",
#         # "Tb10.NT.103",
#         # "Apaizac beto expeditin",
#         # "{irconium-83",
#         # "assassination of\"John F. Kennedy",
#         # "SDS J100759.52+102152.8",
#         # "2MASS J10540655-0031018",
#         # "6C 124133+40580",
#         # "2MASS J0343118+6137172",
#         # "[RMH2016] 011.243560+41.94550",
#         # "ruthenium-:8",
#         # "bismuth-393",
#         # "rubidium-7",
#         # "rubidium-2",
#         # "neod{mium-133",
#         # "neodymiwm-155",
#         # "SDSS J001733.60+0040306",
#         # "Abstraction Latin Patricia Phelps de Cisneros Collection",
#         "informationretrieval"
#     ]
#     for query in queries:
#         iw.print_status("\nQuery: %s" % query)
#         start = time()
#         responds = fzs.lookup_compound(query, max_edit_distance=0)
#         iw.print_status(f"About {len(responds)} results ({(time() - start):.2f} seconds)")
#         for i, respond in enumerate(responds[:10]):
#             iw.print_status(f"{i + 1}. "
#                             f"{respond.distance} - {respond.count} - {respond.term}")
#
#
# if __name__ == '__main__':
#     db = SpellCorrectDB()
#     # db.build()
#     # db.copy()
#     test()
# from symspellpy import SymSpell
#
#
# class SpellCorrect(SymSpell):
#     def __init__(self, max_dictionary_edit_distance=10, prefix_length=12,
#                  count_threshold=1):
#         super().__init__(max_dictionary_edit_distance, prefix_length,
#                          count_threshold)
#         self.db = FuzzySearchDB()
#         self.wiki_items = MItem()
#         self._max_length = 0
#
#     def __init__(self):
#         self._model = SymSpell(
#             max_dictionary_edit_distance=2,
#             prefix_length=7
#         )
#         default_uni_gram = pkg_resources.resource_filename(
#             "symspellpy",
#             "frequency_dictionary_en_82_765.txt"
#         )
#         default_bi_gram = pkg_resources.resource_filename(
#             "symspellpy",
#             "frequency_bigramdictionary_en_243_342.txt"
#         )
#         self._model.load_dictionary(
#             default_uni_gram,
#             term_index=0,
#             count_index=1
#         )
#         self._model.load_bigram_dictionary(
#             default_bi_gram,
#             term_index=0,
#             count_index=2
#         )
#
#         # self._model.load_dictionary(st.DIR_UNI_GRAMS, term_index=0, count_index=1)
#         # self._model.save_pickle(st.DIR_SYM_SPELL, compressed=False)
#
#         # self._model.load_pickle(st.DIR_SYM_SPELL, compressed=False)
#         # self._model.load_bigram_dictionary(st.DIR_BI_GRAMS, term_index=0, count_index=2)
#
#     def search(self, text, distance=2):
#         if text:
#             responds = self._model.lookup(text,
#                                           Verbosity.CLOSEST,
#                                           max_edit_distance=distance)
#             responds = [respond.term for respond in responds]
#             return responds
#         else:
#             return text
#
#     def check(self, text, distance=2):
#         result = text
#         if len(text):
#             y = self._model.lookup_compound(
#                 text,
#                 max_edit_distance=distance,
#                 transfer_casing=True
#             )
#             if y and len(y):
#                 result = y[0].term
#         return result
#
#     def word_seg(self, text, max_distance=0):
#         result = self._model.word_segmentation(text, max_distance)
#
#         if result and hasattr(result, "segmented_string"):
#             result = result.segmented_string
#         else:
#             result = text
#         return result
#
#     def get_expansion(self, text):
#         queries = set()
#         _text = ul.remove_3_duplicate_letters(text)
#         queries.add(_text)
#         for i in range(3):
#             queries.add(self.word_seg(_text, max_distance=i))
#         return queries
#
#
# if __name__ == '__main__':
#     # f.init()
#     sc = SpellCorrect()
#
#     sc.search("student")
#     print(sc.word_seg("thesilveeeeeeeeeeer25"))
#     print(sc.word_seg("Grammaticalerrorsareofmanydifferenttypes,includingarticlesordeterminers,prepositions,nounform,verbform,subject-verbagreement,pronouns,wordchoice,sentencestructure,punctuation,capitalization,etc.Ofalltheerrortypes,determinersandprepositionsareamongthemostfrequenterrorsmadebylearnersofEnglish.[1].Thefocusofthisprojectistocorrecterrorsinspellings,determiners,prepositions&actionverbsusingBERTasalanguagerepresentationmodel."))
#     print(sc.check("11th legislatureof Spain", distance=1))
#     print(sc.word_seg("DJ Sorryyouwastedyourmoneytobehere "))
#
#     print(sc.check("SDSS J113819.08+495740.8"))
#     print(sc.check("silver-25", distance=1))
#     print(sc.check("silver-25", distance=2))
#     print(sc.check("thulkum-150", distance=1))
#     print(sc.check("thulkum-150", distance=2))
#     print(sc.check("thuliwm-167", distance=1))
#     print(sc.check("thulkum-150", distance=2))
#     print(sc.check("washigton stateuniversity"))
import re

from symspellpy import SymSpell
import pkg_resources
from api.utilities import m_utils as ul


class SpellCorrect(object):
    def __init__(self):
        self._model = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
        default_uni_gram = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt")
        default_bi_gram = pkg_resources.resource_filename("symspellpy", "frequency_bigramdictionary_en_243_342.txt")
        self._model.load_dictionary(default_uni_gram, term_index=0, count_index=1)
        self._model.load_bigram_dictionary(default_bi_gram, term_index=0, count_index=2)

    def check(self, text, distance=2):
        result = text
        if len(text):
            y = self._model.lookup_compound(text, max_edit_distance=distance, transfer_casing=True)
            if y and len(y):
                result = y[0].term
        return result

    def word_seg(self, text, max_distance=0):
        text = re.sub(r"(\w)\1\1+", r'\1\1', text)
        result = self._model.word_segmentation(text, max_distance)

        if result and hasattr(result, "segmented_string"):
            result = result.segmented_string
        else:
            result = text
        return result


if __name__ == '__main__':
    # f.init()
    sc = SpellCorrect()
    print(sc.word_seg("informationretrieval"))
    # print(sc.word_seg("DJ Sorryyouwastedyourmoneytobehere "))

    # print(sc.check("SDSS J113819.08+495740.8"))
    # print(sc.check("silver-25", distance=1))
    # print(sc.check("silver-25", distance=2))
    # print(sc.check("thulkum-150", distance=1))
    # print(sc.check("thulkum-150", distance=2))
    # print(sc.check("thuliwm-167", distance=1))
    # print(sc.check("thulkum-150", distance=2))
    # print(sc.check("washigton stateuniversity"))
