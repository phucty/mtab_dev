import logging
import m_config as cf
from api.utilities import m_io as iw
from api.utilities.m_spell import SpellCorrect
from collections import defaultdict


def init(is_log=False):
    if is_log:
        iw.create_dir(cf.DIR_LOG)
        logging.basicConfig(
            filename=cf.DIR_LOG, format="%(message)s", level=logging.INFO
        )

    cf.m_mapper = None
    cf.m_corrector = None
    cf.m_wiki_items = None
    cf.m_search_e = None
    cf.m_search_f = None
    cf.m_search_s = None
    cf.m_item_labels = None

    cf.m_wikidata_nt = None
    cf.m_wikidata_json = None
    cf.m_entity_db_multilingual = None
    cf.pre_lk = None
    cf.err_lk = None
    cf.lang_detector = None
    cf.m_spacy = None

    cf.encoding_detector = None
    _pre_load()


def _pre_load():
    # m_items()
    m_mapper()
    # m_spacy()
    # m_entity_db()
    # m_entity_db_multilingual()
    # m_search_f()
    # m_search_e()
    # encoding_detector()
    iw.print_status("MTab: Tabular Annotation with Knowledge Graphs")


def load_pagerank_stats():
    stats = iw.load_obj_pkl(cf.DIR_WIKI_PAGERANK_STATS)
    cf.WEIGHT_PR_STD = stats["std"]
    cf.WEIGHT_PR_MEAN = stats["mean"]
    cf.WEIGHT_PR_MAX = stats["max"]
    cf.WEIGHT_PR_MIN = stats["min"]
    cf.WEIGHT_PR_DIV = cf.WEIGHT_PR_MAX - cf.WEIGHT_PR_MIN
    cf.WEIGHT_PR_MEAN_RATIO = (cf.WEIGHT_PR_MEAN - cf.WEIGHT_PR_MIN) / cf.WEIGHT_PR_DIV


def encoding_detector():
    if cf.encoding_detector is None:
        import charamel

        cf.encoding_detector = charamel.Detector()
    return cf.encoding_detector


def m_spacy():
    if cf.m_spacy is None:
        from api.annotator.m_spacy import MSpacy

        cf.m_spacy = MSpacy()
    return cf.m_spacy


def lang_detector():
    if cf.lang_detector is None:
        from api.utilities.m_lang_detector import LangDetector

        cf.lang_detector = LangDetector()
    return cf.lang_detector


def pre_lk():
    if cf.pre_lk is None:
        cf.pre_lk = {method: defaultdict() for method in ["a", "b", "f"]}
    return cf.pre_lk


def err_lk():
    if not cf.err_lk:
        cf.err_lk = defaultdict()
    return cf.err_lk


def m_wikidata_nt():
    if not cf.m_wikidata_nt:
        from api.resources.m_parser_wikidata import WDItem

        cf.m_wikidata_nt = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_NT)
    return cf.m_wikidata_nt


def m_wikidata_json():
    if not cf.m_wikidata_json:
        from api.resources.m_parser_wikidata import WDItem

        cf.m_wikidata_json = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_JSON)
    return cf.m_wikidata_json


def m_search_s():
    if not cf.m_search_s:
        # from api.lookup.m_entity_fuzzy import FuzzySearchDB
        # cf.m_entity_db = FuzzySearchDB()
        debug = 1
    return cf.m_search_s


def m_item_labels():
    if not cf.m_item_labels:
        from api.resources.m_item_labels import MEntityLabels

        cf.m_item_labels = MEntityLabels(db_file=cf.DIR_WIKI_LABELS)
    return cf.m_item_labels


def m_search_e():
    if not cf.m_search_e:
        from api.lookup.m_entity_bm25 import ESearch

        cf.m_search_e = ESearch()
    return cf.m_search_e


def m_search_f():
    if not cf.m_search_f:
        from api.lookup.m_entity_fuzzy import FuzzySearch

        cf.m_search_f = FuzzySearch()
    return cf.m_search_f


def m_items():
    if not cf.m_wiki_items:
        from api.resources.m_item import MItem

        cf.m_wiki_items = MItem()
    return cf.m_wiki_items


def m_mapper():
    if not cf.m_mapper:
        from api.resources.m_mapping_id import MappingID

        cf.m_mapper = MappingID()
    return cf.m_mapper


def m_corrector():
    if not cf.m_corrector:
        # from api.resources.m_mapping_id import MappingID
        cf.m_corrector = SpellCorrect()
    return cf.m_corrector
