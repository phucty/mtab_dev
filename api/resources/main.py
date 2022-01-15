from datetime import timedelta
from time import time

from api import m_f
from api.lookup.m_entity_bm25 import ESearch
from api.resources.m_wikigraph import WikidataGraph
from api.resources.m_item import MItem
from api.resources.m_item_labels import MEntityLabels
from api.resources.m_mapping_id import MappingID
from api.resources.m_parser_dbpedia import DPItem
import m_config as cf
from api.resources.m_parser_wikidata import WDItem, WDDumpReader
from api.resources.m_parser_wikipedia import WPItem, WPDumpReader
from api.utilities import m_io as iw


def build_deletes(
    db_name, max_dis, prefix_len, update_from_id=0, update_to_id=None, batch_id=0
):
    print(
        f"{db_name}: distance = {max_dis}, predix = {prefix_len}, [{update_from_id}, {update_to_id}], batch = {batch_id}"
    )
    if db_name == "english":
        lang = cf.DIR_WIKI_LABELS_ENGLISH
    else:
        lang = cf.DIR_WIKI_LABELS_MULTILINGUAL

    fz_db = MEntityLabels(lang)
    fz_db.build_deletes(
        db_name=db_name,
        source="id",
        update_from_id=update_from_id,
        update_to_id=update_to_id,
        batch_id=batch_id,
        max_dis=max_dis,
        prefix_len=prefix_len,
        min_len=1,
    )


if __name__ == "__main__":
    start = time()
    # Resources
    # 1. Extract redirects, and mapping between Wikidata, Wikipedia, DBpedia
    build_obj = MappingID()
    build_obj.build()

    # 2. Extract dumps
    # 2.1 DBpedia
    build_obj = DPItem(cf.DIR_DBPEDIA_ITEMS)
    build_obj.build()

    # 2.2 Wikipedia
    build_obj = WPItem()
    build_obj.build(WPDumpReader(cf.DIR_DUMP_WP))

    # 2.3 Wikidata
    build_obj = WDItem(cf.DIR_WIKIDATA_ITEMS_JSON)
    build_obj.build_from_json_dump(WDDumpReader(cf.DIR_DUMP_WD))

    # 3. Build local Graph
    # 3.1. Build WikiGraph
    build_obj = MItem()
    build_obj.build()

    # 4. Entity Labels
    m_f.init()
    fz_db = MEntityLabels(cf.DIR_WIKI_LABELS)
    fz_db.build()

    # 5. Index
    # 5.1. Build edit distance database
    build_deletes("english", max_dis=2, prefix_len=14)
    build_deletes("english", max_dis=4, prefix_len=10)
    build_deletes("multilingual", max_dis=4, prefix_len=10)

    # 5.2. Elastic Search
    build_obj = ESearch()
    build_obj.build()

    iw.print_status(f"MTab build time: {timedelta(seconds=time() - start)}")
