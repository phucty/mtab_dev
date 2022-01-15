from collections import defaultdict

from api import m_f, m_setting as st
from api.lookup.m_entity_bm25 import ESearch
from api.resources.m_db_item import DBDeletes, DBItem, DBItemDefault
from api.resources.m_item_labels import MEntityLabels
from api.resources.m_mapping_id import MappingID
from api.resources.m_parser_dbpedia import DPItem
from api.resources.m_parser_wikidata import WDItem, WDDumpReader
from api.resources.m_item import MItem
from api.lookup.m_entity_fuzzy import FuzzySearch
from tqdm import tqdm
import api.utilities.m_io as iw
from api.resources.m_wikigraph import WikidataGraph
import m_config as cf
from time import time
from api.semtab import m_entity_search
from api.utilities import m_iw


def check_overlapping_wd_db():
    """
    Calculate the overlapping between Wikidata and DBpedia
    """
    mapping = MappingID()
    wd_items = WDItem()
    dp_items = DPItem()
    # Wikidata -> DBpedia mapping
    # No consider redirect of DBpedia
    c_wd = 0
    c_wd_map = 0
    with tqdm(desc=f"Wikidata: {c_wd_map}/{c_wd} mapping") as p_bar:
        for k in wd_items.keys():
            p_bar.update()
            c_wd += 1
            mapping_db = mapping.get_dbpedia_from_wikidata(k)
            if mapping_db:
                p_bar.set_description(f"Wikidata: {c_wd_map}/{c_wd} mapping")
                c_wd_map += 1

    # DBpedia -> Wikidata mapping
    # Consider redirect of DBpedia
    c_dp = 0
    c_dp_map = 0
    with tqdm(desc=f"DBpedia: {c_dp_map}/{c_dp} mapping") as p_bar:
        for k in dp_items.keys():
            c_dp += 1
            p_bar.update()
            mapping_wd = mapping.get_wikidata_from_dbpedia(k)
            if mapping_wd:
                p_bar.set_description(f"DBpedia: {c_dp_map}/{c_dp} mapping")
                c_dp_map += 1


def call_wiki_graph():
    m_f.init()
    iw.print_status("Create Wiki Graph and calculate PageRank")
    g = WikidataGraph()
    # g.save_vocab()
    g.build(n_cpu=8)
    # g.cal_pagerank()


def call_wikidata_nt_parsing():
    items = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_NT)
    reader = WDDumpReader(cf.DIR_DUMP_WD_SEMTAB)
    items.build_from_nt_dump(reader)
    items.info()


def call_wikidata_json_parsing():
    items = WDItem(db_file=cf.DIR_WIKIDATA_ITEMS_JSON)
    items.build_from_json_dump(WDDumpReader(cf.DIR_DUMP_WD))
    items.info()


def call_dbpedia_parsing():
    items = DPItem()
    items.build()
    items.info()


def call_merge_kgs():
    items = MItem()
    items.build(buff_size=1e7)


def call_update_deletes():
    db = FuzzySearch()
    # db.update_db(update_from_id=10360850, max_dis=2, prefix_len=20, buff_size=4e8, buff_save=4e7)
    # db.update_db(update_from_id=1743421, max_dis=4, prefix_len=10, buff_size=4e8, buff_save=4e7)  # 8% 12085367
    db.update_db(
        update_from_id=11096658, max_dis=8, prefix_len=10, buff_size=4e8, buff_save=4e7
    )  # 2% 3566989
    # db.update_db(update_from_id=41845, max_dis=10, prefix_len=12, buff_size=4e8, buff_save=5e7)


def call_resource_fuzzy_search():
    # m_f.init()

    # fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_ENGLISH)
    # fz_db.build_vocab(n_cpu=8)
    # fz_db.build_words_page_rank(n_cpu=8, from_i=0)
    # fz_db.build_vocab_from_dict(cf.ENTITY_HIST, buff_save=1e7)
    # fz_db.update_deletes(update_from_id=138176424, max_dis=4, prefix_len=10)
    # fz_db.build_deletes_2(db_name="english", source="id", update_from_id=153054178, max_dis=4, prefix_len=10, min_len=1)
    # fz_db.build_deletes_2(db_name="english", source="id", update_from_id=143529511, max_dis=2, prefix_len=20, min_len=1)

    # fz_db.build_deletes_2(db_name="english", source="word", update_from_id=0, max_dis=2, prefix_len=16, min_len=1)

    fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL)
    fz_db.build_deletes(
        db_name="multilingual",
        source="word",
        update_from_id=0,
        max_dis=2,
        prefix_len=14,
        min_len=1,
    )
    # fz_db.build_deletes_2(db_name="multilingual", source="id", update_from_id=231124508, max_dis=4, prefix_len=10, min_len=1)
    # fz_db.update_deletes(update_from_id=138176424, max_dis=2, prefix_len=20)

    # fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL + ".copy")
    # fz_db.update_deletes(lang="multilingual", update_from_id=231124508, max_dis=4, prefix_len=10)
    # fz_db.copy()
    # fz_db.build_vocab_from_dict(cf.ENTITY_HIST, buff_save=1e7)

    #

    # fz_db.build_deletes_2(db_name="_english", update_from_id=0, max_dis=2, prefix_len=10, min_len=1)
    # Done
    #
    # fz_db.build_deletes_2(db_name="_english", update_from_id=85735827, update_to_id=100000000, max_dis=4, prefix_len=10, min_len=1)
    # collect 225214956/731567756 31%

    # fz_db.build_deletes_2(db_name="_english", update_from_id=int(89503993 * 0.95), max_dis=2, prefix_len=20, min_len=1)

    # fz_db = MEntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL)
    # fz_db.build_deletes_2(db_name="_multilingual", update_from_id=200000000, update_to_id=None, max_dis=4, prefix_len=10, min_len=1)

    # 1 0 - 10000000 Done
    # 1 10000000 - 20000000 Done
    # 2 20000000 - 30000000 Done
    # 3 30000000 - 40000000 Done
    # 4 40000000 - 50000000 Done
    # 5 50000000 - 60000000 Done
    # 6 60000000 - 80000000 Done
    # 7 80000000 - 100000000 Done
    # 8 100000000 - 120000000 Done

    # 9 120000000 - 140000000 Done
    # 10 140000000 - 160000000 Done
    # 11 160000000 - 180000000 Done
    # 12 180000000 - 200000000
    # 13 200000000 Done

    # fz_db.build_deletes_2(db_name="_multilingual", update_from_id=200000000, update_to_id=None, max_dis=2, prefix_len=20, min_len=1)
    # 0 0 - 50000000
    # 1 50000000 - 100000000
    # 2 100000000 - 150000000
    # 3 150000000 - 200000000
    # 4 200000000

    # start = time()
    # print(time() - start)
    # fz_db.build_deletes_2(db_name="_multilingual", update_from_id=124420371, max_dis=4, prefix_len=10, min_len=1)

    # fz_db.build_deletes_2(db_name="_multilingual", update_from_id=124420371, max_dis=4, prefix_len=10, min_len=1)

    # fz_db.build_deletes_2(update_from_id=0, max_dis=4, prefix_len=12, min_len=1)  # 86%
    # fz_db.build_deletes_2(update_from_id=132392497, max_dis=2, prefix_len=20, min_len=1)  # 86%
    # fz_db.build_deletes_2(update_from_id=149599926, max_dis=4, prefix_len=10, min_len=1)  # 84% 149599926
    # fz_db.build_deletes_2(update_from_id=100000000, max_dis=8, predix_len=10, min_len=1)  # 56% 92367383
    # 75851230
    # fz_db.build_words_page_rank(buff_save=cf.SIZE_512MB)
    # fz_db.build()
    # fz_db.update_database()
    # fz_db.build(buff_size=50000)
    # print(fz_db.size_vocab())
    # print(fz_db.size_words())

    # fzs = FuzzySearch()
    # fzs.gen_deletes()
    # items = MItem()
    # items.build(buff_size=10000)
    # print(items.lookup_wd_with_word("tokyo"))


def call_deletes_collect(lang, max_dis=2, prefix_len=14, batch_id=0):
    db_file = (
        f"{cf.DIR_MODELS}/deletes_{lang}_d{max_dis}_pl{prefix_len}_dup_{batch_id}.lmdb"
    )
    iw.print_status(f"Collect {db_file}")

    tmp = DBDeletes(db_file=db_file, buff_size=cf.SIZE_1GB, buff_save=1e8)
    tmp.collect(max_dis, prefix_len, from_i=0, batch_id=batch_id)

    # for i in range(9, 14):
    #     db_file = f"{cf.DIR_MODELS}/deletes_multilingual_d{max_dis}_pl{predix_len}_dup_{i}.lmdb"
    #     iw.print_status(db_file)
    #     tmp = DBDeletes(db_file=db_file, buff_size=5e8, buff_save=5e7)
    #     tmp.collect(max_dis, predix_len, from_i=0, dup_id=i)
    # 4 10 67246350
    # 2 20 2360498574
    # multilingual 773093259


def call_elasticsearch():
    from api import m_f

    m_f.init()
    fzs = ESearch()
    fzs.build()


import pickle
import numpy as np


# @profile
def test_list():
    data = {i: i + 1 for i in range(100000)}
    start = time()
    data = [
        (
            str(k).encode(cf.ENCODING)[: cf.LMDB_MAX_KEY],
            pickle.dumps(v),  # zlib.compress(pickle.dumps(v))
        )
        for k, v in data.items()
    ]
    print(f"Time: {time() - start}")


# @profile
def test_loop_1():
    def _get_dump_obj(k, v):
        return (
            str(k).encode(cf.ENCODING)[: cf.LMDB_MAX_KEY],
            pickle.dumps(v),  # zlib.compress(pickle.dumps(v))
        )

    data = {i: i + 1 for i in range(100000)}
    start = time()
    new_data = []
    while data:
        k, v = data.popitem()
        new_data.append(_get_dump_obj(k, v))
    print(f"Time: {time() - start}")


# def call_spell_correction():
#     db = SpellCorrectDB()
#     db.build()


def show_info(db_dir):
    db = DBItemDefault(f"{cf.DIR_MODELS}/{db_dir}.lmdb")
    iw.print_status(db.env.info())
    iw.print_status("%.2f GB" % (db.env.info()["map_size"] / cf.SIZE_1GB))


def copy_deletes():
    # limit = 1000000
    # d = 4
    # pl = 10
    # lang = "multilingual"

    # process_file = f"{cf.DIR_MODELS}/deletes_{lang}_d{d}_pl{pl}.lmdb"
    # iw.print_status(process_file)
    # db = DBItem(process_file, db_default=True)
    # db_names = {b'__items__': {"name": b'__items__', "integerkey": False}}
    # db._copy(db_names, map_size=cf.SIZE_1GB * 600, compress=False, map_extend=cf.SIZE_1GB * 2)

    # start = time()
    # for i, _ in enumerate(db.items_default(decoder="bytes_set", compress=False)):
    #     if i > limit:
    #         break
    # time_1 = limit / (time() - start)
    # iw.print_status(time_1)
    #
    # process_file = f"{cf.DIR_MODELS}/deletes_d{d}_pl{pl}.lmdb.copy"
    # iw.print_status(process_file)
    # db = DBItem(process_file, db_default=True)
    # start = time()
    #
    # for i, _ in enumerate(db.items_default(decoder="bytes_set", compress=True)):
    #     if i > limit:
    #         break
    # time_2 = limit / (time() - start)
    # iw.print_status(time_2)
    # iw.print_status(time_2 / time_1)

    # txn = db.env.begin()
    # print(txn.stat(db.db_default))
    # '[mr2 k'
    db_name = f"{cf.DIR_MODELS}/deletes_multilingual_d4_pl10.lmdb"
    iw.print_status(db_name)
    db = DBItem(db_name)
    db.copy_lmdb()
    # db._copy({b'__items__': b'__items__'}, buff_size=1e8)
    # db.copy_to_bytes_set({b'__del__': b'__del__'})


def print_info():
    lmdb_dirs = iw.get_files_from_dir(f"{cf.DIR_MODELS}", extension="lmdb")
    for lmdb_dir in lmdb_dirs:
        db = DBItem(lmdb_dir)
        iw.print_status(lmdb_dir)
        iw.print_status(db.env.info())
        iw.print_status("%.2f" % (db.env.info()["map_size"] / cf.SIZE_1GB))


def call_test_es():
    m_f.init()
    # m_entity_search.eval_semtab_2020_entity_search(dataset="R1", n_cpu=4, lang="en", limit=0, expensive=False)
    m_entity_search.eval_semtab_2020_entity_search(
        dataset="R2", n_cpu=6, lang="en", limit=0, expensive=True
    )
    # m_entity_search.eval_semtab_2020_entity_search(dataset="R3", n_cpu=8, lang="en", limit=0, expensive=False)
    # m_entity_search.eval_semtab_2020_entity_search(dataset="R4", n_cpu=8, lang="en", limit=0, expensive=False)
    # m_entity_search.eval_semtab_2020_entity_search(dataset="2T", n_cpu=4, lang="en", limit=0, expensive=True)


def test_stats_deletes():
    # f"{cf.DIR_MODELS_MULTILINGUAL}/deletes_multilingual_d{d}_pl{pl}.lmdb"
    db_dirs = [
        f"{cf.DIR_MODELS}/deletes_english_d{2}_pl{14}.lmdb",
        f"{cf.DIR_MODELS}/deletes_english_d{2}_pl{16}.lmdb",
        f"{cf.DIR_MODELS}/deletes_english_d{4}_pl{10}.lmdb",
        f"{cf.DIR_MODELS_MULTILINGUAL}/deletes_multilingual_d{2}_pl{14}.lmdb",
        f"{cf.DIR_MODELS_MULTILINGUAL}/deletes_multilingual_d{4}_pl{10}.lmdb",
    ]
    for db_dir in db_dirs:
        iw.print_status(db_dir)
        # count = []
        stats = defaultdict()
        db = DBItemDefault(db_dir)
        c = 0
        div = 1000
        for k, v in db.items(mode="bytes_set"):
            # count.append(len(v))
            bucket = len(v) // div
            if not stats.get(bucket):
                stats[bucket] = defaultdict(int)
            stats[bucket][len(k)] += 1
            c += 1
            if c > 100000000:
                break
        # plt.hist(count, bins=10)
        # sns.displot(count, stat="probability")
        stats = sorted(stats.items(), key=lambda x: x[0], reverse=True)
        for k, v in stats[:20]:
            _v = sorted(v.items(), key=lambda x: x[1], reverse=True)
            iw.print_status(f"{(k -1) * div} - {k *div}: {_v}")
        # plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
        # plt.show()


def to_csr(row, n):
    indptr = np.zeros(dtype=np.uintc, shape=(n + 1,))
    for i in tqdm(range(len(row)), total=len(row)):
        indptr[row[i] + 1] += 1

    for i in tqdm(range(n), total=n):
        indptr[i + 1] += indptr[i]

    return indptr


def sort_results(challenge="semtab2021", data_name="hardtables", search_mode="a"):
    def sort_csv(file_dir):
        csv_file = m_iw.load_object_csv(file_dir)
        csv_file.sort(key=lambda x: x[0])
        m_iw.save_object_csv(file_dir, csv_file)

    sort_csv(
        st.dir_cea_res.format(
            challenge=challenge, data_name=data_name, search_mode=search_mode
        )
    )
    sort_csv(
        st.dir_cta_res.format(
            challenge=challenge, data_name=data_name, search_mode=search_mode
        )
    )
    sort_csv(
        st.dir_cpa_res.format(
            challenge=challenge, data_name=data_name, search_mode=search_mode
        )
    )


def reindex_cea():
    csv_dir = "/Users/phucnguyen/git/mtab/results/semtab2021/biotables/local/cea.csv"
    csv_file = m_iw.load_object_csv(csv_dir)
    csv_file = [[table_id, col, row, res] for table_id, row, col, res in csv_file]
    m_iw.save_object_csv(csv_dir, csv_file)


if __name__ == "__main__":
    # call_merge_kgs()
    # "de", "es",
    # langs = ["ar", "zh", "pl", "pt"]  # "vi", "ja", "fr", "nl", "ru", "it",
    # parse_wikipedia_dump(langs, n_cpu=8)
    # reindex_cea()
    # sort_results(challenge="semtab2021", data_name="hardtables copy")
    # test_compression()

    # test_rocks_db()
    # exit()
    # from scipy import sparse
    # import networkx as nx
    #
    # G = nx.from_scipy_sparse_matrix(
    #     sparse.load_npz(cf.DIR_WIKI_GRAPH), create_using=nx.DiGraph
    # )
    # debug = 1

    # np.load(f"{cf.DIR_MODELS}/row.dat1")
    # row = to_csr([0, 0, 1, 1, 2, 2, 2, 3], 4)
    # indptr = to_csr(np.load(f"{cf.DIR_MODELS}/row.dat1"), 93839108)
    # np.save(f"{cf.DIR_MODELS}/indptr.dat1", indptr)

    # from scipy import *
    # from scipy.sparse import *

    #
    # #
    # tmp = lil_matrix((3, 3), dtype=np.uintc)
    #
    # row1 = np.array([0, 0, 1])
    # col1 = np.array([0, 2, 2])
    # data1 = np.array([1, 2, 3])
    #
    # tmp1 = lil_matrix()
    # for i in range(len(row1)):
    #     tmp[row1[i], col1[i]] = data1[i]
    #
    # tmp1 = tmp.tocsr()
    #
    # row1 = np.array([0, 0, 1, 2, 2, 2])
    # col1 = np.array([0, 2, 2, 0, 1, 2])
    # data1 = np.array([1, 2, 3, 4, 5, 6])
    # tmp = csr_matrix((data1, (row1, col1)), shape=(3, 3))
    #
    # tmp = 2 * tmp @ tmp
    #
    # tmp1 = csr_matrix((data1, (row1, col1)), shape=(3, 3))
    # tmp2 = tmp1.tocsr()

    # api.resources.main.build_EntityLabels(cf.DIR_WIKI_LABELS_MULTILINGUAL)
    # api.resources.main.update_EntityLabels_PageRank(cf.DIR_WIKI_LABELS_MULTILINGUAL)
    #
    # api.resources.main.build_KeyWord_Search()

    # api.resources.main.update_EntityLabels_PageRank(cf.DIR_WIKI_LABELS_ENGLISH, n_cpu=4)
    # api.resources.main.update_EntityLabels_PageRank(
    #     cf.DIR_WIKI_LABELS_MULTILINGUAL, n_cpu=4
    # )
    # api.resources.main.build_WikiGraph_Edges()
    # api.resources.main.cal_WikiGraph_PageRank()
    # api.resources.main.build_deletes("english", max_dis=2, prefix_len=14)

    # api.resources.main.build_deletes(
    #     "multilingual",
    #     max_dis=4,
    #     prefix_len=10,
    #     update_from_id=223492124,
    #     # update_to_id=126000000,
    #     batch_id=4,
    # )

    # call_deletes_collect(
    #     "multilingual", max_dis=4, prefix_len=10, batch_id=1
    # )  # "multilingual" english
    # call_deletes_collect(
    #     "multilingual", max_dis=4, prefix_len=10, batch_id=2
    # )  # "multilingual" english
    # call_deletes_collect(
    #     "multilingual", max_dis=4, prefix_len=10, batch_id=3
    # )  # "multilingual" english
    # call_deletes_collect(
    #     "multilingual", max_dis=4, prefix_len=10, batch_id=4
    # )  # "multilingual" english
    #
    # db_file = f"{cf.DIR_MODELS}/deletes_multilingual_d4_pl10.lmdb"
    # tmp = DBDeletes(db_file=db_file, buff_size=cf.SIZE_1GB, buff_save=5e7)
    # tmp.copy_lmdb()

    # api.resources.main.build_deletes(
    #     "multilingual",
    #     max_dis=4,
    #     prefix_len=10,
    #     update_from_id=140 000 000,
    #     # update_to_id=140 000 000,
    #     batch_id=3,
    # )
    # api.resources.main.build_ElasticSearchIndex()
    # test_stats_deletes()
    # call_test_es()
    # print_info()
    # merge_lmdb(4, 10)
    # copy_deletes()
    # call_resource_fuzzy_search()
    # call_deletes_collect()
    # convert_deletes_2_bytes_set()
    # test_compression()
    # call_merge_kgs()
    # check_overlapping_wd_db()
    call_wiki_graph()

    # call_elasticsearch()
    # call_update_deletes()
    # call_wikidata_json_parsing()

    # call_wikidata_nt_parsing()
    # call_dbpedia_parsing()
    # call_spell_correction()
    # test_db()
    # test_loop_1()
    # test_loop_2()
    # test_list()
    # test()
