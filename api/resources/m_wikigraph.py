import gc
import array
from scipy import sparse
from collections import defaultdict, Counter

from api.resources.m_db_item import DBItem, serialize
from api.resources.m_mapping_id import MappingID
from api.utilities import m_io as iw
import m_config as cf
from api.resources.m_item import MItem
from api.utilities import m_utils as ul
from tqdm import tqdm
import numpy as np
from multiprocessing.pool import Pool
from contextlib import closing
import networkx as nx
import scipy
import scipy.sparse as sprs
import scipy.spatial
import scipy.sparse.linalg
from api import m_f


def pool_get_outlinks(wd_id):
    if ul.is_redirect_or_disambiguation(wd_id):
        return wd_id, set()
    return wd_id, m_f.m_items().get_wd_outlinks(wd_id)


class WikidataGraph(DBItem):
    def __init__(self, db_file=cf.DIR_WIKI_GRAPH_VOCAB):
        super().__init__(db_file=db_file, max_db=2, map_size=cf.SIZE_1GB * 10)
        # QID - local id (int)
        self.db_vocab = self._env.open_db(b"__vocab__")
        # local id (int) - QID
        self.db_vocab_inv = self._env.open_db(b"__vocab_inv_", integerkey=True)

        self._buff_vocab = defaultdict(int)
        self._len_vocab = self.size_vocab() + len(self._buff_vocab)

        self.graph = None
        # self.build()

    def size_vocab(self):
        return self.get_db_size(self.db_vocab_inv)

    @staticmethod
    def compute_pagerank(
        graph, alpha=0.85, max_iter=1000, tol=1e-06, personalize=None, reverse=False
    ):
        if reverse:
            graph = graph.T
            iw.print_status("Reversed matrix")

        n, _ = graph.shape
        iw.print_status(f"Pagerank Calculation: {n} nodes")
        r = np.asarray(graph.sum(axis=1)).reshape(-1)

        k = r.nonzero()[0]

        D_1 = sprs.csr_matrix((1 / r[k], (k, k)), shape=(n, n))

        if personalize is None:
            personalize = np.ones(n)
        personalize = personalize.reshape(n, 1)
        s = (personalize / personalize.sum()) * n

        z_T = (((1 - alpha) * (r != 0) + (r == 0)) / n)[np.newaxis, :]
        W = alpha * graph.T @ D_1

        x = s
        oldx = np.zeros((n, 1))

        iteration = 0

        tmp_tol = scipy.linalg.norm(x - oldx)
        while tmp_tol > tol:
            iw.print_status(f"Iteration {iteration + 1} - Tol: {tmp_tol}")
            oldx = x
            x = W @ x + s @ (z_T @ x)
            iteration += 1
            if iteration >= max_iter:
                break
            tmp_tol = scipy.linalg.norm(x - oldx)
        x = x / sum(x)

        return x.reshape(-1)

    def build_db_pagerank(
        self, alpha=0.85, max_iter=1000, tol=1e-06, personalize=None, reverse=True
    ):
        self.build()
        pagerank = self.compute_pagerank(
            self.graph, alpha, max_iter, tol, personalize, reverse
        )
        # iw.save_obj_pkl(cf.DIR_WIKI_PAGERANK, pagerank)

        # self.drop_db(self.db_vocab_inv)
        # self.copy_lmdb()

        # tmp = dict()
        # for k, v in tqdm(
        #     self.get_db_iter(self.db_vocab, compress_value=False),
        #     total=self.size_vocab(),
        # ):
        #     tmp[k] = v
        # iw.save_obj_pkl(cf.DIR_WIKI_QID, tmp)

        # tmp = iw.load_obj_pkl(cf.DIR_WIKI_QID)
        # tmp = {v: k for k, v in tmp.items()}
        # iw.print_status(len(tmp))
        #
        # tmp = sorted(tmp.items(), key=lambda x: x[0])
        #
        # tmp = [serialize(k, v, integerkey=True, compress_value=False) for k, v in tmp]
        #
        # self.write_bulk_with_buffer(
        #     self.env, self.db_vocab_inv, tmp, preprocess_data=False
        # )

        # tmp = set()
        # for k in tqdm(
        #     self.get_db_iter(self.db_vocab_inv, get_values=False, integerkey=True),
        #     total=self.size_vocab(),
        # ):
        #     tmp.add(k)
        # iw.print_status(len(tmp))

        # pagerank = iw.load_obj_pkl(cf.DIR_WIKI_PAGERANK)

        # save pagerank stats for normalization later
        pagerank = np.array(pagerank)
        pagerank_stats = {
            "max": np.max(pagerank),
            "min": np.min(pagerank),
            "std": np.std(pagerank),
            "mean": np.mean(pagerank),
            "div": np.max(pagerank) - np.min(pagerank),
        }
        iw.save_obj_pkl(cf.DIR_WIKI_PAGERANK_STATS, pagerank_stats)
        iw.print_status(pagerank_stats)

        pagerank = [
            [
                self.get_value(
                    self.db_vocab_inv, i, integerkey=True, compress_value=False
                ),
                score,
            ]
            for i, score in enumerate(pagerank)
        ]
        iw.print_status(f"Saving {len(pagerank)}")

        wiki_items = MItem()
        wiki_items.write_bulk_with_buffer(
            wiki_items._env,
            wiki_items.db_pagerank,
            pagerank,
            integerkey=False,
            compress_value=False,
        )
        iw.print_status("Done")

    def is_a_statement(self, wd_1, wd_2):
        respond = 0
        try:
            wd_1_id = self.get_value(self.db_vocab, wd_1, compress_value=False)
            wd_2_id = self.get_value(self.db_vocab, wd_2, compress_value=False)
            if wd_1_id is not None and wd_2_id is not None:
                status = self.graph[wd_1_id, wd_2_id]
                if status:
                    respond = status
        except Exception as message:
            iw.print_status(message)
        return respond

    def get_outlinks(self, wd_1):
        respond = []
        wd_1_id = self.get_value(self.db_vocab, wd_1, compress_value=False)
        if wd_1_id is not None:
            status = self.graph.getrow(wd_1_id).nonzero()
            if status and len(status[1]):
                respond = [
                    self.get_value(
                        self.db_vocab_inv, wd_2, integerkey=True, compress_value=False
                    )
                    for wd_2 in status[1]
                ]
                respond = [r for r in respond if r]
        return respond

    def get_inlinks(self, wd_1):
        respond = []
        wd_1_id = self.get_value(self.db_vocab, wd_1, compress_value=False)
        if wd_1_id is not None:
            status = self.graph.getcol(wd_1_id).nonzero()
            if status and len(status[0]):
                respond = [
                    self.get_value(
                        self.db_vocab_inv, wd_2, integerkey=True, compress_value=False
                    )
                    for wd_2 in status[0]
                ]
                respond = [r for r in respond if r]
        return respond

    def get_vocab_id(self, wd_id):
        v_id = self._buff_vocab.get(wd_id)
        if v_id is None:
            v_id = self.get_value(self.db_vocab, wd_id, compress_value=False)

        if v_id is None:
            v_id = self._len_vocab
            self._buff_vocab[wd_id] = v_id
            self._len_vocab += 1
        return v_id

    def get_vocab_word(self, wd_id):
        v_word = self.get_value(
            self.db_vocab_inv, wd_id, integerkey=True, compress_value=False
        )
        return v_word

    def save_buff_vocab(self):
        if not self._buff_vocab:
            return
        self.write_bulk_with_buffer(
            self._env, self.db_vocab, self._buff_vocab, compress_value=False
        )

        # Call inv
        self._buff_vocab = {v: k for k, v in self._buff_vocab.items()}
        self.write_bulk_with_buffer(
            self._env,
            self.db_vocab_inv,
            self._buff_vocab,
            integerkey=True,
            compress_value=False,
        )
        self._buff_vocab = defaultdict(int)

    def build(self, n_cpu=1, buff_save=1e8):
        try:
            self.graph = sparse.load_npz(cf.DIR_WIKI_GRAPH)
        except FileNotFoundError:
            wiki_items = MItem()
            row, col, data = [], [], []

            def update_desc():
                return f"Statements {len(row)}"

            p_bar = tqdm(desc=update_desc(), total=wiki_items.size())
            # if n_cpu == 1:
            #     for wd_i, wiki_item in enumerate(wiki_items.keys()):
            #         wd_id, claims = pool_get_outlinks(wiki_item)
            with closing(Pool(n_cpu)) as pool:
                for wd_i, (wd_id, claims) in enumerate(
                    pool.imap_unordered(pool_get_outlinks, wiki_items.keys())
                ):
                    p_bar.update()
                    if wd_i and wd_i % 100 == 0:
                        p_bar.set_description(desc=update_desc())

                    if len(self._buff_vocab) >= buff_save:
                        self.save_buff_vocab()

                    # if len(row) > 10000:
                    #     break
                    id_head = self.get_vocab_id(wd_id)

                    # Get entity triples
                    if claims:
                        for v_wd_id, v_wd_weight in claims.items():
                            id_tail = self.get_vocab_id(v_wd_id)
                            row.append(id_head)
                            col.append(id_tail)
                            data.append(int(v_wd_weight))
            self.save_buff_vocab()

            n = self._len_vocab
            sparse.save_npz(
                cf.DIR_WIKI_GRAPH,
                sparse.csr_matrix((data, (row, col)), dtype=np.uintc, shape=(n, n)),
            )


# def pool_lookup_2cells(lk_query):
#     responds = []
#     cell_1_wd = f.lk_fz_res().get(lk_query[1])
#     if cf.RES_HIcf.get(lk_query[1]):
#         cell_1_wd += [cf.RES_HIcf.get(lk_query[1])]
#
#     tmp_wd = f.lk_wikidata().get_data(lk_query[1])
#     if tmp_wd and len(tmp_wd):
#         cell_1_wd += tmp_wd
#
#     tmp_wd = f.lk_wikipedia().get(lk_query[1])
#     if tmp_wd and len(tmp_wd):
#         cell_1_wd += tmp_wd
#
#     if cell_1_wd and len(cell_1_wd):
#         cell_1_wd = list(dict.fromkeys(cell_1_wd))
#     else:
#         cell_1_wd = []
#
#     cell_2_wd = f.lk_fz_res().get(lk_query[2])
#     if cf.RES_HIcf.get(lk_query[2]):
#         cell_2_wd += [cf.RES_HIcf.get(lk_query[2])]
#
#     tmp_wd = f.lk_wikidata().get_data(lk_query[2])
#     if tmp_wd and len(tmp_wd):
#         cell_2_wd += tmp_wd
#
#     tmp_wd = f.lk_wikipedia().get(lk_query[2])
#     if tmp_wd and len(tmp_wd):
#         cell_2_wd += tmp_wd
#
#     if cell_2_wd and len(cell_2_wd):
#         cell_2_wd = list(dict.fromkeys(cell_2_wd))
#     else:
#         cell_2_wd = []
#
#     if cell_1_wd and len(cell_1_wd) and cell_2_wd and len(cell_2_wd):
#         if len(cell_1_wd) * len(cell_2_wd) > 1000000:
#             iw.print_status(f"Too large: {len(cell_1_wd):d} x {len(cell_2_wd):d} = "
#                             f"{len(cell_1_wd) * len(cell_2_wd):d}")
#         for wd_1 in cell_1_wd:
#             cell_2_set = set()
#             for wd_2 in cell_2_wd:
#                 if f.wikidata_graph().is_a_statement(wd_1, wd_2):
#                     cell_2_set.add(wd_2)
#
#             if len(cell_2_set):
#                 responds.append({"core": wd_1,
#                                  "core_label": f.wikidata_info().get_labels(wd_1, wd_1),
#                                  "other": cell_2_set})
#
#     return lk_query, responds
#
#
# def lookup_2cells(n_cpu=1):
#     count = 0
#     error = 0
#     try:
#         lk_res = iw.load_obj_pkl(cf.DIR_LK_RES_FZ_2CELLS_2CELLS, is_message=True)
#         # for lk_query, responds in lk_res.items():
#         #     count, error = print_status(count, error)
#
#     except Exception as message:
#         iw.print_status(message)
#         lk_res = defaultdict()
#
#     lk_queries = []
#     lk_cell2 = defaultdict(list)
#     for lk_query in iw.load_obj_pkl(cf.DIR_LK_QUERIES_GT_2CELLS):
#         if lk_query[0] == 1 and not lk_res.get((lk_query[1], lk_query[2])):
#             lk_queries.append(lk_query)
#             lk_cell2[lk_query[2]].append(lk_query)
#
#     lk_cell2 = sorted(lk_cell2.items(), key=lambda x: len(x[1]), reverse=True)
#
#     lk_queries = []
#     for _, lk_cell2_values in lk_cell2:
#         lk_queries.extend(lk_cell2_values)
#
#     lk_queries = sorted(list(lk_queries), key=lambda x: x[2], reverse=True)
#
#     count = 0
#     error = 0
#     error_core = set()
#     with tqdm(total=len(lk_queries)) as p_bar:
#         with closing(Pool(processes=n_cpu)) as p:
#             for lk_query, responds in p.imap_unordered(pool_lookup_2cells, lk_queries):
#                 p_bar.update()
#                 lk_res[(lk_query[1], lk_query[2])] = responds
#                 if len(responds):
#                     count += 1
#                     iw.print_status("\nOK:[%s]-%d | %s | %s" % (lk_query[0], len(responds), lk_query[1], lk_query[2]),
#                                     is_screen=False)
#                     for respond_i, respond in enumerate(responds):
#                         for value in respond["other"]:
#                             if ul.is_wd_item(value):
#                                 value_label = str(f.wikidata_info().get_labels(value))
#                             else:
#                                 value_label = ""
#                             iw.print_status(
#                                 "%d. %s[%s] - %s[%s]" % (
#                                     respond_i + 1, respond.get("core", ""), respond.get("core_label", ""),
#                                     value, value_label), is_screen=False)
#
#                 else:
#                     iw.print_status("\nError:[%s] %s | %s" % (lk_query[0], lk_query[1], lk_query[2]), is_screen=False)
#                     error += 1
#                     error_core.add(lk_query[1])
#                 p_bar.set_description("Success: %d - Error: %d - Str: %d" % (count, error, len(error_core)))
#             else:
#                 error += 1
#                 error_core.add(lk_query[1])
#         iw.save_obj_pkl(cf.DIR_LK_RES_FZ_2CELLS_2CELLS, lk_res)
#

if __name__ == "__main__":
    m_f.init()
    wiki_graph = WikidataGraph()
    # wiki_graph.build(n_cpu=8)
    wiki_graph.build_db_pagerank()
    # build_obj = MItem()
    # build_obj.cal_max_pagerank()

    #
    # tmp = f.lk_fz_res_2cell_2cell().get(("V*!AY Psc", "Pisces"))
    # tmp = wiki_graph.get_inlinks("P31")
    # tmp = wiki_graph.get_outlinks("P31")
    # wiki_graph.get_statements("V*!AY Psc", "Pisces")
    #
    # lookup_2cells(n_cpu=4)
    # 'Q1001328' 'Q17172850'
    # print(wiki_graph.is_a_statement("Q84825066", "Q12546"))
    # print(wiki_graph.is_a_statement("P000815", "Q21502404"))
    # print(wiki_graph.get_outlinks("P000815"))
    # print(wiki_graph.get_inlinks("Q21502404"))
    # print(wiki_graph.get_inlinks('Q5751207'))
    # print(wiki_graph.is_a_statement("P1083", "Q27826989"))
    # print(wiki_graph.get_outlinks("P1083"))
    # print(wiki_graph.get_inlinks("Q27826989"))


"""
Statements 844045860: 100%|████████| 95064301/95064301 [3:17:37<00:00, 8017.39it/s]
"""
