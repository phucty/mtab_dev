import numpy as np
from scipy import sparse
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


class BM25Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, k1=1.2, b=0.75, use_idf=True):
        self._k1 = k1
        self._b = b
        self._use_idf = use_idf

    @property
    def k1(self):
        return self._k1

    @property
    def b(self):
        return self._b

    @property
    def use_idf(self):
        return self._use_idf

    def fit(self, X, y=None):
        if not sparse.issparse(X):
            X = sparse.csc_matrix(X)

        if self._use_idf:
            (n_samples, n_features) = X.shape
            if sparse.isspmatrix_csr(X):
                df = np.bincount(X.indices, minlength=X.shape[1])
            else:
                df = np.diff(sparse.csc_matrix(X, copy=False).indptr)

            idf = np.log(1.0 + (float(n_samples) - df + 0.5) / (df + 0.5))
            self._idf_diag = sparse.spdiags(idf, diags=0, m=n_features, n=n_features, format="csr")

        return self

    def transform(self, X, copy=True):
        if hasattr(X, "dtype") and np.issubdtype(X.dtype, np.float):
            X = sparse.csr_matrix(X, copy=copy)
        else:
            X = sparse.csr_matrix(X, dtype=np.float64, copy=copy)

        (n_samples, n_features) = X.shape

        doc_len = X.sum(axis=1).A
        nonzero_sizes = X.indptr[1:] - X.indptr[0:-1]
        rep_doc_len = np.repeat(doc_len, nonzero_sizes)
        avg_len = np.mean(doc_len)

        k1 = self._k1
        b = self._b
        data = X.data * (k1 + 1.0) / (X.data + k1 * (1.0 - b + b / avg_len * rep_doc_len))
        X = sparse.csr_matrix((data, X.indices, X.indptr), shape=X.shape)

        if self._use_idf:
            X = X * self._idf_diag

        return X

    @property
    def idf_(self):
        return np.ravel(self._idf_diag.sum(axis=0))


class BM25Vectorizer(CountVectorizer):
    def __init__(
        self,
        input="content",
        encoding="utf-8",
        decode_error="strict",
        strip_accents=None,
        lowercase=True,
        preprocessor=None,
        tokenizer=None,
        analyzer="word",
        stop_words=None,
        token_pattern=r"(?u)\b\w\w+\b",
        ngram_range=(1, 1),
        max_df=1.0,
        min_df=1,
        max_features=None,
        vocabulary=None,
        binary=False,
        dtype=np.int64,
        k1=1.2,
        b=0.75,
        use_idf=True,
    ):

        super(BM25Vectorizer, self).__init__(
            input=input,
            encoding=encoding,
            decode_error=decode_error,
            strip_accents=strip_accents,
            lowercase=lowercase,
            preprocessor=preprocessor,
            tokenizer=tokenizer,
            analyzer=analyzer,
            stop_words=stop_words,
            token_pattern=token_pattern,
            ngram_range=ngram_range,
            max_df=max_df,
            min_df=min_df,
            max_features=max_features,
            vocabulary=vocabulary,
            binary=binary,
            dtype=dtype,
        )

        self._bm25 = BM25Transformer(k1=k1, b=b, use_idf=use_idf)

    @property
    def k1(self):
        return self._bm25.k1

    @property
    def b(self):
        return self._bm25.b

    @property
    def idf_(self):
        return self._bm25.idf_

    def fit(self, raw_documents, y=None):
        X = super(BM25Vectorizer, self).fit_transform(raw_documents)
        self._bm25.fit(X)
        return self

    def fit_transform(self, raw_documents, y=None):
        X = super(BM25Vectorizer, self).fit_transform(raw_documents)
        self._bm25.fit(X)
        return self._bm25.transform(X, copy=False)

    def transform(self, raw_documents, copy=True):
        X = super(BM25Vectorizer, self).transform(raw_documents)
        return self._bm25.transform(X, copy=False)


if __name__ == '__main__':
    lk = BM25Vectorizer()
    corpus = ["Human machine interface for lab abc computer applications",
              "A survey of user opinion of computer system response time",
              "The EPS user interface management system",
              "System and human system engineering testing of EPS",
              "Relation of user perceived response time to error measurement",
              "The generation of random binary unordered trees",
              "The intersection graph of paths in trees",
              "Graph IV Widths of trees and well quasi ordering",
              "Graph minors A survey"]
    corpus_v = lk.fit_transform(corpus)

    query = ['The intersection graph of paths in trees survey Graph']
    res_v = lk.transform(query)
    res_i = cosine_similarity(res_v, corpus_v).flatten()
    res = {d: s for d, s in zip(corpus, res_i)}

    print("Done")
