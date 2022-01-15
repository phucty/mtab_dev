import pickle
import random
import struct
import zlib
from itertools import islice
from time import time
import lzma
import lmdb
import msgpack
import numpy as np
import orjson

# import quickle
import rocksdb
import snappy
from api.resources.m_db_fuzzy import DBDupInt
from tqdm import tqdm
import brotli
import api.utilities.m_io as iw
import m_config as cf
from api.resources.m_db_item import DBItem, DBItemDefault, deserialize

# from api.resources.m_db_rocks import DBItemRocks
from api.resources.m_item import MItem
import api.utilities.m_utils as ul
from lz4 import frame

# import fdb
import gzip


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def get_encoded_data(data, encode="pickle", compress=True):
    if encode == "msgpack":
        data1 = [
            (str(k).encode(cf.ENCODING), msgpack.packb(v, default=set_default),)
            for k, v in data
        ]
    elif encode == "quickle":
        data1 = [(k.encode(cf.ENCODING), quickle.dumps(v)) for k, v in data]
    elif encode == "orjson":
        data1 = [(k.encode(cf.ENCODING), orjson.dumps(v)) for k, v in data]
    elif encode == "numpy":
        data1 = [
            (struct.pack("I", k), np.array(list(v), dtype=np.uint32).tobytes())
            for k, v in data
        ]
    else:  # pickle
        data1 = [ul.get_dump_obj(k, v) for k, v in data]

    if compress:
        data1 = [(k, frame.compress(v)) for k, v in data1]
    return data1


def get_decoded_data(data, encode="pickle", compress=True):
    if compress:
        data = [(k, frame.decompress(v)) for k, v in data]
    if encode == "msgpack":
        data1 = {k.decode(cf.ENCODING): msgpack.unpackb(v) for k, v in data}
    elif encode == "quickle":
        data1 = {k.decode(cf.ENCODING): quickle.loads(v) for k, v in data}
    elif encode == "orjson":
        data1 = {k.decode(cf.ENCODING): orjson.loads(v) for k, v in data}
    elif encode == "numpy":
        data1 = {
            struct.unpack("I", k)[0]: np.frombuffer(v, dtype=np.uint32).flat
            for k, v in data
        }
    else:  # pickle
        data1 = {k.decode(cf.ENCODING): pickle.loads(v) for k, v in data}
    return data1


def dump_foundationdb(data, encode="pickle", id="", compress=True):
    # Database setup
    iw.delete_file(cf.DIR_TEST_DB_3 + id)

    fdb.api_version(630)
    env = lmdb.open(
        cf.DIR_TEST_DB_3 + id,
        map_async=True,
        map_size=cf.LMDB_MAP_SIZE,
        subdir=False,
        lock=False,
        max_dbs=1,
    )
    db = env.open_db(b"__test__")

    start = time()
    data1 = get_encoded_data(data, encode, compress)
    with env.begin(db=db, write=True, buffers=True) as txn:
        _, add = txn.cursor().putmulti(data1)
    read_time = time() - start

    start = time()
    loaded_data = []
    with env.begin(db=db) as txn:
        cursor = txn.cursor()
        for key, value in cursor.iternext():
            loaded_data.append([key, value])
    loaded_data = get_decoded_data(loaded_data, encode, compress)
    write_time = time() - start
    print(f"{encode}\tlmdb\t{compress}\t{read_time:.2f}\t{write_time:.2f}")
    data = {k: v for k, v in data}
    if loaded_data != data:
        print("Wrong decoding")


def dump_lmdb(data, encode="pickle", id="", compress=True):
    # Database setup
    iw.delete_file(cf.DIR_TEST_DB_2 + id)
    env = lmdb.open(
        cf.DIR_TEST_DB_2 + id,
        map_async=True,
        map_size=cf.LMDB_MAP_SIZE,
        subdir=False,
        lock=False,
        max_dbs=1,
    )
    db = env.open_db(b"__test__")

    start = time()
    data1 = get_encoded_data(data, encode, compress)
    with env.begin(db=db, write=True, buffers=True) as txn:
        _, add = txn.cursor().putmulti(data1)
    read_time = time() - start

    start = time()
    loaded_data = []
    with env.begin(db=db) as txn:
        cursor = txn.cursor()
        for key, value in cursor.iternext():
            loaded_data.append([key, value])
    loaded_data = get_decoded_data(loaded_data, encode, compress)
    write_time = time() - start
    print(f"{encode}\tlmdb\t{compress}\t{read_time:.2f}\t{write_time:.2f}")
    # data = {k: v for k, v in data}
    # if len(loaded_data) != len(data) or any(
    #     k for k in data if k in loaded_data and data[k] != loaded_data[k]
    # ):
    #     print("Wrong decoding")


def dump_rocksdb(data, encode="pickle", id="", compress=True):
    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 300000
    opts.write_buffer_size = 536870912
    opts.max_write_buffer_number = 3
    opts.target_file_size_base = 536870912
    opts.compression = rocksdb.CompressionType.lz4hc_compression
    # opts.max_background_compactions = 4
    opts.table_factory = rocksdb.BlockBasedTableFactory(
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
        block_cache_compressed=rocksdb.LRUCache(512 * (1024 ** 2)),
    )
    iw.delete_file(cf.DIR_TEST_ROCKSDB + id)
    db = rocksdb.DB(
        cf.DIR_TEST_ROCKSDB + id, opts
    )  # rocksdb.Options(create_if_missing=True))
    batch = rocksdb.WriteBatch()
    # Number of batches, the more, the less memory used
    max_batches = 100
    batch_size = min(len(data) - 1, int(len(data) / max_batches))

    start = time()
    data1 = get_encoded_data(data, encode, compress)
    for i, (k, v) in enumerate(data1):
        batch.put(k, v)
        if i % batch_size == 0:
            db.write(batch)
            batch = rocksdb.WriteBatch()

    db.write(batch)
    read_time = time() - start

    start = time()
    loaded_data = []
    iterator = db.iteritems()
    iterator.seek_to_first()
    for k, v in iterator:
        loaded_data.append([k, v])
    loaded_data = get_decoded_data(loaded_data, encode, compress)
    write_time = time() - start
    print(f"{encode}\trocksdb\t{compress}\t{read_time:.2f}\t{write_time:.2f}")

    # data = {k: v for k, v in data}
    # if len(loaded_data) != len(data) or any(
    #     k for k in data if k in loaded_data and data[k] != loaded_data[k]
    # ):
    #     print("Wrong decoding")


def dump_lmdb_num(data, encode="numpy", id="", compress=False):
    # Database setup
    iw.delete_file(cf.DIR_TEST_DB_2 + id)
    env = lmdb.open(
        cf.DIR_TEST_DB_2 + id,
        map_async=True,
        map_size=cf.LMDB_MAP_SIZE,
        subdir=False,
        lock=False,
        max_dbs=1,
    )
    db = env.open_db(b"__test__", integerkey=True)

    start = time()
    data = [(k, np.array(list(v), dtype=np.uint32)) for k, v in data]
    data1 = [(struct.pack("I", k), v.tobytes()) for k, v in data]
    with env.begin(db=db, write=True, buffers=True) as txn:
        _, add = txn.cursor().putmulti(data1)
    read_time = time() - start

    start = time()
    loaded_data = []
    with env.begin(db=db) as txn:
        cursor = txn.cursor()
        for key, value in cursor.iternext():
            loaded_data.append([key, value])
    loaded_data = [
        (struct.unpack("I", k)[0], np.frombuffer(v, dtype=np.uint32).flat)
        for k, v in loaded_data
    ]
    write_time = time() - start
    print(f"{encode}\tlmdb\t{compress}\t{read_time:.2f}\t{write_time:.2f}")
    data = {k: v for k, v in data}
    if len(loaded_data) != len(data) or any(
        k for k in data if k in loaded_data and data[k] != loaded_data[k]
    ):
        print("Wrong decoding")


def test_db():
    # Numerical data
    data = {
        random.randint(0, 912162600): list(
            {random.randint(0, 912162600) for _ in range(random.randint(0, 500))}
        )
        for _ in range(100000)
    }
    print(f"Numerical data: {len(data)}")
    data = sorted(data.items(), key=lambda x: x[0])

    dump_lmdb(data, "pickle", "0", compress=False)
    dump_lmdb(data, "pickle", "0_compress", compress=True)
    dump_rocksdb(data, "pickle", "0", compress=False)
    dump_rocksdb(data, "pickle", "0_compress", compress=True)
    #
    dump_lmdb(data, "msgpack", "1", compress=False)
    dump_lmdb(data, "msgpack", "1_compress", compress=True)
    dump_rocksdb(data, "msgpack", "1", compress=False)
    dump_rocksdb(data, "msgpack", "1_compress", compress=True)

    dump_lmdb(data, "numpy", "3", compress=False)
    dump_rocksdb(data, "numpy", "3", compress=False)
    #
    # dump_lmdb(data, "quickle", 2)
    # dump_rocksdb(data, "quickle", 2)

    # Wiki Items data
    # limit = 3000
    # print(f"\nWiki Item data: {limit}")
    # db_items = MItem()
    # data = []
    # for i, i_wd in enumerate(db_items.keys()):
    #     if i >= limit:
    #         break
    #     data.append((i_wd, db_items.get_item(i_wd)))
    # data.sort(key=lambda x: x[0])

    # dump_lmdb(data, "pickle", "3", compress=False)
    # dump_lmdb(data, "pickle", "3_compress", compress=True)
    # dump_rocksdb(data, "pickle", "3", compress=False)
    # dump_rocksdb(data, "pickle", "3_compress", compress=True)

    # dump_lmdb(data, "msgpack", "4", compress=False)
    # dump_lmdb(data, "msgpack", "4_compress", compress=True)
    # dump_rocksdb(data, "msgpack", "4", compress=False)
    # dump_rocksdb(data, "msgpack", "4_compress", compress=True)

    # dump_lmdb(data, "quickle", "5", compress=False)
    # dump_lmdb(data, "quickle", "5_compress", compress=True)
    # dump_rocksdb(data, "quickle", "5", compress=False)
    # dump_rocksdb(data, "quickle", "5_compress", compress=True)

    # dump_lmdb(data, "orjson", "6", compress=False)
    # dump_lmdb(data, "orjson", "6_compress", compress=True)
    # dump_rocksdb(data, "orjson", "6", compress=False)
    # dump_rocksdb(data, "orjson", "6_compress", compress=True)


def test_loop_2():
    def _get_dump_obj(k, v):
        return (
            str(k).encode(cf.ENCODING)[: cf.LMDB_MAX_KEY],
            pickle.dumps(v),  # zlib.compress(pickle.dumps(v))
        )

    data = {i: i + 1 for i in range(100000)}
    new_data = []
    buff_size = 20000
    while data:
        result = dict(islice(data.items(), buff_size))
        new_data.extend([_get_dump_obj(k, v) for k, v in result.items()])
        for k in result:
            del data[k]


def call_compress(function, org_data, level=0, is_pickle=True, is_numpy=False):
    org_len = len(pickle.dumps(org_data))

    start = time()
    data = org_data

    if is_numpy:
        data = np.array(list(data), dtype=np.uintc).tobytes()

    if is_pickle:
        data = msgpack.packb(data)
        # data = pickle.dumps(org_data)

    if function is None:
        compressed_obj = data
    elif function == frame:
        compressed_obj = function.compress(data, compression_level=level)
    else:
        compressed_obj = function.compress(data)

    len_compressed_obj = len(compressed_obj)

    compress_time = len_compressed_obj / (time() - start) / 1048576

    ratio = len_compressed_obj / org_len

    start = time()
    if function is None:
        decompress_obj = compressed_obj
    else:
        decompress_obj = function.decompress(compressed_obj)

    if is_pickle:
        decompress_obj = msgpack.unpackb(decompress_obj)
        # decompress_data = pickle.loads(decompress_obj)
    if is_numpy:
        # decompress_data = array.array("I", compressed_obj)
        decompress_obj = np.frombuffer(decompress_obj, dtype=np.uint32).flat

    decompress_time = len_compressed_obj / (time() - start) / 1048576
    if set(decompress_obj) != set(org_data):
        print("Error")
    return f"Ratio:{ratio:.4f}[{org_len}/{len(compressed_obj)}] | Compress:{compress_time:.2f}MiB/s | Decompress:{decompress_time:.2f}MiB/s"


def call_compress_bytes(data, compress=False, use_numpy=False, data_pickle_len=0):
    start = time()

    if use_numpy:
        data_np = np.array(list(data), dtype=np.uint32)
        compressed_obj = data_np.tobytes()
    else:
        compressed_obj = data

    if compress:
        compressed_obj = msgpack.packb(compressed_obj)
        # compressed_obj = zlib.compress(compressed_obj)
        # compressed_obj = snappy.(compressed_obj)
    compress_time = data_pickle_len / (time() - start) / 1048576
    ratio = data_pickle_len / len(compressed_obj)
    compress_n = len(compressed_obj)

    start = time()
    if compress:
        # compressed_obj = zlib.decompress(compressed_obj)
        compressed_obj = msgpack.unpackb(compressed_obj)
    if use_numpy:
        decompress_data = set(np.frombuffer(compressed_obj, dtype=np.uint32).flat)
    else:
        decompress_data = compressed_obj

    decompress_time = data_pickle_len / (time() - start) / 1048576

    if set(decompress_data) != set(data):
        print("Error")
    return f"Ratio:{ratio:.4f}[{data_pickle_len}/{compress_n}] | Compress:{compress_time:.2f}MiB/s | Decompress:{decompress_time:.2f}MiB/s"


def call_pickle(data):
    start = time()

    compressed_obj = pickle.dumps(data)
    data_pickle_len = len(compressed_obj)

    compress_time = data_pickle_len / (time() - start) / 1048576
    ratio = data_pickle_len / data_pickle_len

    start = time()
    decompress_data = pickle.loads(compressed_obj)
    decompress_time = data_pickle_len / (time() - start) / 1048576
    if set(decompress_data) != set(data):
        print("Error")
    return f"Ratio:{ratio:.4f}[{len(compressed_obj)}/{data_pickle_len}] | Compress:{compress_time:.2f}MiB/s | Decompress:{decompress_time:.2f}MiB/s"


def test_compression():
    data = list({random.randint(0, 912162600) for _ in range(1000000)})
    print(len(data))

    print(call_pickle(data) + "\tpickle")

    print(call_compress(None, data, is_pickle=True, is_numpy=False) + "\tnone\tpickle")
    print(call_compress(None, data, is_pickle=False, is_numpy=True) + "\tnone\tnumpy")
    print(
        call_compress(None, data, is_pickle=True, is_numpy=True)
        + "\tnone\tnumpy\tpickle"
    )

    # print("snappy  : " + call_compress(snappy, data))
    # print("snappy  : " + call_compress(snappy, data, is_pickle=True, is_numpy=False))
    # print("snappy  : " + call_compress(snappy, data, is_pickle=False, is_numpy=True))

    print(call_compress(lzma, data, is_pickle=True, is_numpy=False) + "\tlzma\tpickle")
    print(call_compress(lzma, data, is_pickle=False, is_numpy=True) + "\tlzma\tnumpy")
    print(
        call_compress(lzma, data, is_pickle=True, is_numpy=True)
        + "\tlzma\tnumpy\tpickle"
    )

    print(
        call_compress(brotli, data, is_pickle=True, is_numpy=False) + "\tbrotli\tpickle"
    )
    print(
        call_compress(brotli, data, is_pickle=False, is_numpy=True) + "\tbrotli\tnumpy"
    )
    print(
        call_compress(brotli, data, is_pickle=True, is_numpy=True)
        + "\tbrotli\tnumpy\tpickle"
    )

    #
    print(call_compress(gzip, data, is_pickle=True, is_numpy=False) + "\tgzip\tpickle")
    print(call_compress(gzip, data, is_pickle=False, is_numpy=True) + "\tgzip\tnumpy")
    print(
        call_compress(gzip, data, is_pickle=True, is_numpy=True)
        + "\tgzip\tnumpy\tpickle"
    )
    #
    for i in range(17):
        print(
            call_compress(frame, data, i, is_pickle=True, is_numpy=False)
            + f"\tlz4-{i:2}\tpickle"
        )
        print(
            call_compress(frame, data, i, is_pickle=False, is_numpy=True)
            + f"\tlz4-{i:2}\tnumpy"
        )


def test_rocks_db(db_name="english", max_dis=2, prefix_len=14):
    dir_db = f"{cf.DIR_MODELS}/deletes_{db_name}_d{max_dis}_pl{prefix_len}"
    rock_db = DBItemRocks(dir_db + ".rock")
    lmdb_db = DBItemDefault(dir_db + ".lmdb")
    encode = "bytes_set"
    for k, v in tqdm(lmdb_db.items(mode=encode), total=lmdb_db.size()):
        rock_db.put_item(k, v, encoder=encode)

    rock_db.save_batch()


def test_lmdb_integerkey(max_list=500000, max_item=100):
    data = {
        random.randint(0, 912162600): list(
            {random.randint(0, 912162600) for _ in range(random.randint(1, max_item))}
        )
        for _ in range(max_list)
    }
    print(f"Numerical data: {len(data)}")
    data = sorted(data.items(), key=lambda x: x[0])

    start = time()
    env_1 = DBItem(f"{cf.DIR_MODELS}/test_db_1.lmdb", max_db=1)
    db_1 = env_1.env.open_db(b"__test__", integerkey=True)
    env_1.write_bulk_with_buffer(
        env_1.env,
        db_1,
        data,
        sort_key=False,
        integerkey=True,
        bytes_value=cf.ToBytesType.INT_LIST,
        compress_value=False,
    )
    write_time = time() - start

    start = time()
    for k, v in data:
        tmp = env_1.get_value(
            db_1,
            k,
            integerkey=True,
            bytes_value=cf.ToBytesType.INT_LIST,
            compress_value=False,
        )
        if set(tmp) != set(v):
            print(f"Error get")
            break
    read_time = time() - start
    print(f"LMDB integerkey\t{read_time:.2f}s\t{write_time:.2f}s")
    env_1.close()

    start = time()
    env_2 = DBItem(f"{cf.DIR_MODELS}/test_db_2.lmdb", max_db=1)
    db_2 = env_2.env.open_db(b"__test__", integerkey=False)
    data1 = [[f"Q{k}", v] for k, v in data]
    env_2.write_bulk_with_buffer(
        env_2.env,
        db_2,
        data1,
        sort_key=False,
        integerkey=False,
        bytes_value=cf.ToBytesType.INT_LIST,
        compress_value=False,
    )
    write_time = time() - start

    start = time()
    for k, v in data:
        tmp = env_2.get_value(
            db_2,
            "Q" + str(k),
            integerkey=False,
            bytes_value=cf.ToBytesType.INT_LIST,
            compress_value=False,
        )
        if set(tmp) != set(v):
            print(f"Error get")
            break
    read_time = time() - start
    print(f"LMDB\t{read_time:.2f}s\t{write_time:.2f}s")
    env_2.close()

    """
    Numerical data: 999448
    --> INI LIST is better
    """


def test_lmdb_int_list_or_integerdup(max_list=1000000, max_item=500):
    data = {
        random.randint(0, 912162600): list(
            {random.randint(0, 912162600) for _ in range(random.randint(1, max_item))}
        )
        for _ in range(max_list)
    }
    print(f"Numerical data: {len(data)}")
    data = sorted(data.items(), key=lambda x: x[0])

    env_1 = DBItem(f"{cf.DIR_MODELS}/test_db_1.lmdb", max_db=1)
    db_1 = env_1.env.open_db(b"__test__", integerkey=True)

    start = time()
    env_1.write_bulk_with_buffer(
        env_1.env,
        db_1,
        data,
        sort_key=False,
        integerkey=True,
        bytes_value=cf.ToBytesType.INT_LIST,
        compress_value=False,
        show_progress=False,
        step=10000,
    )
    write_time = time() - start

    start = time()
    for k, v in data:
        tmp = env_1.get_value(
            db_1,
            k,
            integerkey=True,
            bytes_value=cf.ToBytesType.INT_LIST,
            compress_value=False,
        )
        if set(tmp) != set(v):
            print(f"Error get")
            break
    read_time = time() - start

    print(f"LMDB integerkey INT_LIST\t{read_time:.2f}s\t{write_time:.2f}s")

    start = time()
    env_2 = DBDupInt(f"{cf.DIR_MODELS}/test_db_2.lmdb", db_name=b"__test__")

    env_2.put_multi_with_buffer(data, sort_key=False)
    write_time = time() - start

    start = time()
    for k, v in data:
        tmp = env_2.get_multi(k)
        if set(tmp) != set(v):
            print(f"Error get")
            break
    read_time = time() - start
    print(f"LMDB integerkey integerdup \t{read_time:.2f}\t{write_time:.2f}")
    """
    Numerical data: 999448
    LMDB integerkey INT_LIST	47.38s	31.56s 1.76G
    LMDB integerkey integerdup 	221.96	295.93 1.85G
    --> INI LIST is better
    """


if __name__ == "__main__":
    # test_db()
    # test_compression()
    # test_lmdb_int_list_or_integerdup()
    test_lmdb_integerkey()  # max_list=10, max_item=5
