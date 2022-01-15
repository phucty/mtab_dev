import copy
import pickle
import random
import struct
import zlib

import msgpack
import numpy as np
from collections import defaultdict, Counter
from contextlib import closing
from time import sleep
import sys
import lmdb
from tqdm import tqdm

from lz4 import frame
import m_config as cf
from api.resources.m_item import MItem

from api.utilities import m_io as iw
import gc
from api.utilities import m_utils as ul
import os
import rocksdb


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def deserialize_key(key, integerkey=False):
    if not integerkey:
        return key.decode(cf.ENCODING)
    return struct.unpack("I", key)[0]


def deserialize_value(value, mode="msgpack", compress=True):
    if mode == "numpy":
        value = np.frombuffer(value, dtype=np.uint32).flat
    else:  # mode == "msgpack"
        if compress:
            value = frame.decompress(value)
        value = msgpack.unpackb(value)
    return value


def serialize_key(key, integerkey=False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(cf.ENCODING)[: cf.LMDB_MAX_KEY]
    return struct.pack("I", key)


def serialize_value(value, mode="msgpack", compress=True):
    if mode == "numpy":
        value = np.array(list(value), dtype=np.uint32).tobytes()
    else:  # mode == "msgpack"
        value = msgpack.packb(value, default=set_default)
        if compress:
            value = frame.compress(value)

    return value


def serialize(key, value, integerkey=False, mode="msgpack", compress=True):
    return serialize_key(key, integerkey), serialize_value(value, mode, compress)


class DBSubspace:
    def __init__(self, db_dir, read_only=True):
        iw.create_dir(db_dir)
        self.db_name = db_dir
        self.db = rocksdb.DB(
            db_dir,
            rocksdb.Options(
                create_if_missing=True,
                write_buffer_size=cf.SIZE_512MB,
                compression=rocksdb.CompressionType.lz4_compression,
                # inplace_update_support=False,
                # max_open_files=300000,
                # max_write_buffer_number=3,
                # target_file_size_base=67108864,
                # table_factory=rocksdb.BlockBasedTableFactory(
                #     filter_policy=rocksdb.BloomFilterPolicy(10),
                #     block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
                #     block_cache_compressed=rocksdb.LRUCache(512 * (1024 ** 2)),
                # ),
            ),
            read_only=read_only,
        )

    def get_size(self):
        return int(
            self.db.get_property(b"rocksdb.estimate-num-keys").decode(cf.ENCODING)
        )

    def get_item(self, key, integerkey=False, mode="msgpack", compress=True):
        key = serialize_key(key, integerkey)
        item = self.db.get(key)
        if item is None:
            return None
        return deserialize_value(item, mode, compress)

    def get_items(self, keys, integerkey=False, mode="msgpack", compress=True):
        keys_s = [serialize_key(k, integerkey) for k in keys]
        responds = {
            k: deserialize_value(v, mode, compress)
            for k, v in self.db.multi_get(keys_s)
            if v is not None
        }
        return responds

    def set_item(self, key, value, integerkey=False, mode="msgpack", compress=True):
        key = serialize_key(key, integerkey)
        value = serialize_value(value, mode, compress)
        self.db.put(key, value)

    def set_items(
        self,
        items,
        integerkey=False,
        mode="msgpack",
        compress=True,
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
    ):
        if isinstance(items, dict):
            items = sorted(items.items(), key=lambda x: x[0])
        else:
            items = items.sort(key=lambda x: x[0])

        batch = rocksdb.WriteBatch()
        buff_size = 0
        p_bar = None

        def update_desc():
            return f"Saving: buff:{buff_size/buff_limit * 100:.2f}%"

        if show_progress:
            p_bar = tqdm(desc=update_desc())
        for i, (k, v) in enumerate(items):
            k = serialize_key(k, integerkey)
            v = serialize_value(v, mode, compress)
            buff_size += len(k) + len(v)
            batch.put(k, v)

            if buff_size >= buff_limit:
                self.db.write(batch)
                batch = rocksdb.WriteBatch()
                buff_size = 0

            if show_progress and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())

        if buff_size:
            self.db.write(batch)
            p_bar.set_description(desc=update_desc())

        if show_progress:
            p_bar.close()

    def remove_value(self, key, integerkey=False):
        key = serialize_key(key, integerkey)
        self.db.delete(key)

    def keys(self, integerkey=False):
        iterator = self.db.iterkeys()
        iterator.seek_to_first()
        for k in iterator:
            yield deserialize_key(k, integerkey)

    def values(self, mode="msgpack", compress=True):
        iterator = self.db.itervalues()
        iterator.seek_to_first()
        for k in iterator:
            yield deserialize_value(k, mode, compress)

    def items(self, integerkey=False, mode="msgpack", compress=True):
        iterator = self.db.iteritems()
        iterator.seek_to_first()
        for k, v in iterator:
            yield deserialize_key(k, integerkey), deserialize_value(v, mode, compress)


class DBSpace:
    def __init__(self, db_dir):
        self.db_items = DBSubspace(db_dir + "/items")
        self.db_nums = DBSubspace(db_dir + "/nums")
        self.db_test = DBSubspace(db_dir + "/test")


def test_db():
    limit = 3000
    print(f"\nWiki Item data: {limit}")
    test_db = DBSpace(db_dir=cf.DIR_TEST_ROCKSDB)
    db_items = MItem()
    data_items = []
    for i, i_wd in enumerate(db_items.keys()):
        if i >= limit:
            break
        data_items.append((i_wd, db_items.get_item(i_wd)))
    data_items.sort(key=lambda x: x[0])

    test_db.db_items.set_items(data_items)

    data_nums = {
        random.randint(0, 912162600): list(
            {random.randint(0, 912162600) for _ in range(random.randint(0, 500))}
        )
        for _ in range(1000)
    }
    data_nums = sorted(data_nums.items(), key=lambda x: x[0])
    test_db.db_nums.set_items(data_nums, integerkey=True, mode="numpy")

    loaded_data_items = list(test_db.db_items.items())
    loaded_data_nums = list(test_db.db_nums.items(integerkey=True, mode="numpy"))
    debug = 1


if __name__ == "__main__":
    test_db()
