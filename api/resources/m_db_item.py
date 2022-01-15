import gc
import os
import struct
import zlib
from collections import defaultdict, OrderedDict
from contextlib import closing

import lmdb
import msgpack
import numpy as np
from lz4 import frame
from tqdm import tqdm

import m_config as cf
from api import m_f
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


def set_default(obj):
    if isinstance(obj, set):
        return sorted(list(obj))
    raise TypeError


def deserialize_key(key, integerkey=False):
    if not integerkey:
        return key.decode(cf.ENCODING)
    try:
        return struct.unpack("I", key)[0]
    except Exception:
        iw.print_status(key)
        raise Exception


def deserialize_value(value, bytes_value=cf.ToBytesType.OBJ, compress_value=True):
    if bytes_value == cf.ToBytesType.INT_LIST:
        value = np.frombuffer(value, dtype=np.uint32).tolist()
    else:  # mode == "msgpack"
        if compress_value:
            try:
                value = frame.decompress(value)
            except RuntimeError:
                pass
        value = msgpack.unpackb(value)
    return value


def deserialize(
    key, value, integerkey=False, bytes_value=cf.ToBytesType.OBJ, compress_value=True
):
    return (
        deserialize_key(key, integerkey=integerkey),
        deserialize_value(
            value, bytes_value=bytes_value, compress_value=compress_value
        ),
    )


def serialize_key(key, integerkey=False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(cf.ENCODING)[: cf.LMDB_MAX_KEY]
    return struct.pack("I", key)


def serialize_value(value, bytes_value=cf.ToBytesType.OBJ, compress_value=True):
    if bytes_value == cf.ToBytesType.INT_LIST:
        value = np.array(list(value), dtype=np.uint32).tobytes()
    else:  # mode == "msgpack"
        value = msgpack.packb(value, default=set_default)
        if compress_value:
            value = frame.compress(value)
    return value


def serialize(
    key, value, integerkey=False, bytes_value=cf.ToBytesType.OBJ, compress_value=True
):
    return (
        serialize_key(key, integerkey=integerkey),
        serialize_value(value, bytes_value=bytes_value, compress_value=compress_value),
    )


def preprocess_data_before_dump(
    data,
    integerkey=False,
    bytes_value=cf.ToBytesType.OBJ,
    compress_value=True,
    sort_key=True,
):
    if isinstance(data, dict):
        first_key, first_value = None, None
        for k, v in data.items():
            first_key, first_value = k, v
            break
    else:
        first_key = data[0][0]
        first_value = data[0][0]

    if not ul.is_byte_obj(first_key) and not ul.is_byte_obj(first_value):
        is_serialized = False
    else:
        is_serialized = True

    def iter_items():
        if isinstance(data, dict):
            for k, v in data.items():
                yield k, v
        else:
            for k, v in data:
                yield k, v

    iter_data = iter_items()

    if isinstance(data, dict) or not is_serialized:
        data = [
            serialize(
                k,
                v,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )
            if not is_serialized
            else (k, v)
            for k, v in iter_data
            if k is not None
        ]

    if sort_key:
        data.sort(key=lambda x: x[0])

    return data


class DBItem:
    def __init__(self, db_file, max_db, map_size=cf.LMDB_MAP_SIZE):
        self._db_file = db_file
        iw.create_dir(self._db_file)
        self._max_db = max_db
        self._env = lmdb.open(
            self._db_file,
            map_async=True,
            map_size=map_size,
            subdir=False,
            lock=False,
            max_dbs=max_db,
        )
        self._env.set_mapsize(map_size)

    @property
    def env(self):
        return self._env

    def get_map_size(self):
        tmp = self._env.info().get("map_size")
        if not tmp:
            return "Unknown"
        return f"{tmp / cf.SIZE_1GB:.0f}GB"

    def close(self):
        self._env.close()

    def copy_lmdb(self):
        """
        Copy current env to new one (reduce file size)
        :return:
        :rtype:
        """
        iw.print_status(self._env.stat())
        if self._env.stat().get("map_size"):
            iw.print_status("%.2fGB" % (self._env.stat()["map_size"] % cf.SIZE_1GB))
        new_dir = self._db_file + ".copy"
        self._env.copy(path=new_dir, compact=True)
        try:
            if os.path.exists(self._db_file):
                os.remove(self._db_file)
        except Exception as message:
            iw.print_status(message)
        os.rename(new_dir, self._db_file)

    def get_iter_with_prefix(
        self,
        db,
        prefix,
        integerkey=False,
        get_values=True,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=True,
    ):
        with self._env.begin(db=db, write=False) as txn:
            cur = txn.cursor()
            prefix = serialize_key(prefix, integerkey=integerkey)
            cur.set_range(prefix)

            while cur.key().startswith(prefix) is True:
                try:
                    if cur.key() and not cur.key().startswith(prefix):
                        continue
                    key = deserialize_key(cur.key(), integerkey=integerkey)
                    if get_values:
                        value = deserialize_value(
                            cur.value(),
                            bytes_value=bytes_value,
                            compress_value=compress_value,
                        )
                        yield key, value
                    else:
                        yield key
                except Exception as message:
                    iw.print_status(message)
                cur.next()

    def is_available(self, db, key_obj, integerkey=False):
        with self._env.begin(db=db) as txn:
            key_obj = serialize_key(key_obj, integerkey=integerkey)
            if key_obj:
                try:
                    value_obj = txn.get(key_obj)
                    if value_obj:
                        return True
                except Exception as message:
                    iw.print_status(message)
        return False

    def get_value(
        self,
        db,
        key_obj,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=True,
    ):
        """
        Get value from a database. The key_obj could be one key or multiple key in list, set, or tuple.
        There are two serialization mode (convert python object to bytes): msgpack and numpy
        cf.ToBytesMode.OBJ: is used with normal literal values
        cf.ToBytesMode.INT_LIST: is used with integer values (store index values)
        compress with lz4 as the default setting to save space. The compress is only used with msgpack.
        :param db:
        :type db:
        :param key_obj:
        :type key_obj:
        :param bytes_value:
        :type bytes_value:
        :param integerkey:
        :type integerkey:
        :param compress_value:
        :type compress_value:
        :return:
        :rtype:
        """
        with self._env.begin(db=db) as txn:
            if (
                isinstance(key_obj, list)
                or isinstance(key_obj, set)
                or isinstance(key_obj, tuple)
            ):
                key_obj = [serialize_key(k, integerkey=integerkey) for k in key_obj]
                responds = dict()
                for k, v in txn.cursor(db).getmulti(key_obj):
                    if v:
                        try:
                            k, v = deserialize(
                                k,
                                v,
                                integerkey=integerkey,
                                bytes_value=bytes_value,
                                compress_value=compress_value,
                            )
                            responds[k] = v
                        except Exception as message:
                            iw.print_status(message)
            else:
                key_obj = serialize_key(key_obj, integerkey=integerkey)
                responds = None
                if key_obj:
                    try:
                        value_obj = txn.get(key_obj)
                        if value_obj:
                            responds = deserialize_value(
                                value_obj,
                                bytes_value=bytes_value,
                                compress_value=compress_value,
                            )
                    except Exception as message:
                        iw.print_status(message)
        return responds

    def head(
        self,
        db,
        n,
        bytes_value=cf.ToBytesType.OBJ,
        from_i=0,
        integerkey=False,
        compress_value=True,
    ):
        respond = defaultdict()
        for i, (k, v) in enumerate(
            self.get_db_iter(
                db,
                bytes_value=bytes_value,
                from_i=from_i,
                integerkey=integerkey,
                compress_value=compress_value,
            )
        ):
            respond[k] = v
            if i == n - 1:
                break
        return respond

    def get_db_iter(
        self,
        db,
        get_values=True,
        deserialize_obj=True,
        from_i=0,
        to_i=-1,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=True,
    ):
        if to_i == -1:
            to_i = self.get_db_size(db)

        with self._env.begin(db=db) as txn:
            cur = txn.cursor()
            for i, db_obj in enumerate(cur.iternext(values=get_values)):
                if i < from_i:
                    continue
                if i >= to_i:
                    break

                if get_values:
                    key, value = db_obj
                else:
                    key = db_obj
                try:
                    if deserialize_obj:
                        key = deserialize_key(key, integerkey=integerkey)
                        if get_values:
                            value = deserialize_value(
                                value,
                                bytes_value=bytes_value,
                                compress_value=compress_value,
                            )
                    if get_values:
                        yield (key, value)
                    else:
                        yield key
                # Todo: handlers
                except UnicodeDecodeError:
                    iw.print_status(f"UnicodeDecodeError: {i}")
                except Exception:
                    iw.print_status(i)
                    raise Exception

    def get_db_size(self, db):
        with self._env.begin(db=db) as txn:
            return txn.stat()["entries"]

    def delete(self, db, key, integerkey=False, with_prefix=False):
        if not (
            isinstance(key, list) or isinstance(key, set) or isinstance(key, tuple)
        ):
            key = [key]

        if with_prefix:
            true_key = set()
            for k in key:
                for tmp_k in self.get_iter_with_prefix(
                    db, k, integerkey=integerkey, get_values=False
                ):
                    true_key.add(tmp_k)
            if true_key:
                key = list(true_key)

        deleted_items = 0
        with self.env.begin(db=db, write=True, buffers=True) as txn:
            for k in key:
                try:
                    status = txn.delete(serialize_key(k, integerkey))
                    if status:
                        deleted_items += 1
                except Exception as message:
                    iw.print_status(message)
        return deleted_items

    @staticmethod
    def write_bulk(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=True,
        one_sample_write=False,
    ):
        data = preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            compress_value=compress_value,
            sort_key=sort_key,
        )
        added_items = 0
        try:
            with env.begin(db=db, write=True, buffers=True) as txn:
                if not one_sample_write:
                    _, added_items = txn.cursor().putmulti(data)
                else:
                    for k, v in data:
                        txn.put(k, v)
                        added_items += 1
        except lmdb.MapFullError:
            curr_limit = env.info()["map_size"]
            new_limit = curr_limit + cf.SIZE_1GB * 5
            env.set_mapsize(new_limit)
            return DBItem.write_bulk(env, db, data, sort_key=False)
        except lmdb.BadValsizeError:
            iw.print_status(lmdb.BadValsizeError)
        except lmdb.BadTxnError:
            if one_sample_write:
                return DBItem.write_bulk(
                    env, db, data, sort_key=False, one_sample_write=True,
                )
        except Exception:
            raise Exception
        return added_items

    @staticmethod
    def write_bulk_with_buffer(
        env,
        db,
        data,
        sort_key=True,
        integerkey=False,
        bytes_value=cf.ToBytesType.OBJ,
        compress_value=True,
        show_progress=True,
        step=10000,
        message="DB Write",
    ):
        data = preprocess_data_before_dump(
            data,
            bytes_value=bytes_value,
            integerkey=integerkey,
            compress_value=compress_value,
            sort_key=sort_key,
        )

        def update_desc():
            return f"{message} buffer: {buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        p_bar = None
        buff_size = 0
        i_pre = 0
        if show_progress:
            p_bar = tqdm(total=len(data))

        for i, (k, v) in enumerate(data):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
            buff_size += len(k) + len(v)

            if buff_size >= cf.LMDB_BUFF_BYTES_SIZE:
                c = DBItem.write_bulk(env, db, data[i_pre:i], sort_key=False)
                if c != len(data[i_pre:i]):
                    iw.print_status(
                        f"WriteError: Missing data. Expected: {len(data[i_pre:i])} - Actual: {c}"
                    )
                i_pre = i
                buff_size = 0

        if buff_size:
            DBItem.write_bulk(env, db, data[i_pre:], sort_key=False)

        if show_progress:
            p_bar.update(len(data) % step)
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def update_bulk_with_buffer(
        self,
        env,
        db,
        data,
        update_type=cf.DBUpdateType.SET,
        integerkey=False,
        bytes_value=cf.ToBytesType.INT_LIST,
        compress_value=True,
        show_progress=True,
        step=10000,
        message="",
        buff_limit=cf.LMDB_BUFF_BYTES_SIZE,
    ):
        buff = []
        p_bar = None
        c_skip, c_update, c_new, c_buff = 0, 0, 0, 0

        def update_desc():
            return (
                f"{message}"
                f"|Skip:{c_skip:,}"
                f"|New:{c_new:,}"
                f"|Update:{c_update:,}"
                f"|Buff:{c_buff / buff_limit * 100:.0f}%"
            )

        if show_progress:
            p_bar = tqdm(total=len(data), desc=update_desc())

        for i, (k, v) in enumerate(data.items()):
            if show_progress and i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(update_desc())

            db_obj = self.get_value(
                db,
                k,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )
            if update_type == cf.DBUpdateType.SET:
                if db_obj:
                    db_obj = set(db_obj)
                    v = set(v)
                    if db_obj and len(v) <= len(db_obj) and db_obj.issuperset(v):
                        c_skip += 1
                        continue
                    if db_obj:
                        v.update(db_obj)
                        c_update += 1
                    else:
                        c_new += 1
                else:
                    c_new += 1
            else:
                if db_obj:
                    v += db_obj
                    c_update += 1
                else:
                    c_new += 1

            k, v = serialize(
                k,
                v,
                integerkey=integerkey,
                bytes_value=bytes_value,
                compress_value=compress_value,
            )

            c_buff += len(k) + len(v)
            buff.append((k, v))

            if c_buff >= buff_limit:
                DBItem.write_bulk(env, db, buff)
                buff = []
                c_buff = 0

        if buff:
            DBItem.write_bulk(env, db, buff)
        if show_progress:
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def modify_db_compress_value(
        self,
        c_db,
        c_integerkey=False,
        c_bytes_value=cf.ToBytesType.OBJ,
        c_compress_value=True,
        n_integerkey=False,
        n_bytes_value=cf.ToBytesType.OBJ,
        n_compress_value=False,
        step=1000,
    ):
        buff = []
        buff_size = 0

        def update_desc():
            return f"buff:{buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"

        p_bar = tqdm(total=self.get_db_size(c_db))
        for i, (k, v) in enumerate(
            self.get_db_iter(
                c_db,
                integerkey=c_integerkey,
                bytes_value=c_bytes_value,
                compress_value=c_compress_value,
            )
        ):
            k, v = serialize(
                k,
                v,
                integerkey=n_integerkey,
                bytes_value=n_bytes_value,
                compress_value=n_compress_value,
            )
            buff_size += len(k) + len(v)
            buff.append((k, v))
            if buff_size >= cf.LMDB_BUFF_BYTES_SIZE:
                self.write_bulk(self.env, c_db, buff)
                buff = []
                buff_size = 0
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())
        if buff:
            self.write_bulk(self.env, c_db, buff)

    def drop_db(self, db):
        with self._env.begin(write=True) as in_txn:
            in_txn.drop(db)
            print(in_txn.stat())

    def copy_new_file(
        self, db_names, map_size, buff_size=cf.SIZE_512MB, compress=True, message=False,
    ):
        new_dir = self._db_file + ".copy"
        print(self._env.info())
        iw.print_status("%.2fGB" % (self._env.info()["map_size"] / cf.SIZE_1GB))
        save_drive = 0
        with closing(
            lmdb.open(
                new_dir,
                subdir=False,
                map_async=True,
                lock=False,
                map_size=map_size,
                max_dbs=len(db_names),
            )
        ) as env:
            print(env.info())
            for db_name_src, copy_args in db_names.items():
                db_name_tar = copy_args["name"]

                org_db = self._env.open_db(db_name_src)
                is_integerkey = False
                if copy_args.get("integerkey"):
                    is_integerkey = copy_args["integerkey"]
                tar_db = env.open_db(db_name_tar, integerkey=is_integerkey)

                org_db_n = self.get_db_size(org_db)

                iw.print_status(
                    f"\nCopy: {self._db_file} - {str(db_name_src)} --> {str(db_name_tar)}"
                )

                def update_desc():
                    if compress:
                        return f"Save: {save_drive / cf.SIZE_1GB:.2f}GB|buff:{len_buff/cf.SIZE_1MB}MB"
                    else:
                        return f"buff:{len_buff/cf.SIZE_1MB:.2f}MB"

                with self._env.begin(db=org_db) as txn:
                    cur = txn.cursor()
                    buff = []
                    len_buff = 0
                    if message:
                        p_bar = tqdm(desc=update_desc(), total=org_db_n)
                    for i, (key, value) in enumerate(iter(cur)):
                        if message:
                            p_bar.update()
                        if message and i and i % 100000 == 0:
                            p_bar.set_description(desc=update_desc())
                        if compress:
                            old_size = len(value)
                            value = zlib.compress(value)
                            save_drive += old_size - len(value)
                        buff.append((key, value))

                        len_buff += len(value) + len(key)

                        if len_buff > buff_size:
                            if message:
                                p_bar.set_description(desc=update_desc())
                            DBItem.write_bulk(env, tar_db, buff)
                            buff.clear()
                            len_buff = 0
                            gc.collect()
                    if buff:
                        if message:
                            p_bar.set_description(desc=update_desc())
                        DBItem.write_bulk(env, tar_db, buff)
                        buff.clear()
                        gc.collect()
                    if message:
                        p_bar.close()
            iw.print_status(env.info())
            iw.print_status("%.2fGB" % (env.info()["map_size"] / cf.SIZE_1GB))


class DBItemDefault(DBItem):
    def __init__(self, db_file, map_size=cf.LMDB_MAP_SIZE):
        super().__init__(db_file=db_file, max_db=1, map_size=map_size)
        self.db = self._env.open_db(b"__items__")

    def get_delete_candidates(
        self, candidate, phrase_len, max_edit_distance, get_label=False
    ):
        prefix = f"{candidate}|"
        label_len = []
        candidate_len = len(candidate)
        for k in self.get_iter_with_prefix(
            self.db, prefix, get_values=False, bytes_value=cf.ToBytesType.INT_LIST,
        ):
            try:
                suggestion_len = int(k.replace(prefix, ""))
                if (
                    abs(suggestion_len - phrase_len) > max_edit_distance
                    or suggestion_len < candidate_len
                ):
                    continue
                label_len.append(suggestion_len)
            except Exception as message:
                iw.print_status(message)

        label_len.sort(key=lambda x: abs(x - phrase_len))

        for i in label_len:
            tmp = self.get_value(
                self.db, f"{prefix}{i}", bytes_value=cf.ToBytesType.INT_LIST,
            )
            if tmp:
                if get_label:
                    map_label = m_f.m_item_labels().get_label_from_lid(tmp)
                    for lid, label in map_label.items():
                        yield lid, label
                    # for lid in tmp:
                    #     yield lid, m_f.m_item_labels().get_label_from_lid(lid)
                else:
                    for lid in tmp:
                        yield lid

    def get_item(
        self, text, bytes_value=cf.ToBytesType.OBJ, integerkey=False, compress=True
    ):
        return self.get_value(
            self.db,
            text,
            bytes_value=bytes_value,
            integerkey=integerkey,
            compress_value=compress,
        )

    def items(
        self, bytes_value=cf.ToBytesType.OBJ, from_i=0, integerkey=False, compress=True
    ):
        return self.get_db_iter(
            self.db,
            bytes_value=bytes_value,
            from_i=from_i,
            integerkey=integerkey,
            compress_value=compress,
        )

    def size(self):
        if not self.db:
            return 0
        return self.get_db_size(self.db)


class DBDeletes(DBItem):
    def __init__(self, db_file=cf.DIR_DELETES_DB, buff_size=1e5, buff_save=1e5):
        super().__init__(db_file=db_file, max_db=2)
        self._db_file = db_file
        self._buff_size = buff_size
        self._buff_save = buff_save

        db_dup_dir = db_file + ".dup"
        self._db_dup = DBItemDefault(db_file=db_dup_dir)
        # self._env.open_db(b'__dup__')
        self._buff_dup = defaultdict(int)

        self._db_del = self._env.open_db(b"__del__")
        self._buff_del = defaultdict(set)

        self._buff_count = 0

    def size_deletes(self):
        return self._db_dup.size() + len(self._buff_dup)

    def size_data(self):
        return self.get_db_size(self._db_del) + len(self._buff_del)

    def size_buff(self):
        return self._buff_count

    def _get_dup(self, text, is_add=True):
        v_id = self._buff_dup.get(text)
        if v_id is None:
            v_id = self._db_dup.get_item(text)

        if is_add:
            if v_id is None:
                v_id = 0
            else:
                v_id += 1
            self._buff_dup[text] = v_id
        return v_id

    @staticmethod
    def _key(vocab_id, index):
        return f"{vocab_id}||{index}"

    def _get_key(self, vocab_id):
        index = self._get_dup(vocab_id, is_add=True)
        return self._key(vocab_id, index)

    def get(self, text):
        current_index = self._db_dup.get_item(text)
        responds = set()
        for i in range(current_index + 1):
            v = self.get_value(self._db_del, self._key(text, i))
            if v:
                responds.update(v)
        return responds

    def add(self, key, value):
        self._buff_del[key].add(value)
        self._buff_count += 1

        if len(self._buff_del) + self._buff_count >= self._buff_save:
            # if self._buff_count >= self._buff_save:
            self.save(see_progress=False)

    def build_deletes(self, from_i, max_dis=2, prefix_len=7, min_len=1):
        from api.resources.m_item_labels import MEntityLabels

        e_labels = MEntityLabels(cf.DIR_WIKI_LABELS_ENGLISH)

        def update_desc():
            return (
                f"d={max_dis}|lp={prefix_len}|"
                f"deletes:{self.size_deletes() / 1000000:.1f}M|"
                f"data:{self.size_data() / 1000000:.1f}M|"
                f"buff:{self.size_buff() / 1000000:.1f}M"
            )

        with tqdm(total=e_labels.size_vocab(), desc=update_desc()) as p_bar:
            p_bar.update(from_i)

            for i, (words, words_id) in enumerate(e_labels.iter_vocab(from_i=from_i)):
                p_bar.update()

                # if i < 91814823:  # 4-10: 91814823  8-10: 48855254 2-20: 87885553
                #     continue
                # tmp_punc = ul.norm_text(words, punctuations=True)

                # tmp = ul.norm_text(words, punctuations=False)
                # if i < update_from_id:
                #     continue

                # if i < update_from_id:
                #     tmp_punc = ul.norm_text(words, punctuations=True)
                #     if tmp_punc != words.lower():
                #         tmp = ul.norm_text(words, punctuations=False)
                #         if tmp == tmp_punc:
                #             continue

                # if "\u002D" not in words:
                #     continue

                if ul.is_wd_item(words):
                    continue

                # available = current_db.get_value_default(words[:prefix_len], decoder="bytes_set")
                # if available and words_id in available:
                #     continue

                if len(words) > self._max_length:
                    words = words[: self._max_length]

                edits = ul.delete_edits_prefix(words, max_dis, prefix_len)

                for delete in edits:
                    if len(delete) >= min_len:
                        db_deletes.add(delete, words_id)
                if i and i % 1000 == 0:
                    p_bar.set_description(update_desc())
            db_deletes.save(see_progress=True)

    def save(self, see_progress=False, buff_limit=cf.SIZE_512MB):
        # Save items
        buff = []
        p_bar = None
        n_buff = 0

        def update_desc():
            return f"buff:{n_buff / cf.SIZE_1MB:.0f}MB"

        if see_progress:
            p_bar = tqdm(total=len(self._buff_del))

        # while self._buff_del:
        #     k, v = self._buff_del.popitem()
        for i, (k, v) in enumerate(self._buff_del.items()):
            if see_progress and i % 999 == 0:
                p_bar.update(1000)
                p_bar.set_description(desc=update_desc())

            key_id = self._get_key(k)

            dump_obj = ul.get_dump_obj(key_id, v, encoder="bytes_set")
            if dump_obj:
                buff.append(dump_obj)
                n_buff += len(dump_obj[0]) + len(dump_obj[1])

            if n_buff > buff_limit:
                # p_bar.set_description(update_des())
                DBItem.write_bulk(self._env, self._db_del, buff)
                buff.clear()
                n_buff = 0
                buff = []
                gc.collect()
        if see_progress:
            p_bar.close()

        if buff:
            DBItem.write_bulk(self._env, self._db_del, buff)
            buff.clear()
            gc.collect()
        self._buff_del.clear()
        gc.collect()

        # Save vocab dup
        # DBItem._write_bulk(self._env, self._db_dup, self._buff_dup)
        self.write_bulk_with_buffer(
            env=self._db_dup.env,
            db=self._db_dup.db,
            db_inv=None,
            data=self._buff_dup,
            buff_size=self._buff_size,
            is_message=see_progress,
        )
        self._buff_dup.clear()
        self._buff_count = 0
        gc.collect()
        iw.print_status("")

    def collect(self, d, pl, from_i=0, batch_id=None, map_size=cf.LMDB_MAP_SIZE):
        if batch_id is None:
            copy_dir = self._db_file.replace(f"_dup", "")
        else:
            copy_dir = self._db_file.replace(f"_dup_{batch_id}", "")
        iw.print_status(copy_dir)
        iw.print_status(self.env.info())
        copy_db = DBItemDefault(db_file=copy_dir)
        iw.print_status(copy_db.env.info())
        buff = defaultdict(set)
        buff_c = 0
        copy_db_size = copy_db.size()

        def update_desc():
            return (
                f"d={d}|pl={pl}|"
                f"deletes:{copy_db_size/1000000:.1f}M|"
                f"buff:{len(buff)/1000000:.1f}M|"
                f"items:{buff_c/1000000:.1f}M"
            )

        n = self.size_deletes()
        p_bar = tqdm(total=n, desc=update_desc())
        p_bar.update(from_i)

        for _to_i, (k, k_i) in enumerate(self._db_dup.items(from_i=from_i)):
            if buff and (len(buff) % 1000 == 0 or k == n - 1):
                p_bar.set_description(update_desc())
            p_bar.update()
            v = set()
            for i in range(k_i + 1):
                v_tmp = self.get_value(
                    self._db_del, self._key(k, i), bytes_value="bytes_set"
                )
                if v_tmp:
                    v.update(v_tmp)

            buff_c += len(v)
            buff[k] = v

            if buff_c > self._buff_save:
                copy_db.update_bulk_with_buffer(
                    env=copy_db.env,
                    db=copy_db.db,
                    data=buff,
                    show_progress=True,
                    update_type=cf.DBUpdateType.SET,
                    bytes_value=cf.ToBytesType.INT_LIST,
                )
                copy_db_size = copy_db.size()
                p_bar.set_description(update_desc())
                buff.clear()
                del buff
                buff = defaultdict(set)
                buff_c = 0
                gc.collect()
                iw.print_status("")

        if buff:
            copy_db.update_bulk_with_buffer(
                env=copy_db.env,
                db=copy_db.db,
                data=buff,
                show_progress=True,
                update_type=cf.DBUpdateType.SET,
                bytes_value=cf.ToBytesType.INT_LIST,
            )
        p_bar.close()
        iw.delete_file(self._db_file)
        iw.delete_file(self._db_file + ".dup")


def merge_lmdb(d=4, pl=10, buff_save=1e8):
    db_sou = DBItemDefault(f"{cf.DIR_MODELS}/deletes_d{d}_pl{pl}.lmdb")
    db_tar = DBItemDefault(f"{cf.DIR_MODELS}/deletes_d{d}_pl{pl}_1.lmdb")
    buff = defaultdict(set)
    buff_c = 0
    copy_db_size = db_tar.size()

    def update_desc():
        return (
            f"d={d}|pl={pl}|"
            f"deletes:{copy_db_size / 1000000:.1f}M|"
            f"buff:{len(buff) / 1000000:.1f}M|"
            f"items:{buff_c / 1000000:.1f}M"
        )

    p_bar = tqdm(total=db_sou.size(), desc=update_desc())
    for i, (k, v) in enumerate(db_sou.items(mode="bytes_set")):
        if buff and len(buff) % 10000 == 0:
            p_bar.set_description(update_desc())
        p_bar.update()

        buff_c += len(v)
        buff[k] = v

        if buff_c > buff_save:
            db_tar.update_bulk_with_buffer(
                env=db_tar.env,
                db=db_tar.db,
                data=buff,
                show_progress=True,
                update_type=cf.DBUpdateType.SET,
                bytes_value=cf.ToBytesType.INT_LIST,
            )
            copy_db_size = db_tar.size()
            p_bar.set_description(update_desc())
            buff.clear()
            del buff
            buff = defaultdict(set)
            buff_c = 0
            gc.collect()

    if buff:
        db_tar.update_bulk_with_buffer(
            env=db_tar.env,
            db=db_tar.db,
            data=buff,
            show_progress=True,
            update_type=cf.DBUpdateType.SET,
            bytes_value=cf.ToBytesType.INT_LIST,
        )


if __name__ == "__main__":
    merge_lmdb(4, 10)
    # max_dis = 4
    # predix_len = 10
    # db = DBItem(db_file=f"{cf.DIR_MODELS}/deletes_d{max_dis}_pl{predix_len}.lmdb", db_default=True)
    # tmp1 = db.head(db.db_default, 10, decoder="bytes_set", from_i=0)
    # tmp = DBDeletes(db_file=f"{cf.DIR_MODELS}/deletes_d{max_dis}_pl{predix_len}_dup.lmdb",
    #                            buff_size=1e6, buff_save=1e6)
    # tmp.collect()
