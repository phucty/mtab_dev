import lmdb
import os, sys
import struct


# B(ascii 'string') -> bytes
try:
    bytes("")  # Python>=2.6, alias for str().
    B = lambda s: s
except TypeError:  # Python3.x, requires encoding parameter.
    B = lambda s: bytes(s, "ascii")

# BL('s1', 's2') -> ['bytes1', 'bytes2']
BL = lambda *args: list(map(B, args))
# TS('s1', 's2') -> ('bytes1', 'bytes2')
BT = lambda *args: tuple(B(s) for s in args)
# O(int) -> length-1 bytes
O = lambda arg: B(chr(arg))
# OCT(s) -> parse string as octal
OCT = lambda s: int(s, 8)


KEYS = BL("a", "b", "baa", "d")
ITEMS = [(k, B("")) for k in KEYS]
REV_ITEMS = ITEMS[::-1]
VALUES = [B("") for k in KEYS]

KEYS2 = BL("a", "b", "baa", "d", "e", "f", "g", "h")
ITEMS2 = [(k, B("")) for k in KEYS2]
REV_ITEMS2 = ITEMS2[::-1]
VALUES2 = [B("") for k in KEYS2]

KEYSFIXED = BL("a", "b", "c", "d", "e", "f", "g", "h")
VALUES_MULTI = [
    (struct.pack("I", 95000000), struct.pack("I", 1), struct.pack("I", 10000))
    for k in KEYSFIXED
]
ITEMS_MULTI_FIXEDKEY = [
    (kv[0], v) for kv in list(zip(KEYSFIXED, VALUES_MULTI)) for v in kv[1]
]


def _put_items(items, t, db=None):
    for k, v in items:
        if db:
            t.put(k, v, db=db)
        else:
            t.put(k, v)


def putData(t, db=None):
    _put_items(ITEMS, t, db=db)


def putBigData(t, db=None):
    _put_items(ITEMS2, t, db=db)


def putBigDataMultiFixed(t, db=None):
    items = ITEMS_MULTI_FIXEDKEY
    _put_items(items, t, db=db)


env = lmdb.open("test", max_dbs=1)

txn = env.begin(write=True)

db = env.open_db(key=b"testdb", txn=txn, dupsort=True, integerdup=True, dupfixed=True)

c = txn.cursor(db=db)

putBigDataMultiFixed(txn, db)

c.getmulti(KEYSFIXED)
env.close()

os.system("rm -r students")
