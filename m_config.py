import re
from datetime import datetime
from enum import Enum

# ------------------------------------------------------------------------------
# Project directory
# DIR_ROOT = "/home/phuc/api/mtab"
DIR_ROOT = "/Users/phucnguyen/git/mtab"
# DIR_ROOT = "/Users/mtab/git/mtab"


# ------------------------------------------------------------------------------

# Log
FORMAT_DATE = "%Y_%m_%d_%H_%M"
DIR_LOG = f"{DIR_ROOT}/log/{datetime.now().strftime(FORMAT_DATE)}.txt"

# ------------------------------------------------------------------------------

DUMPS_WP_VER = "20211101"  # "20201201"
DUMPS_WD_VER_1 = "20211025"
DUMPS_WD_VER_2 = "20211101"

DUMPS_DP_VER = "20210601"

# Dumps
DIR_DUMPS = f"{DIR_ROOT}/data/dump"
# Wikipedia XML dump -multistream
DIR_DUMP_WP = f"{DIR_DUMPS}/enwiki-{DUMPS_WP_VER}-pages-articles.xml.bz2"

# Wikipedia SQL dump for ID mapping - Wikipedia - Wikidata
DIR_DUMP_WIKIPEDIA_PAGE = f"{DIR_DUMPS}/enwiki-{DUMPS_WP_VER}-page.sql.gz"
DIR_DUMP_WIKIPEDIA_PROPS = f"{DIR_DUMPS}/enwiki-{DUMPS_WP_VER}-page_props.sql.gz"
DIR_DUMP_WIKIPEDIA_REDIRECT = f"{DIR_DUMPS}/enwiki-{DUMPS_WP_VER}-redirect.sql.gz"

# Wikidata JSON dump
DIR_DUMP_WD = f"{DIR_DUMPS}/wikidata-{DUMPS_WD_VER_1}-all.json.bz2"  # 20201207
# Truthy dump SemTab 2020
DIR_DUMP_WD_SEMTAB = f"{DIR_DUMPS}/latest-truthy.nt.bz2"
DIR_DUMP_WIKIDATA_REDIRECT = (
    f"{DIR_DUMPS}/wikidatawiki-{DUMPS_WD_VER_2}-redirect.sql.gz"
)
DIR_DUMP_WIKIDATA_PAGE = f"{DIR_DUMPS}/wikidatawiki-{DUMPS_WD_VER_2}-page.sql.gz"

# DBpedia Data bus dump
DIR_DUMP_DP = f"{DIR_DUMPS}/dbpedia/{DUMPS_DP_VER}/"
DIR_DUMP_DP_WP = f"{DIR_DUMP_DP}/wikipedia-links_lang=en.ttl.bz2"

DIR_DUMP_DBPEDIA_WIKIDATA = f"{DIR_DUMP_DP}/ontology--DEV_type=parsed_sorted.nt"
DIR_DUMP_DBPEDIA_REDIRECT = f"{DIR_DUMP_DP}/redirects_lang=en.ttl.bz2"

DIR_DUMP_DP_LABELS = f"{DIR_DUMP_DP}/labels_lang=en.ttl.bz2"
DIR_DUMP_DP_DESC = f"{DIR_DUMP_DP}/short-abstracts_lang=en.ttl.bz2"
DIR_DUMP_DP_TYPES_SPECIFIC = f"{DIR_DUMP_DP}/instance-types_lang=en_specific.ttl.bz2"
DIR_DUMP_DP_TYPES_TRANSITIVE = (
    f"{DIR_DUMP_DP}/instance-types_lang=en_transitive.ttl.bz2"
)
DIR_DUMP_DP_INFOBOX = f"{DIR_DUMP_DP}/infobox-properties_lang=en.ttl.bz2"
DIR_DUMP_DP_OBJECTS = f"{DIR_DUMP_DP}/mappingbased-objects_lang=en.ttl.bz2"
DIR_DUMP_DP_LITERALS = f"{DIR_DUMP_DP}/mappingbased-literals_lang=en.ttl.bz2"
DIR_DUMP_DP_DISAMBIGUATION = f"{DIR_DUMP_DP}/disambiguations_lang=en.ttl.bz2"
# ------------------------------------------------------------------------------

# Built Models
DIR_MODELS = f"{DIR_ROOT}/data/models"
# Change this in mac mini
DIR_MODELS_MULTILINGUAL = DIR_MODELS
# DIR_MODELS_MULTILINGUAL = "/Volumes/models"

DIR_DUMP_TABLES = f"{DIR_MODELS}/dump_tables_{DUMPS_WP_VER}.lmdb"

DIR_PRE_LANG = f"{DIR_MODELS}/lid.176.bin"  # Small .ftz
DIR_TEST_ROCKSDB = f"{DIR_MODELS}/rocksdb"
DIR_TEST_DB_2 = f"{DIR_MODELS}/test.lmdb"
DIR_TEST_DB_3 = f"{DIR_MODELS}/test.foundation"
DIR_DELETES_DB = f"{DIR_MODELS}/deletes_8_12.lmdb"

DIR_DB_FUZZY = f"{DIR_MODELS}" + "/fuzzy_{lang}_{n_deletes}_{prefix_len}.lmdb"

DIR_WIKIDATA_ITEMS_JSON = f"{DIR_MODELS}/wikidata_items_json.lmdb"
DIR_WIKIDATA_ITEMS_NT = f"{DIR_MODELS}/wikidata_items_nt.lmdb"
DIR_WIKIPEDIA_ITEMS = f"{DIR_MODELS}/wikipedia_items.lmdb"
DIR_DBPEDIA_ITEMS = f"{DIR_MODELS}/dbpedia_items.lmdb"
DIR_MAPPING_ID = f"{DIR_MODELS}/mapping_id.lmdb"
# Todo: change back to normal
DIR_W_ITEMS = f"{DIR_MODELS}/wiki_items.lmdb"

DIR_WIKI_GRAPH = f"{DIR_MODELS}/wiki_graph.npz"
DIR_WIKI_GRAPH_VOCAB = f"{DIR_MODELS}/wiki_graph.lmdb"
DIR_WIKI_PAGERANK = f"{DIR_MODELS}/wiki_graph.pkl"
DIR_WIKI_QID = f"{DIR_MODELS}/qid.pkl"
DIR_WIKI_PAGERANK_STATS = f"{DIR_MODELS}/wiki_graph_pagerank_stats.pkl"

DIR_WIKI_GRAPH_VOCAB_1 = f"{DIR_MODELS}/wiki_graph_vocab.pkl"

DIR_WIKI_GRAPH_ROW = f"{DIR_MODELS}/wiki_graph_row.pkl"
DIR_WIKI_GRAPH_COL = f"{DIR_MODELS}/wiki_graph_col.pkl"
DIR_WIKI_GRAPH_DATA = f"{DIR_MODELS}/wiki_graph_data.pkl"
DIR_WIKI_GRAPH_PAGERANK = f"{DIR_MODELS}/wiki_graph_pagerank.pkl"

DIR_WIKI_LABELS = f"{DIR_MODELS}/wiki_labels.lmdb"
DIR_WIKI_LABELS_ENGLISH = f"{DIR_MODELS}/wiki_labels_en.lmdb"
DIR_WIKI_LABELS_MULTILINGUAL = f"{DIR_MODELS}/wiki_labels_multilingual.lmdb"

DIR_WIKI_LK_VOCAB = f"{DIR_MODELS}/wiki_lk_vocab.pkl"
# DIR_WIKI_LK_WORDS = f"{DIR_MODELS}/wiki_lk_words.pkl"
DIR_WIKI_LK_DELETES = f"{DIR_MODELS}/wiki_lk_deletes.pkl"
# ------------------------------------------------------------------------------
# Datasets
DIR_SEMTAB = f"{DIR_ROOT}/data/semtab"
DIR_SEMTAB_2 = f"{DIR_SEMTAB}/2020"
DIR_SEMTAB_1 = f"{DIR_SEMTAB}/2019"
DIR_SEMTAB_2_R4 = f"{DIR_SEMTAB_2}/Round4"
DIR_SEMTAB_2_R4_TAR_CEA = f"{DIR_SEMTAB_2_R4}/CEA_Round4_Targets.csv"
DIR_SEMTAB_2_R4_TAR_CTA = f"{DIR_SEMTAB_2_R4}/CTA_Round4_Targets.csv"
DIR_SEMTAB_2_R4_TAR_CPA = f"{DIR_SEMTAB_2_R4}/CPA_Round4_Targets.csv"

DIR_SAMPLE_TABLES = f"{DIR_SEMTAB}/samples/tables"
DIR_SAMPLE_TAR_CEA = f"{DIR_SEMTAB}/samples/cea.csv"
DIR_SAMPLE_TAR_CTA = f"{DIR_SEMTAB}/samples/cta.csv"
DIR_SAMPLE_TAR_CPA = f"{DIR_SEMTAB}/samples/cpa.csv"
DIR_SAMPLE_ZIP = "/Users/phucnguyen/git/mtab/data/semtab/mytables.zip"

# ------------------------------------------------------------------------------
# Prefix
# Wikidata
WD = "http://www.wikidata.org/entity/"
WDT = "http://www.wikidata.org/prop/direct/"
WDT3 = "http://www.wikidata.org/prop/direct-normalized/"
WDT2 = "http://www.wikidata.org/prop/statement/"

WD_PROP_LABEL = "http://schema.org/name"
WD_PROP_DES = "http://schema.org/description"
WD_PROP_ALIAS = "http://www.w3.org/2004/02/skos/core#altLabel"

WD_TOP = {
    "http://www.w3.org/2002/07/owl#Thing",
    # "Q35120",  # something
    # "Q830077",  # subject
    # "Q18336849",  # item with given name property
    # "Q23958946",  # individual/instance
    # "Q26720107",  # subject of a right
    # "Q488383",  # object
    # "Q4406616",  # concrete object
    # "Q29651224",  # natural object
    # "Q223557",  # physical object
    # "Q16686022",  # natural physical object
    "Q35120",  # entity
    "Q830077",  # subject
    "Q18336849",  # item with given name property
    "Q23958946",  # individual/instance
    "Q26720107",  # subject of a right
    "Q488383",  # object
    "Q4406616",  # concrete object
    "Q29651224",  # natural object
    "Q223557",  # physical object
    "Q16686022",  # natural physical object
    # new additions
    # classes, objects, etc.
    "Q16889133",  # class
    "Q5127848",  # class
    "Q7184903",  # abstract object
    "Q16686448",  # artificial entity
    "Q151885",  # concept
    "Q8205328",  # artificial physical object
    "Q29651519",  # mental object
    "Q24017414",  # first-order metaclass
    "Q23960977",  # (meta)class"
    "Q19478619",  # metac"Q17339814"lass
    "Q23959932",  # fixed-order metaclass
    "Q21522864",  # class or metaclass of Wikidata ontology
    "Q19361238",  # Wikidata metaclass
    # general high-level concepts
    "Q595523",  # notion
    "Q1969448",  # term
    "Q4393498",  # representation
    "Q2145290",  # mental representation
    "Q1923256",  # intension
    "Q131841",  # idea
    "Q24229398",  # agent
    "Q1190554",  # occurrence
    "Q1914636",  # activity
    "Q26907166",  # temporal entity
    "Q4026292",  # action
    "Q9332",  # behavior
    "Q6671777",  # structure
    "Q1347367",  # capability
    "Q937228",  # property
    "Q930933",  # relation
    "Q1207505",  # quality
    "Q39875001",  # measure
    "Q337060",  # perceptible object
    "Q483247",  # phenomenon
    "Q16722960",  # phenomenon
    "Q602884",  # social phenomenon
    "Q769620",  # social action
    "Q1656682",  # event
    "Q386724",  # work
    "Q17537576",  # creative work
    "Q15621286",  # intellectual work
    "Q15401930",  # product (result of work/effort)
    "Q28877",  # goods
    "Q58778",  # system
    # groups and collections
    "Q99527517",  # collection entity
    "Q98119401",  # group or class of physical objects
    "Q36161",  # set
    "Q20937557",  # series
    "Q16887380",  # group
    "Q61961344",  # group of physical objects
    "Q16334295",  # group of humans
    "Q16334298",  # group of living things
    "Q874405",  # social group
    "Q3533467",  # group action
    # geographic terms
    "Q618123",  # geographical object
    "Q20719696",  # physico-geographical object
    "Q1503302",  # geographic object
    "Q58416391",  # spatial entity
    "Q15642541",  # human-geographic territorial entity
    "Q1496967",  # territorial entity
    "Q27096213",  # geographic entity
    "Q27096235",  # artificial geographic entity
    "Q2221906",  # geographic location
    "Q58415929",  # spatio-temporal entity
    "Q5839809",  # regional space
    "Q1251271",  # geographic area
    "Q82794",  # geographic region
    "Q35145263",  # natural geographic object
    "Q27096220",  # natural geographic entity
    # units
    "Q3563237",  # economic unit
    "Q2198779",  # unit
    "Q5371079",  # emic unit
    "Q20817253",  # linguistic unit
    "Q3695082",  # sign
    "Q7887142",  # unit of analysis
    "Q15198957",  # aspect of music
    "Q271669",  # landform
    "Q12766313",  # geomorphological unit
    "Q15989253",  # part
    # others (less general)
    "Q17334923",  # location
    "Q3257686",  # locality
    "Q43229",  # organization
    "Q177634",  # community
    "Q15324",  # body of water
    "Q863944",  # land waters
    "Q2507626",  # water area
    "Q3778211",  # legal person
    "Q4330518",  # research object
    "Q56061",  # administrative territorial entity
    "Q1048835",  # political territorial entity
    "Q1799794",  # administrative territorial entity of a specific level
    "Q12076836",  # administrative territorial entity of a single country
    "Q7210356",  # political organisation
    "Q155076",  # juridical person
    "Q12047392",  # legal form
    "Q1063239",  # polity
    "Q32178211",  # musical organization
    "Q4897819",  # role
    "Q1781513",  # position
    "Q1792379",  # art genre
    "Q483394",  # genre
}

# Wikipedia dump
WP_IGNORED_NS = (
    "wikipedia:",
    "file:",
    "portal:",
    "template:",
    "mediawiki:",
    "user:",
    "help:",
    "book:",
    "draft:",
    "module:",
    "timedtext:",
)
WP_NAMESPACE_RE = re.compile(r"^{(.*?)}")
WP_DISAMBIGUATE_REGEXP = re.compile(
    r"{{\s*(disambiguation|disambig|disamb|dab|geodis)\s*(\||})", re.IGNORECASE
)

# Wikipedia
WIKI_EN = "http://en.wikipedia.org/wiki/"

# DBpedia
DBR = "http://dbpedia.org/resource/"
DBO = "http://dbpedia.org/ontology/"
DBP = "http://dbpedia.org/property/"

FOAF = "http://xmlns.com/foaf/0.1/"
PURL = "http://purl.org/dc/elements/1.1/"
SKOS = "http://www.w3.org/2004/02/skos/core#"

PREFIX_LIST = {WD, WDT, WDT2, DBR, DBO, DBP, WIKI_EN, FOAF, PURL}

REMOVE_HTML_TAGS = [
    # ["<ref", "</ref>"],
    # ["<ref", "/>"],
    # ["{{", "}}"],
    ["[[File:", "]]"],
    # ["<hiero>", "</hiero>"],
    # ["<poem", ">"],
    # ["<code>", "</code>"],
    # ["<span", ">"],
    # ["{| class=", "|}"],
    # ["<!--", "-->"],
    # ["<center", ">"],
    # ["<div", "</div>"],
    # ["<syntaxhighlight", "</syntaxhighlight>"],
    # ["<big", ">"],
    # ["<math", "</math>"],
]

REMOVE_TAGS = [
    "<br />",
    "<br>",
    "<br",
    "&nbsp;",
    "&nbsp",
    "nbsp",
    "'''",
    "''",
    "</small>",
    "<small>",
    "}}",
    "/>",
    "</span>",
    "<sub>",
    "</sub>",
    "<sup>",
    "</sup>",
    "</poem>",
    "</center>",
    "</big>",
]
IGNORED_LINKS = [
    "File:",
]
# ------------------------------------------------------------------------------
# Configuration parameters
ENCODING = "utf-8"
LANG = "en"

SIZE_1MB = 1048576
SIZE_512MB = SIZE_1MB * 512
SIZE_1GB = SIZE_1MB * 1024

LMDB_MAP_SIZE = 10737418240  # 10GB
LMDB_BUFF_BYTES_SIZE = SIZE_1MB * 256
LMDB_MAX_KEY = 511

S_TABLE_LIMIT = 10000
S_TABLE_COLUMN_LIMIT = 1000
S_TABLE_ROW_LIMIT = 1000000

MAX_EDIT_DISTANCE = 10
WEIGHT_PAGERANK = 3e7  # max page_rank = 0.00011779942083252528
WEIGHT_PR_STD = 3.8780082925364126e-08  # 4.046594838893245e-08
WEIGHT_PR_MEAN = 1.0520270953571298e-08  # 1.0656537784491441e-08
WEIGHT_PR_MAX = 0.00012037439418941944  #  0.00011678802493033757
WEIGHT_PR_MIN = 4.757236044866987e-09  #  4.871408368733647e-09
WEIGHT_PR_DIV = WEIGHT_PR_MAX - WEIGHT_PR_MIN
WEIGHT_PR_MEAN_RATIO = (WEIGHT_PR_MEAN - WEIGHT_PR_MIN) / WEIGHT_PR_DIV

WEIGHT_WD = 3
WEIGHT_TYPES = 1
WEIGHT_W_OTHERS = 1

ES_INDEX_NAME_EN = "mtab_english"
ES_INDEX_NAME_ALL = "mtab_multilingual"

ES_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {"properties": {"label": {"type": "text"}}},
}

LIMIT_GEN_CAN = 50
LIMIT_SEARCH = 50
LIMIT_SEARCH_ES = 1000
LIMIT_CEA_TAR = 1000
LIMIT_TABLES = 100
WEIGHT_ES = 90
LMDB_BUFF_LIMIT = 1073741824 * 5  # 25GB

max_cans = 3
HEADER_FIRST_ROW = 1


TABLE_EXTENSIONS = {
    "txt",
    "csv",
    "tsv",
    "xml",
    "json",
    "xls",
    "xlsm",
    "xlsb",
    "xltx",
    "xlsx",
    "xlt",
    "xltm",
}
ALLOWED_EXTENSIONS = set(["zip"] + list(TABLE_EXTENSIONS))

HTTP = {"http", "https"}
TEXT_SPACY = {
    "PERSON",
    "NORP",
    "FAC",
    "ORG",
    "LOC",
    "GPE",
    "PRODUCT",
    "EVENT",
    "WORK_OF_ART",
    "LAW",
    "LANGUAGE",
}
NUM_SPACY = {"CARDINAL", "PERCENT", "MONEY", "ORDINAL", "QUANTITY", "TIME", "DATE"}


NUM_DUCK = {
    "amount-of-money",
    "phone-number",
    "distance",
    "duration",
    "email",
    "number",
    "ordinal",
    "quantity",
    "temperature",
    "time",
    "url",
    "volume",
}
TEXT_DUCK = {"text"}
NONE_CELLS = {
    "''",
    '""',
    "-",
    "--",
    "'-'",
    '"-"',
    " ",
    ".",
    "' '",
    '" "',
    "nan",
    "none",
    "null",
    "blank",
    "yes",
    "unknown",
    "?",
    "??",
    "???",
    "0",
    "total",
}

# ------------------------------------------------------------------------------
# Global variable
global m_mapper  # Mapper id between Wikidata, Wikipedia, DBpedia
global m_corrector  # Spell correcter
global m_wiki_items  # Wiki resources

global m_item_labels  # Wiki entity names
global m_entity_db_multilingual  # # Wiki entity names multi language
global m_search_e  # Entity search with BM25
global m_search_f  # Entity search with edit distance
global m_search_s  # Statement search

global m_wikidata_nt
global m_wikidata_json

global pre_lk
global err_lk
global lang_detector
global m_spacy

global encoding_detector


# ------------------------------------------------------------------------------


# Enum
class EnumPr(Enum):
    SOFTMAX = 1
    AVG = 2


class EnumRank(Enum):
    RANK = 1
    SCORE = 2
    EQUAL = 3


class SourceType:
    TEXT = "text"
    FILE = "file"
    URL = "url"
    OBJ = "obj"


class DataType:
    TEXT = 1
    NUM = 0
    NONE = 2


class ToBytesType:
    OBJ = 0
    INT_LIST = 1


class DBUpdateType:
    SET = 0
    COUNTER = 1


min_sim_value = 0.97
