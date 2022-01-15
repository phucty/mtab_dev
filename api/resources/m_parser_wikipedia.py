import bz2
import gc
import re
from collections import defaultdict
from contextlib import closing
from multiprocessing.pool import Pool
from xml.etree.cElementTree import iterparse

import lmdb
import six
import wikitextparser as wtp
from tqdm import tqdm

import m_config as cf
from api.resources.m_db_item import DBItem, DBItemDefault, serialize
from api.resources.m_mapping_id import MappingID
from api.utilities import m_io as iw
from api.utilities import m_utils as ul
import pickle
import zlib


class WPDumpReader(object):
    def __init__(self, dump_file, ignored_ns=cf.WP_IGNORED_NS):
        self._dump_file = dump_file
        self._ignored_ns = ignored_ns
        with bz2.BZ2File(self._dump_file) as f:
            self._lang = re.search(
                r'xml:lang="(.*)"', six.text_type(f.readline())
            ).group(1)

    @property
    def dump_file(self):
        return self._dump_file

    @property
    def language(self):
        return self._lang

    def __iter__(self):
        with bz2.BZ2File(self._dump_file) as f:
            for (title, wiki_text, redirect) in self._extract_pages(f):
                lower_title = title.lower()
                if any([lower_title.startswith(ns) for ns in self._ignored_ns]):
                    continue

                yield [title, self._lang, wiki_text, redirect]

    @staticmethod
    def _extract_pages(in_file):
        def _get_namespace(_tag):
            match_obj = cf.WP_NAMESPACE_RE.match(_tag)
            if match_obj:
                ns = match_obj.group(1)
                if not ns.startswith("http://www.mediawiki.org/xml/export-"):
                    raise ValueError("%s not recognized as MediaWiki namespace" % ns)
                return ns
            else:
                return ""

        def _to_unicode(s):
            if isinstance(s, str):
                return s
            return s.decode(cf.ENCODING)

        elems = (elem for (_, elem) in iterparse(in_file, events=(b"end",)))
        elem = next(elems)

        tag = six.text_type(elem.tag)
        namespace = _get_namespace(tag)
        page_tag = "{%s}page" % namespace
        text_path = "./{%s}revision/{%s}text" % (namespace, namespace)
        title_path = "./{%s}title" % namespace
        redirect_path = "./{%s}redirect" % namespace
        for elem in elems:
            if elem.tag == page_tag:
                title = elem.find(title_path).text
                text = elem.find(text_path).text or ""
                redirect = elem.find(redirect_path)
                if redirect is not None:
                    redirect = ul.norm_wikipedia_title(
                        _to_unicode(redirect.attrib["title"])
                    )

                yield _to_unicode(title), _to_unicode(text), redirect
                elem.clear()


class WikiTables:
    def __init__(self, db_file):
        self._db_file = db_file
        self._env = lmdb.open(self._db_file, subdir=False, lock=False, max_dbs=1)
        self._db_tables = self._env.open_db(b"__tables__")

    def __reduce__(self):
        return [self.__class__, [self._db_file]]

    def tables(self):
        with self._env.begin(db=self._db_tables) as txn:
            cur = txn.cursor()
            for (key, value) in iter(cur):
                yield (
                    key.decode(cf.ENCODING),
                    pickle.loads(zlib.decompress(value)),
                )

    def get_table(self, table_id):
        value = None
        with self._env.begin(db=self._db_tables) as txn:
            value = txn.get(table_id.encode(cf.ENCODING))
            if value:
                value = pickle.loads(zlib.decompress(value))
        return value


class WikiTable(object):
    def __init__(self, wikipedia_title, index, table_parser):
        self.wikipedia_title = wikipedia_title
        self.index = index
        self.table_data = []
        self.n_row = 0
        self.n_col = 0
        self.n_link = 0
        self.n_mis_text = 0
        self.n_mis_link = 0
        self.header_rows = []
        self.title = ""
        self._parse_link(table_parser)

    def __repr__(self):
        return "%s||%s" % (self.wikipedia_title, self.index)

    def _parse_link(self, table_parser):
        if table_parser.caption:
            self.title = (
                wtp.parse(ul.replace_html_tags_2(table_parser.caption))
                .plain_text()
                .strip()
            )

        self.header_rows = [
            i
            for i, row in enumerate(table_parser._match_table)
            if row[0]["sep"] == b"!"
        ]
        for row_obj in table_parser.data():
            results_row = []
            if len(row_obj) > self.n_col:
                self.n_col = len(row_obj)
            for cell_text in row_obj:
                cell_obj = WikiTableCell(cell_text)
                if not cell_obj.has_text:
                    self.n_mis_text += 1
                if not cell_obj.has_link:
                    self.n_mis_link += 1
                self.n_link += cell_obj.n_links
                results_row.append(cell_obj)
            # if sum([1 for row_obj in results_row if row_obj.has_text]):
            self.table_data.append(results_row)
            self.n_row += 1

    @property
    def n_cel(self):
        return self.n_row * self.n_col

    @property
    def r_mis_link(self):
        return self.n_mis_link / self.n_cel

    @property
    def r_mis_text(self):
        return self.n_mis_text / self.n_cel


class WikiTableCell(object):
    def __init__(self, cell_text):
        self.text = None
        self.links = []
        self._parse_cell(cell_text)

    def _parse_cell(self, cell_text):
        if cell_text:
            # Remove html tags:
            cell_text = ul.replace_html_tags_2(cell_text)
            cell_text = wtp.parse(cell_text).plain_text(replace_wikilinks=False)

            link_parser = wtp.parse(cell_text)
            self.text = link_parser.string
            if link_parser.wikilinks:
                self.links = [
                    WikiTableLink(link_obj) for link_obj in link_parser.wikilinks
                ]
                self.links.sort(key=lambda x: x.start)
                i, j = 0, 0
                self.text = ""
                while i < len(link_parser.string) and j < len(self.links):
                    self.text += (
                        link_parser.string[i : self.links[j].start] + self.links[j].text
                    )

                    i = self.links[j].end
                    j += 1
                self.text += link_parser.string[i:]
                self.text = " ".join(self.text.split(" ")).strip()

                # Remove Files link
                self.links = [
                    l
                    for l in self.links
                    if l.link
                    and not any([l.link.startswith(ns) for ns in cf.IGNORED_LINKS])
                ]

                last_position = 0
                for link in self.links:
                    start = self.text.find(link.text, last_position, len(self.text))
                    if start >= last_position:
                        link.start = start
                        link.end = start + len(link.text)
                        last_position = link.end

    def __repr__(self):
        if self.has_text:
            if self.has_link:
                return "%s [%d]" % (self.text, len(self.links))
            else:
                return self.text
        else:
            return ""

    @property
    def has_text(self):
        return bool(self.text)

    @property
    def has_link(self):
        return bool(self.links)

    @property
    def n_links(self):
        return len(self.links)


class WikiTableLink(object):
    def __init__(self, link_obj):
        self.start = -1
        self.end = -1
        self.text = None
        self.link = None
        self._parse_link(link_obj)

    def _parse_link(self, link_obj):
        self.start = link_obj.span[0]
        self.end = link_obj.span[1]
        self.text = link_obj.text if link_obj.text else link_obj.title
        self.link = link_obj.title

    def __repr__(self):
        if self.link:
            return self.link
        else:
            return ""


class WPPage(object):
    def __init__(self, title, lang, wiki_text, redirect):
        self.title = title
        self.lang = lang
        self.redirect = redirect
        self.wiki_text = wiki_text
        self.wp_obj = None
        self.wp_wd = None
        if not self.redirect:
            try:
                self.wp_obj, self.wp_wd = self._parse_wikipedia_page(wiki_text)
            except Exception as message:
                iw.print_status(message)
                iw.print_status(wiki_text)

    def __repr__(self):
        return self.title

    def __reduce__(self):
        return (self.__class__, (self.title, self.lang, self.wiki_text, self.redirect))

    def parse_table(self):
        # if self.title == "Tokyo":
        #     table_parser = wtp.parse(self.wiki_text)
        #     for t in table_parser.get_tables():
        # debug = 1
        # else:
        #     return []

        table_parser = wtp.parse(self.wiki_text).tables

        tables = []
        for i, _table_parser in enumerate(table_parser):
            try:
                table = WikiTable(self.title, i, _table_parser)
                if table.n_row < 2 or table.n_col < 2:
                    continue
                # if table.r_mis_text < 0.75 and table.n_row > 2 and 1 < table.n_col and table.r_mis_link < 1:
                tables.append(table)
                # WikiTable(self.title, i, _table_parser)
            except Exception as message:
                iw.print_status("Error: Table Parsing - %s" % message, is_screen=False)
        return tables

    @staticmethod
    def _parse_wikipedia_page(wiki_text):
        def norm_text(text):
            text = " ".join(text.split())
            return text

        wp_obj = {
            "claims_wd": defaultdict(set),
            "claims_literal": defaultdict(set),
        }
        wp_wd = None
        if not wiki_text or not len(wiki_text):
            return wp_obj, wp_wd

        w_parser = wtp.parse(wiki_text)
        if not w_parser:
            return None

        for w_section in w_parser.sections:
            # Infobox
            for w_template in w_section.templates:
                if "Infobox" in w_template.name:
                    for w_argument in w_template.arguments:
                        w_prop = " ".join(w_argument.name.split())
                        if not len(w_prop):
                            continue
                        if not len(w_argument.wikilinks):
                            w_value = ul.wiki_plain_text(w_argument.value)
                            w_value = norm_text(w_value)
                            if len(w_value):
                                wp_obj["claims_literal"][w_prop].add(w_value)
                        else:
                            for w_link in w_argument.wikilinks:
                                if not w_link.title:
                                    continue
                                w_value = ul.norm_wikipedia_title(w_link.title)
                                if w_value[:5] == "File:":
                                    continue
                                wp_obj["claims_wd"][w_prop].add(w_value)
            # Sections
            prop_value = "Section: Information"
            if w_section.title:
                prop_value = f"Section: {norm_text(w_section.title)}"

            for w_link in w_section.wikilinks:
                if w_link.title:
                    wp_obj["claims_wd"][prop_value].add(
                        ul.norm_wikipedia_title(w_link.title)
                    )

            # Wikidata mapping
            if w_section.title == "External links":
                for i_templates in w_section.templates:
                    if i_templates.name == "Subject bar":
                        for i_arguments in i_templates.arguments:
                            if i_arguments.name == "d" and ul.is_wd_item(
                                i_arguments.value
                            ):
                                wp_wd = i_arguments.value

        return wp_obj, wp_wd


class WPItem(DBItem):
    def __init__(self, db_file=cf.DIR_WIKIPEDIA_ITEMS):
        super().__init__(db_file=db_file, max_db=1)
        self._db_pages = self._env.open_db(b"__page__")

    def copy(self, compress=False):
        db_names = {b"__page__": b"__page__"}
        self.copy_new_file(
            db_names, map_size=cf.SIZE_1GB * 10, compress=compress,
        )

    def info(self):
        iw.print_status(self.env.info())
        iw.print_status(f"Wikipedia pages: {self.size()}")

    def items(self, compress=True):
        return self.get_db_iter(self._db_pages, compress_value=compress)

    def size(self):
        return self.get_db_size(self._db_pages)

    def get_item(self, title, compress=True):
        return self.get_value(self._db_pages, title, compress_value=compress)

    def iter_pages(self, compress=True):
        return self.get_db_iter(self._db_pages, compress_value=compress)

    def _build_from_dump(self, iter_items):
        buff_wp = []
        c_ok = 0
        c_redirect = 0
        buff_size = 0

        def update_desc():
            return f"{c_ok} pages | {c_redirect} redirects | buff: {buff_size / cf.SIZE_512MB * 100:.0f}%"

        p_bar = tqdm(desc=update_desc())
        for i, iter_item in enumerate(iter_items):
            responds = parse_reader(iter_item)
            p_bar.update()
            if not responds:
                continue
            if responds.redirect:
                c_redirect += 1
            else:
                c_ok += 1
                key, value = serialize(responds.title, responds.wp_obj)
                buff_size += len(key) + len(value)
                buff_wp.append((key, value))

            if i and i % 1000 == 0:
                p_bar.set_description(update_desc())

            if buff_size >= cf.SIZE_512MB:
                self.write_bulk(self.env, self._db_pages, buff_wp)
                p_bar.set_description(update_desc())
                buff_size = 0
                buff_wp = []

        if len(buff_wp):
            self.write_bulk(self.env, self._db_pages, buff_wp)
        p_bar.close()

    def build(self, iter_items):
        iw.print_status("Parse Wikipedia dump")
        self._build_from_dump(iter_items)


def parse_reader(responds):
    title, lang, wiki_text, redirect = responds
    wiki_page = WPPage(title, lang, wiki_text, redirect)
    return wiki_page


def test_wikipedia_reader(n_limit=10):
    dump_reader = WPDumpReader(cf.DIR_DUMP_WP)
    n_c = 0
    n_ok = 0
    for i, entity_obj in enumerate(dump_reader):
        if entity_obj:
            n_ok += 1
        n_c += 1
        print(f"{n_ok}/{n_c}: {str(entity_obj)}")
        if n_c > n_limit:
            break


def test_wikipedia_parser():
    items = WPItem()
    items.build(WPDumpReader(cf.DIR_DUMP_WP))
    items.info()


def parse_result(page):
    title, lang, wiki_text, redirect = page
    tables = defaultdict()
    try:
        wikipage = WPPage(title, lang, wiki_text, redirect)
        if not wikipage.redirect:
            wp_tables = wikipage.parse_table()
            if wp_tables:

                for table in wp_tables:
                    tables[str(table)] = table
                return title, tables
    except Exception as message:
        iw.print_status(f"{title} - {message}")
        pass
    return title, tables


def parse_wikipedia_dump(langs, n_cpu=1, buff_save=cf.SIZE_1MB * 10):
    for lang in langs:
        out_file = f"{cf.DIR_MODELS}/dump_tables_{lang}.lmdb"

        w_dump = WPDumpReader(
            f"{cf.DIR_DUMPS}/{lang}wiki-{cf.DUMPS_WP_VER}-pages-articles.xml.bz2"
        )
        buffer = []
        buff_size = 0
        count_tables = 0
        count_pages = 0
        iw.create_dir(out_file)
        iw.delete_file(out_file)

        def update_desc():
            return f"{lang} | Pages:{count_pages} | Tables: {count_tables} | Buff:{buff_size/cf.SIZE_1MB:.2f}MB"

        p_bar = tqdm(desc=update_desc())
        db_tables = DBItemDefault(db_file=out_file)
        # if n_cpu == 1:
        #     for page in w_dump:
        #         _page, _tables = parse_result(page)
        with closing(Pool(n_cpu)) as pool:
            for _page, _tables in pool.imap_unordered(parse_result, w_dump):
                count_pages += 1
                p_bar.update()
                if count_pages % 10 == 0:
                    p_bar.set_description(desc=update_desc())
                    # break

                if not _tables:
                    continue

                count_tables += len(_tables)
                # for _table_key, _table_content in _tables.items():
                #     count_tables += 1

                d_obj = ul.get_dump_obj(_page, _tables, compress=True)
                buff_size += len(d_obj[0]) + len(d_obj[1])
                buffer.append(d_obj)

                # if _page == "Tokyo":
                #     db_tables.write_bulk(db_tables.env, db_tables.db_default, buffer)
                #     break

                if buff_size > buff_save:
                    db_tables.write_bulk(db_tables.env, db_tables.db, buffer)
                    buffer = []
                    buff_size = 0

            if buff_size:
                db_tables.write_bulk(db_tables.env, db_tables.db, buffer)
                p_bar.set_description(desc=update_desc())
        p_bar.close()
        #
    # all_cpu = multiprocessing.cpu_count()
    # pool_size = all_cpu
    # w_dump = WikiDump(dump_file)
    # n_table, n_row, n_col, n_link, n_mis_text, n_mis_link, n_header_row = [], [], [], [], [], [], []
    # n_page = 0
    #
    # table_dump_dir = "./temp/table_dump_%s/" % dump_date
    # table_buf = []
    #
    # buffer_size = 100000
    # index_buf = 0
    # with Pool(pool_size) as pool:
    #     page_buf = []
    #     redirect_buf = []
    #     for _tables, _n_table, _n_row, _n_col, _n_link, _n_mis_text, _n_mis_link, _n_header_row \
    #             in pool.imap_unordered(parse_result, w_dump):
    #         n_page += 1
    #         if _n_table:
    #             table_buf.extend(_tables)
    #             n_table.append(_n_table)
    #             n_row.append(_n_row)
    #             n_col.append(_n_col)
    #             n_link.append(_n_link)
    #             n_mis_text.append(_n_mis_text)
    #             n_mis_link.append(_n_mis_link)
    #             n_header_row.append(_n_header_row)
    #
    #         if len(table_buf) >= buffer_size:
    #             save_buf = table_buf[:buffer_size]
    #             rema_buf = table_buf[buffer_size:].copy()
    #             write_db(table_dump_dir, index_buf, save_buf)
    #             index_buf += 1
    #             del table_buf[:]
    #             del table_buf
    #             table_buf = rema_buf
    #
    #         if n_page % buffer_size == 0:
    #             print_status(n_page, n_table, n_row, n_col, n_link, n_mis_text, n_mis_link, n_header_row)
    #
    # print_status(n_page, n_table, n_row, n_col, n_link, n_mis_text, n_mis_link, n_header_row)
    # if len(table_buf):
    #     write_db(table_dump_dir, index_buf, table_buf)

    # iw.save_object_pickle("./temp/wiki_table_%s.pkl" % dump_date, tables)


if __name__ == "__main__":
    build_obj = WPItem()
    tmp = build_obj.get_item("Tokyo")
    # build_obj.build(WPDumpReader(cf.DIR_DUMP_WP))
    iw.print_status("Done")

    # db.copy(compress=False)
    # for k, v in tqdm(db.items(compress=True)):
    #     debug = 1
    # test_wikipedia_reader()
    # test_wikipedia_parser()
    # langs = ["vi", "ja", "de", "fr", "nl", "ru", "it", "es", "ar", "zh", "pl", "pt"]
    # parse_wikipedia_dump(langs, n_cpu=1)
    # db_tables = DBItem(
    #     db_file=cf.DIR_DUMP_TABLES, db_default=True, map_size=cf.LMDB_MAP_SIZE
    # )
    # tmp = db_tables.get_item_default("Mayakoba Golf Classic", compress=True)
    # debug = 1

"""
8519726 pages | 9777275 redirects | buff: 73%: : 18297562it [10:05:05, 503.98it/s] 
"""
