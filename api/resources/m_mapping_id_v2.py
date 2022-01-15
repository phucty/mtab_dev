import gzip
from collections import defaultdict

from tqdm import tqdm

import m_config as cf
from api.resources.m_db_rocks import DBSubspace
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


class MappingID:
    def __init__(self, db_dir=cf.DIR_MAPPING_ID.replace(".lmdb", ""), read_only=True):
        self.db_dir = db_dir
        self.db_redirect_wikipedia = DBSubspace(
            db_dir + "/db_redirect_wikipedia", read_only
        )
        self.db_redirect_dbpedia = DBSubspace(
            db_dir + "/db_redirect_dbpedia", read_only
        )
        self.db_redirect_wikidata = DBSubspace(
            db_dir + "/db_redirect_wikidata", read_only
        )

        self.db_redirect_of_wikipedia = DBSubspace(
            db_dir + "/db_redirect_of_wikipedia", read_only
        )
        self.db_redirect_of_dbpedia = DBSubspace(
            db_dir + "/db_redirect_of_dbpedia", read_only
        )
        self.db_redirect_of_wikidata = DBSubspace(
            db_dir + "/db_redirect_of_wikidata", read_only
        )

        self.db_wikipedia_wikidata = DBSubspace(
            db_dir + "/db_wikipedia_wikidata", read_only
        )
        self.db_wikidata_wikipedia = DBSubspace(
            db_dir + "/db_wikidata_wikipedia", read_only
        )

        self.db_dbpedia_wikipedia = DBSubspace(
            db_dir + "/db_dbpedia_wikipedia", read_only
        )
        self.db_wikipedia_dbpedia = DBSubspace(
            db_dir + "/db_wikipedia_dbpedia", read_only
        )

        self.db_dbpedia_class_wikidata = DBSubspace(
            db_dir + "/db_dbpedia_class_wikidata", read_only
        )
        self.db_dbpedia_property_wikidata = DBSubspace(
            db_dir + "/db_dbpedia_property_wikidata", read_only
        )
        self.db_wikidata_dbpedia_class = DBSubspace(
            db_dir + "/db_wikidata_dbpedia_class", read_only
        )
        self.db_wikidata_dbpedia_property = DBSubspace(
            db_dir + "/db_wikidata_dbpedia_property", read_only
        )

    @staticmethod
    def _is_insert(line):
        return line.startswith("INSERT INTO")

    @staticmethod
    def _get_db_obj(db, key, return_default):
        respond = db.get_item(key)
        if respond is None or not respond:
            return return_default
        return respond

    def get_redirect_wikipedia(self, key):
        return self._get_db_obj(self.db_redirect_wikipedia, key, key)

    def get_redirect_dbpedia(self, key):
        return self._get_db_obj(self.db_redirect_dbpedia, key, key)

    def get_redirect_wikidata(self, key):
        return self._get_db_obj(self.db_redirect_wikidata, key, key)

    def get_redirect_of_wikipedia(self, key):
        return self._get_db_obj(self.db_redirect_of_wikipedia, key, set())

    def get_redirect_of_dbpedia(self, key):
        return self._get_db_obj(self.db_redirect_of_dbpedia, key, set())

    def get_redirect_of_wikidata(self, key):
        return self._get_db_obj(self.db_redirect_of_wikidata, key, set())

    @staticmethod
    def _get_items_with_redirects(
        item_id, db_item, func_redirect_item=None, func_tar_redirect=None
    ):
        respond = db_item.get_item(item_id)
        if not respond and func_redirect_item:
            item_id_redirect = func_redirect_item(item_id)
            if item_id_redirect:
                respond = db_item.get_item(item_id_redirect)

        if respond and func_tar_redirect:
            respond_redirect = func_tar_redirect(respond)
            if respond_redirect:
                respond = respond_redirect
        return respond

    def get_wikipedia_from_wikidata(self, wd_id):
        return self._get_items_with_redirects(
            wd_id,
            db_item=self.db_wikidata_wikipedia,
            func_redirect_item=self.get_redirect_wikidata,
            func_tar_redirect=self.get_redirect_wikipedia,
        )

    def get_wp_title_from_dp_title(self, db_title):
        return self._get_items_with_redirects(
            db_title,
            db_item=self.db_dbpedia_wikipedia,
            func_redirect_item=self.get_redirect_dbpedia,
            func_tar_redirect=self.get_redirect_wikipedia,
        )

    def get_wikidata_from_dbpedia_class(self, dp_class):
        return self._get_items_with_redirects(
            dp_class,
            db_item=self.db_dbpedia_class_wikidata,
            func_tar_redirect=self.get_redirect_wikidata,
        )

    def get_wikidata_from_dbpedia_property(self, dp_prop):
        return self._get_items_with_redirects(
            dp_prop,
            db_item=self.db_dbpedia_property_wikidata,
            func_tar_redirect=self.get_redirect_wikidata,
        )

    def get_dbpedia_class_from_wikidata(self, wd_id):
        return self._get_items_with_redirects(
            wd_id,
            db_item=self.db_wikidata_dbpedia_class,
            func_redirect_item=self.get_redirect_wikidata,
        )

    def get_dbpedia_prop_from_wikidata(self, wd_id):
        return self._get_items_with_redirects(
            wd_id,
            db_item=self.db_wikidata_dbpedia_property,
            func_redirect_item=self.get_redirect_wikidata,
        )

    def get_wikidata_from_wikipedia(self, wp_title):
        return self._get_items_with_redirects(
            wp_title,
            db_item=self.db_wikipedia_wikidata,
            func_redirect_item=self.get_redirect_wikipedia,
            func_tar_redirect=self.get_redirect_wikidata,
        )

    def get_wikidata_from_dbpedia(self, db_title):
        wp_title = self.get_wp_title_from_dp_title(db_title)
        if wp_title:
            return self.get_wikidata_from_wikipedia(wp_title)
        return None

    def get_dbpedia_from_wikipedia(self, wp_title):
        return self._get_items_with_redirects(
            wp_title,
            db_item=self.db_wikipedia_dbpedia,
            func_redirect_item=self.get_redirect_wikipedia,
            func_tar_redirect=self.get_redirect_dbpedia,
        )

    def get_dbpedia_from_wikidata(self, wd_id):
        wp_title = self.get_wikipedia_from_wikidata(wd_id)
        if wp_title:
            return self.get_dbpedia_from_wikipedia(wp_title)
        return None

    def _build_dbpedia_redirect(self):
        iw.print_status("\nDBpedia: ")
        buff_redirect = defaultdict()
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DBPEDIA_REDIRECT),
            desc="Parse DBpedia redirect",
        ):
            respond = ul.parse_triple_line(line)
            if not respond:
                continue
            dp_title, _, db_redirect = respond
            db_redirect = ul.norm_wikipedia_title(db_redirect, unquote=True)
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if db_redirect and dp_title:
                buff_redirect[dp_title] = db_redirect

        if len(buff_redirect):
            self.db_redirect_dbpedia.set_items(buff_redirect)

        buff_redirect_of = defaultdict(set)
        for k, v in tqdm(
            buff_redirect.items(), total=len(buff_redirect), desc="DBpedia Redirect of"
        ):
            buff_redirect_of[v].add(k)
        del buff_redirect

        if len(buff_redirect_of):
            self.db_redirect_of_dbpedia.set_items(buff_redirect_of)

    def _build_wikidata_redirect(self):
        iw.print_status("\nWikidata: ")
        i_wd = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_PAGE, "rt", encoding="utf-8", newline="\n"
        ) as f:
            with tqdm(desc="Parse Wikidata IRI") as p_bar:
                for line in f:
                    if not line.startswith("INSERT INTO"):
                        continue
                    for v in ul.parse_sql_values(line):
                        if ul.is_wd_item(v[2]):
                            p_bar.update()
                            i_wd[v[0]] = v[2]

        buff_redirect = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            with tqdm(desc="Parse Wikidata Redirect") as p_bar:
                for line in f:
                    if not line.startswith("INSERT INTO"):
                        continue
                    for v in ul.parse_sql_values(line):
                        if ul.is_wd_item(v[2]) and i_wd.get(v[0]):
                            p_bar.update()
                            buff_redirect[i_wd[v[0]]] = v[2]

        if buff_redirect:
            self.db_redirect_wikidata.set_items(buff_redirect)

        buff_redirect_of = defaultdict(set)
        for k, v in tqdm(
            buff_redirect.items(),
            total=len(buff_redirect),
            desc="Parse Wikidata Redirect of",
        ):
            buff_redirect_of[v].add(k)
        del buff_redirect

        if len(buff_redirect_of):
            self.db_redirect_of_wikidata.set_items(buff_redirect_of)

    def _build_wikipedia_redirect_and_mapping_wikipedia_wikidata(self):
        iw.print_status("\nWikipedia: ")
        map_wp_id_title = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIPEDIA_PAGE, "rt", encoding=cf.ENCODING, newline="\n"
        ) as f:
            for line in tqdm(f, desc="Parse Wikipedia Page"):
                if not self._is_insert(line):
                    continue
                for v in ul.parse_sql_values(line):
                    if v[1] == "0":
                        wp_id = v[0]
                        wp_title = ul.norm_wikipedia_title(v[2])
                        map_wp_id_title[wp_id] = wp_title

        buff_redirect = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIPEDIA_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            for line in tqdm(f, desc="Parse Wikipedia Redirect"):
                if not self._is_insert(line):
                    continue
                for v in ul.parse_sql_values(line):
                    wp_source_id = v[0]
                    wp_source_title = map_wp_id_title.get(wp_source_id)
                    if wp_source_title:
                        wp_target_title = ul.norm_wikipedia_title(v[2])
                        buff_redirect[wp_source_title] = wp_target_title
            if len(buff_redirect):
                self.db_redirect_wikipedia.set_items(buff_redirect)

        buff_redirect_of = defaultdict(set)
        for k, v in tqdm(
            buff_redirect.items(),
            total=len(buff_redirect),
            desc="Parse Wikipedia Redirect of",
        ):
            buff_redirect_of[v].add(k)
        del buff_redirect

        if len(buff_redirect_of):
            self.db_redirect_of_wikipedia.set_items(buff_redirect_of)

        iw.print_status("\nMapping: ")
        buff_wikipedia_wikidata = defaultdict()
        with gzip.open(cf.DIR_DUMP_WIKIPEDIA_PROPS, "r") as f:
            for line in tqdm(f, desc="Parse Wikipedia -> Wikidata"):
                line = line.decode("utf-8", "ignore")
                if not self._is_insert(line):
                    continue
                for v in ul.parse_sql_values(line):
                    wd_id = v[2]
                    if v[1] == "wikibase_item" and ul.is_wd_item(wd_id):
                        wp_title = map_wp_id_title.get(v[0])
                        if not wp_title:
                            continue
                        # Mapping from Wikipedia title to Wikidata ID
                        wp_title = self.get_redirect_wikipedia(wp_title)
                        wd_id = self.get_redirect_wikidata(wd_id)

                        buff_wikipedia_wikidata[wp_title] = wd_id

            if len(buff_wikipedia_wikidata):
                self.db_wikipedia_wikidata.set_items(buff_wikipedia_wikidata)
                # inverse
                buff_wikipedia_wikidata = {
                    v: k for k, v in buff_wikipedia_wikidata.items()
                }
                self.db_wikidata_wikipedia.set_items(buff_wikipedia_wikidata)

    def _build_mapping_dbpedia_wikidata(self):
        buff_class_dp_wd = defaultdict()
        buff_prop_dp_wd = defaultdict()
        iw.print_status("\nMapping: ")
        p_bar = tqdm(desc="Parse DBpedia schema -> Wikidata ")
        c_ok = 0
        for line in iw.read_line_from_file(cf.DIR_DUMP_DBPEDIA_WIKIDATA):
            p_bar.update()
            respond = ul.parse_triple_line(line)
            if not respond or "wikidata" not in line:
                continue
            dp_title, _, wd_id = respond

            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            wd_id = ul.remove_prefix(wd_id)

            if wd_id and dp_title and ul.is_wd_item(wd_id):
                c_ok += 1
                dp_title = self.get_redirect_dbpedia(dp_title)
                wd_id = self.get_redirect_wikidata(wd_id)
                if "q" == wd_id[0].lower():
                    buff_class_dp_wd[dp_title] = wd_id
                if "p" == wd_id[0].lower():
                    dp_title = (dp_title[0].lower() + dp_title[1:]).replace("_", " ")
                    buff_prop_dp_wd[dp_title] = wd_id

        if buff_class_dp_wd:
            self.db_dbpedia_class_wikidata.set_items(buff_class_dp_wd)
            # inverse
            buff_class_dp_wd = {v: k for k, v in buff_class_dp_wd.items()}
            self.db_wikidata_dbpedia_class.set_items(buff_class_dp_wd)

        if buff_prop_dp_wd:
            self.db_dbpedia_property_wikidata.set_items(buff_prop_dp_wd)
            # inverse
            buff_prop_dp_wd = {v: k for k, v in buff_prop_dp_wd.items()}
            self.db_wikidata_dbpedia_property.set_items(buff_prop_dp_wd)

    def _build_mapping_dbpedia_wikipedia(self):
        buff_map = defaultdict()
        iw.print_status("\nMapping: ")
        p_bar = tqdm(desc="Parse DBpedia -> Wikipedia")
        c_ok = 0
        for line in iw.read_line_from_file(cf.DIR_DUMP_DP_WP):
            p_bar.update()
            respond = ul.parse_triple_line(line)
            if not respond:
                continue
            dp_title, _, wp_title = respond
            wp_title = ul.norm_wikipedia_title(wp_title, unquote=True)
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if len(wp_title) and len(dp_title):
                c_ok += 1
                dp_title = self.get_redirect_dbpedia(dp_title)
                wp_title = self.get_redirect_wikipedia(wp_title)
                buff_map[dp_title] = wp_title

        if len(buff_map):
            self.db_dbpedia_wikipedia.set_items(buff_map)
            # inverse
            buff_map = {v: k for k, v in buff_map.items()}
            self.db_wikipedia_dbpedia.set_items(buff_map)

    def build(self):
        iw.print_status("Mapping ID: ")
        self._build_dbpedia_redirect()
        self._build_wikidata_redirect()
        self._build_wikipedia_redirect_and_mapping_wikipedia_wikidata()
        self._build_mapping_dbpedia_wikidata()
        self._build_mapping_dbpedia_wikipedia()

    def test_db_wp(self):
        c = 0
        for k, v in tqdm(self.db_wikipedia_dbpedia.items()):
            c += 1
            print(f"{k} - {v} - {str(self.get_wikidata_from_wikipedia(k))}")

            if c == 1000:
                break


if __name__ == "__main__":
    mapping_id = MappingID(read_only=False)
    mapping_id.build()
    iw.print_status("Done")
    # mapping_id = MappingID()
    # assert mapping_id.get_dbpedia_from_wikidata("Q25550430") == "Gary Gygax"
    # assert mapping_id.get_dbpedia_from_wikidata("Q1379") == "Gary Gygax"
    # assert mapping_id.get_wikipedia_from_wikidata("Q1379") == "Gary Gygax"
    # assert mapping_id.get_dbpedia_from_wikipedia("Gary Gygax") == "Gary Gygax"
    # assert mapping_id.get_dbpedia_from_wikipedia("Gary Gygax") == "Gary Gygax"
    # assert mapping_id.get_dbpedia_from_wikipedia("Gary Gygax") == "Q25550430"
    # assert mapping_id.get_dbpedia_from_wikipedia("Gary Gygax") == "Q25550430"


# Log
"""

"""
