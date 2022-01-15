import gzip
from collections import defaultdict
from datetime import timedelta
from time import time

from tqdm import tqdm

import m_config as cf
from api import m_f
from api.resources.m_db_item import DBItem
from api.utilities import m_io as iw
from api.utilities import m_utils as ul


class MappingID(DBItem):
    def __init__(self, db_file=cf.DIR_MAPPING_ID):
        super().__init__(db_file=db_file, max_db=14, map_size=cf.SIZE_1GB * 10)

        self.db_redirect_wikipedia = self._env.open_db(b"__wpredirect__")
        self.db_redirect_wikidata = self._env.open_db(b"__wdredirect__")
        self.db_redirect_dbpedia = self._env.open_db(b"__dpredirect__")

        self.db_redirect_of_wikipedia = self._env.open_db(b"__wpredirectof__")
        self.db_redirect_of_wikidata = self._env.open_db(b"__wdredirectof__")
        self.db_redirect_of_dbpedia = self._env.open_db(b"__dpredirectof__")

        self.db_wikipedia_wikidata = self._env.open_db(b"__wpwd__")
        self.db_wikidata_wikipedia = self._env.open_db(b"__wdwp__")

        self.db_dbpedia_wikipedia = self._env.open_db(b"__dpwp__")
        self.db_wikipedia_dbpedia = self._env.open_db(b"__wpdb__")

        self.db_dbpedia_class_wikidata = self._env.open_db(b"__class_dpwd__")
        self.db_dbpedia_property_wikidata = self._env.open_db(b"__prop_dpwd__")
        self.db_wikidata_dbpedia_class = self._env.open_db(b"__class_wddp__")
        self.db_wikidata_dbpedia_property = self._env.open_db(b"__prop_wddp__")

    @staticmethod
    def _is_insert(line):
        return line.startswith("INSERT INTO")

    def _get_db_obj(self, db, key, return_default, compress_value=False):
        respond = self.get_value(db, key, compress_value=compress_value)
        if respond is None or not respond:
            return return_default
        return respond

    def get_redirect_dbpedia(self, key):
        return self._get_db_obj(
            self.db_redirect_dbpedia, key, key, compress_value=False
        )

    def get_redirect_wikipedia(self, key):
        return self._get_db_obj(
            self.db_redirect_wikipedia, key, key, compress_value=False
        )

    def get_redirect_wikidata(self, key):
        return self._get_db_obj(
            self.db_redirect_wikidata, key, key, compress_value=False
        )

    def get_redirect_of_wikidata(self, key):
        return self._get_db_obj(
            self.db_redirect_of_wikidata, key, set(), compress_value=True
        )

    def get_redirect_of_wikipedia(self, key):
        return self._get_db_obj(
            self.db_redirect_of_wikipedia, key, set(), compress_value=True
        )

    def get_redirect_of_dbpedia(self, key):
        return self._get_db_obj(
            self.db_redirect_of_dbpedia, key, set(), compress_value=True
        )

    def _get_items_with_redirects(
        self, item_id, db_item, func_redirect_item=None, func_tar_redirect=None
    ):
        respond = self.get_value(db_item, item_id, compress_value=False)
        if not respond and func_redirect_item:
            item_id_redirect = func_redirect_item(item_id)
            if item_id_redirect != item_id:
                respond = self.get_value(
                    db_item, item_id_redirect, compress_value=False
                )

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

    def get_wikipedia_from_dbpedia(self, db_title):
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

    def get_wikidata_from_dbpedia_prop(self, dp_prop):
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
        wp_title = self.get_wikipedia_from_dbpedia(db_title)
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

    @staticmethod
    def _build_inverse_set_obj(buff_obj):
        buff_obj_inv = defaultdict(set)
        for k, v in buff_obj.items():
            buff_obj_inv[v].add(k)
        return buff_obj_inv

    def _build_dbpedia_redirects(self):
        buff_obj = defaultdict()
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DBPEDIA_REDIRECT),
            desc="DBpedia redirects",
        ):
            respond = ul.parse_triple_line(line)
            if not respond:
                continue
            dp_title, _, db_redirect = respond
            db_redirect = ul.norm_wikipedia_title(db_redirect, unquote=True)
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if db_redirect and dp_title:
                buff_obj[dp_title] = db_redirect

        if buff_obj:
            self.write_bulk(
                self._env, self.db_redirect_dbpedia, buff_obj, compress_value=False
            )
            buff_obj = self._build_inverse_set_obj(buff_obj)
            self.write_bulk(
                self._env, self.db_redirect_of_dbpedia, buff_obj, compress_value=True
            )

    def _build_wikidata_redirects(self):
        i_wd = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_PAGE, "rt", encoding="utf-8", newline="\n"
        ) as f:
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in ul.parse_sql_values(line):
                    if ul.is_wd_item(v[2]):
                        i_wd[v[0]] = v[2]

        buff_obj = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIDATA_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            p_bar = tqdm(desc="Wikidata redirects")
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in ul.parse_sql_values(line):
                    if ul.is_wd_item(v[2]) and i_wd.get(v[0]):
                        p_bar.update()
                        buff_obj[i_wd[v[0]]] = v[2]
            p_bar.close()

        if buff_obj:
            self.write_bulk(
                self._env, self.db_redirect_wikidata, buff_obj, compress_value=False
            )
            buff_obj = self._build_inverse_set_obj(buff_obj)
            self.write_bulk(
                self._env, self.db_redirect_of_wikidata, buff_obj, compress_value=True
            )

    def _build_wikipedia_redirect_and_mapping_wikipedia_wikidata(self):
        map_wp_id_title = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIPEDIA_PAGE, "rt", encoding=cf.ENCODING, newline="\n"
        ) as f:
            for line in f:
                if not self._is_insert(line):
                    continue
                for v in ul.parse_sql_values(line):
                    if v[1] == "0":
                        wp_id = v[0]
                        wp_title = ul.norm_wikipedia_title(v[2])
                        map_wp_id_title[wp_id] = wp_title

        buff_obj = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WIKIPEDIA_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            p_bar = tqdm(desc="Wikipedia redirects")
            for line in f:
                if not self._is_insert(line):
                    continue
                for v in ul.parse_sql_values(line):
                    wp_source_id = v[0]
                    wp_source_title = map_wp_id_title.get(wp_source_id)
                    if wp_source_title:
                        wp_target_title = ul.norm_wikipedia_title(v[2])
                        buff_obj[wp_source_title] = wp_target_title
                        p_bar.update()
            p_bar.close()
            if buff_obj:
                self.write_bulk(
                    self._env,
                    self.db_redirect_wikipedia,
                    buff_obj,
                    compress_value=False,
                )
                buff_obj = self._build_inverse_set_obj(buff_obj)
                self.write_bulk(
                    self._env,
                    self.db_redirect_of_wikipedia,
                    buff_obj,
                    compress_value=True,
                )

        buff_map = defaultdict()
        with gzip.open(cf.DIR_DUMP_WIKIPEDIA_PROPS, "r") as f:
            p_bar = tqdm(desc="Mapping Wikipedia title -> Wikidata ID")
            for line in f:
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
                        # wp_title = self.get_redirect_wikipedia(wp_title)
                        # wd_id = self.get_redirect_wikidata(wd_id)
                        if buff_map.get(wp_title) and buff_map.get(wp_title) != wd_id:
                            iw.print_status(
                                f"{wp_title}: {buff_map.get(wp_title)} - {wd_id}",
                                is_screen=False,
                            )

                        buff_map[wp_title] = wd_id
                        p_bar.update()
            p_bar.close()

            if buff_map:
                self.write_bulk(
                    self._env,
                    self.db_wikipedia_wikidata,
                    buff_map,
                    compress_value=False,
                )
                buff_map = {v: k for k, v in buff_map.items()}
                self.write_bulk(
                    self._env,
                    self.db_wikidata_wikipedia,
                    buff_map,
                    compress_value=False,
                )

    def _build_mapping_dbpedia_wikidata(self):
        buff_class = defaultdict()
        buff_prop = defaultdict()
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DBPEDIA_WIKIDATA),
            desc="Mapping DBpedia classes, properties -> Wikidata ID",
        ):
            respond = ul.parse_triple_line(line)
            if not respond or "wikidata" not in line:
                continue
            dp_title, _, wd_id = respond
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            wd_id = ul.remove_prefix(wd_id)

            if wd_id and dp_title and ul.is_wd_item(wd_id):
                # dp_title = self.get_redirect_dbpedia(dp_title)
                # wd_id = self.get_redirect_wikidata(wd_id)
                if "q" == wd_id[0].lower():
                    if buff_class.get(dp_title) and buff_class.get(dp_title) != wd_id:
                        iw.print_status(
                            f"{dp_title}: {buff_class.get(dp_title)} - {wd_id}",
                            is_screen=False,
                        )
                    buff_class[dp_title] = wd_id
                if "p" == wd_id[0].lower():
                    dp_title = (dp_title[0].lower() + dp_title[1:]).replace("_", " ")
                    if buff_prop.get(dp_title) and buff_prop.get(dp_title) != wd_id:
                        iw.print_status(
                            f"{dp_title}: {buff_prop.get(dp_title)} - {wd_id}",
                            is_screen=False,
                        )
                    buff_prop[dp_title] = wd_id

        if buff_class:
            self.write_bulk(
                self._env,
                self.db_dbpedia_class_wikidata,
                buff_class,
                compress_value=False,
            )
            buff_class = {v: k for k, v in buff_class.items()}
            self.write_bulk(
                self._env,
                self.db_wikidata_dbpedia_class,
                buff_class,
                compress_value=False,
            )
        if buff_prop:
            self.write_bulk(
                self._env,
                self.db_dbpedia_property_wikidata,
                buff_prop,
                compress_value=False,
            )
            buff_prop = {v: k for k, v in buff_prop.items()}
            self.write_bulk(
                self._env,
                self.db_wikidata_dbpedia_property,
                buff_prop,
                compress_value=False,
            )

    def _build_mapping_dbpedia_wikipedia(self):
        buff_map = defaultdict()
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_WP),
            desc="Mapping DBpedia title -> Wikipedia title",
        ):
            respond = ul.parse_triple_line(line)
            if not respond:
                continue
            dp_title, dp_prop, wp_title = respond
            if dp_prop != "primaryTopic":
                continue
            wp_title = ul.norm_wikipedia_title(wp_title, unquote=True)
            dp_title = ul.norm_wikipedia_title(dp_title, unquote=True)
            if wp_title and dp_title:
                # dp_title = self.get_redirect_dbpedia(dp_title)
                # wp_title = self.get_redirect_wikipedia(wp_title)
                if buff_map.get(dp_title) and buff_map.get(dp_title) != wp_title:
                    iw.print_status(
                        f"{dp_title}: {buff_map.get(dp_title)} - {wp_title}",
                        is_screen=False,
                    )
                buff_map[dp_title] = wp_title

        if buff_map:
            self.write_bulk(
                self._env, self.db_dbpedia_wikipedia, buff_map, compress_value=False
            )
            buff_map = {v: k for k, v in buff_map.items()}
            self.write_bulk(
                self._env, self.db_wikipedia_dbpedia, buff_map, compress_value=False
            )

    def build(self):
        iw.print_status("\nExtract redirects, and mapping ID resources: ")
        start = time()
        self._build_dbpedia_redirects()
        self._build_wikidata_redirects()
        self._build_wikipedia_redirect_and_mapping_wikipedia_wikidata()
        self._build_mapping_dbpedia_wikidata()
        self._build_mapping_dbpedia_wikipedia()
        # self.copy_lmdb()
        iw.print_status(f"Run time: {timedelta(seconds=time() - start)}")

    def test_db_wp(self):
        c = 0
        for k, v in tqdm(self.get_db_iter(self.db_wikipedia_dbpedia)):
            c += 1
            print(f"{k} - {v} - {str(self.get_wikidata_from_wikipedia(k))}")
            if c == 1000:
                break

    def modify_env_compress_value(self):
        self.modify_db_compress_value(
            self.db_redirect_wikipedia, c_compress_value=True, n_compress_value=False
        )
        self.modify_db_compress_value(
            self.db_redirect_wikidata, c_compress_value=True, n_compress_value=False
        )
        self.modify_db_compress_value(
            self.db_redirect_dbpedia, c_compress_value=True, n_compress_value=False
        )

        self.modify_db_compress_value(
            self.db_wikipedia_wikidata, c_compress_value=True, n_compress_value=False
        )
        self.modify_db_compress_value(
            self.db_wikidata_wikipedia, c_compress_value=True, n_compress_value=False
        )

        self.modify_db_compress_value(
            self.db_dbpedia_wikipedia, c_compress_value=True, n_compress_value=False
        )
        self.modify_db_compress_value(
            self.db_wikipedia_dbpedia, c_compress_value=True, n_compress_value=False
        )

        self.modify_db_compress_value(
            self.db_dbpedia_class_wikidata,
            c_compress_value=True,
            n_compress_value=False,
        )
        self.modify_db_compress_value(
            self.db_dbpedia_property_wikidata,
            c_compress_value=True,
            n_compress_value=False,
        )
        self.modify_db_compress_value(
            self.db_wikidata_dbpedia_class,
            c_compress_value=True,
            n_compress_value=False,
        )
        self.modify_db_compress_value(
            self.db_wikidata_dbpedia_property,
            c_compress_value=True,
            n_compress_value=False,
        )
        self.copy_lmdb()


if __name__ == "__main__":
    # m_f.init(is_log=True)
    mapping_id = MappingID()
    mapping_id.build()
    mapping_id.modify_env_compress_value()
    iw.print_status("Done")
    assert mapping_id.get_dbpedia_from_wikidata("Q25550430") == "Gary Gygax"
    assert mapping_id.get_dbpedia_from_wikipedia("Gary Gygax") == "Gary Gygax"

    assert mapping_id.get_wikipedia_from_wikidata("Q1379") == "Gary Gygax"
    assert mapping_id.get_wikipedia_from_dbpedia("Gary Gygax") == "Gary Gygax"

    assert mapping_id.get_wikidata_from_wikipedia("Gary Gygax") == "Q25550430"
    assert mapping_id.get_wikidata_from_dbpedia("Gary Gygax") == "Q25550430"


"""
Extract redirects, and mapping ID resources: 
DBpedia redirects: 9728112it [01:44, 93173.09it/s]
Wikidata redirects: 3502730it [00:24, 142302.85it/s]
Wikipedia redirects: 9775604it [00:47, 206631.48it/s]
Mapping Wikipedia title -> Wikidata ID: 6573989it [01:49, 60135.56it/s]
Mapping DBpedia classes, properties -> Wikidata ID: 32334it [00:00, 205172.83it/s]
Mapping DBpedia title -> Wikipedia title: 63616012it [49:39, 21353.26it/s]
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 0, 'entries': 14}
"""
