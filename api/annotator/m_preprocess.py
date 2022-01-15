import os
import stat
from collections import Counter
from urllib.parse import urlparse

import markdown
import pandas
import petl

from api.utilities import m_io as iw
import m_config as cf
from collections import defaultdict
from api.utilities import m_utils as ul


def input_validation(source_type, source):
    if not source:
        return False

    # Text validation
    if source_type == cf.SourceType.TEXT:
        if not isinstance(source, str):
            return False

    # File validation
    elif source_type == cf.SourceType.FILE:
        if not isinstance(source, str) or not os.path.isfile(source):
            return False

        file_ext = os.path.splitext(source)[1][1:]
        if file_ext not in cf.TABLE_EXTENSIONS:
            return False

        try:
            stat.S_ISFIFO(os.stat(source).st_mode)
        except (OSError, ValueError):
            return TypeError

    # URL validation
    elif source_type == cf.SourceType.URL:
        scheme = urlparse(source).scheme
        if scheme not in cf.HTTP:
            return False

    # URL object matrix
    elif source_type == cf.SourceType.OBJ:
        if isinstance(source, str):
            return False
    else:
        return False

    return True


def load_table(source_type, source, table_name=""):
    def parse_xml_table(source):
        tables_xml = pandas.read_html(source)
        if tables_xml:
            return [tables_xml[0].columns.values.tolist()] + tables_xml[
                0
            ].values.tolist()
        else:
            return None

    return_obj = {"encoding": "utf-8", "cell": [], "name": table_name}
    table_obj = None
    # parse source metadata
    if source_type == cf.SourceType.FILE or source_type == cf.SourceType.TEXT:
        if source_type == cf.SourceType.FILE:

            return_obj["encoding"] = iw.get_encoding(source)
            file_ext = os.path.splitext(source)[1][1:]
            return_obj["name"] = os.path.splitext(os.path.basename(source))[0]
            if file_ext == "csv":
                table_obj = iw.load_object_csv(source, encoding=return_obj["encoding"])
                # file_dialect = iw.get_dialect(source, return_obj["encoding"])
                # if file_dialect:
                #     table_obj = petl.fromcsv(
                #         source, encoding=return_obj["encoding"], dialect=file_dialect
                #     )
                # else:
                #     table_obj = petl.fromcsv(source, encoding=return_obj["encoding"])
            elif file_ext == "tsv":
                table_obj = petl.fromtsv(source, encoding=return_obj["encoding"])
            elif file_ext == "txt":
                table_obj = petl.fromtext(source, encoding=return_obj["encoding"])
            elif file_ext == "xls":
                table_obj = petl.fromxls(source, encoding=return_obj["encoding"])
            elif file_ext in ["xlsm", "xlsb", "xltx", "xlsx", "xlt", "xltm"]:
                table_obj = petl.fromxlsx(source)
            elif file_ext == "xml":
                table_obj = parse_xml_table(source)
        else:
            # csv style
            counter = Counter(source)
            most_common = {k for k, _ in counter.most_common(3)}

            return_obj["encoding"] = iw.get_encoding(source)

            if most_common == {" ", "-", "|"}:
                table_obj = markdown.markdown(
                    source, extensions=["markdown.extensions.tables"]
                )
                table_obj = parse_xml_table(table_obj)
            else:
                file_dialect = iw.get_dialect(source, return_obj["encoding"])
                source = petl.MemorySource(str.encode(source))
                if file_dialect:
                    table_obj = petl.fromcsv(
                        source, encoding=return_obj["encoding"], dialect=file_dialect
                    )
                else:
                    table_obj = petl.fromcsv(source, encoding=return_obj["encoding"])

    elif source_type == cf.SourceType.URL:
        pass
    elif source_type == cf.SourceType.OBJ:
        table_obj = source

    # Parse cell
    if table_obj:
        for row in table_obj:
            row_norm = []
            for col in row:
                tmp_cell = ul.norm_text(str(col), punctuations=True, lower=False)
                # tmp_date = ul.get_date(tmp_cell)
                # if tmp_date:
                #     tmp_cell = tmp_date
                row_norm.append(tmp_cell)
            if row_norm:
                # row = ftfy.fix_text(row)
                return_obj["cell"].append(row_norm)

    return return_obj


def run(source_type, source, table_name=None):
    table_obj = {
        "type": source_type,
        "source": source,
        "validation": input_validation(source_type, source),
        "cell": defaultdict(),
        "encoding": None,
        "name": None,
    }
    if table_obj["validation"]:
        return_obj = load_table(source_type, source, table_name)
        table_obj["cell"]["values"] = return_obj["cell"]
        table_obj["encoding"] = return_obj["encoding"]
        table_obj["name"] = return_obj["name"]
    return table_obj
