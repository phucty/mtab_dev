import os
from api.annotator.m_input import TargetCEA, TargetCTA, TargetCPA

from flask import render_template, request, jsonify, flash, redirect, url_for
from api import m_f
from time import time
from api.annotator.main import annotate_api, template_encode_results, run
from collections import defaultdict
import m_config as cf
from werkzeug.utils import secure_filename
from api.utilities import m_io as iw
from api.lookup import m_entity_search
from interface.form import FormSearch, FormTabAnnotation
from interface import app, markdown_formatter
from marshmallow import Schema, fields
from flask_paginate import Pagination, get_page_args
import interface
from api.utilities import m_utils as ul


def call_mtabes(query, mode="a", lang="en", expensive=False, limit=20):
    start = time()
    responds, r_bm25, r_fuzz = m_entity_search.search(
        query, lang=lang, mode=mode, limit=limit, expensive=expensive
    )
    run_time = time() - start
    responds = m_f.m_items().get_search_info(responds)
    iw.print_status(
        f"MTabES[{lang}|{mode}|{20}|{True}|{False}]: "
        f"{len(responds)} results in {time() - start:.4f}s - {query}"
    )

    for i in range(len(responds)):
        if mode == "a":
            try:
                responds[i]["score_bm25"] = r_bm25.get(responds[i]["id"], 0)
                responds[i]["score_fuzzy"] = r_fuzz.get(responds[i]["id"], 0)
            except:
                responds[i]["score_bm25"] = 0
                responds[i]["score_fuzzy"] = 0
            responds[i]["score_bm25"] = "%.4f" % (responds[i]["score_bm25"])
            responds[i]["score_fuzzy"] = "%.4f" % (responds[i]["score_fuzzy"])

        responds[i]["score"] = "%.4f" % (responds[i]["score"])
    return responds, run_time


# @app.route('/test1', methods=["GET", "POST"])
@app.route("/mtabes", methods=["GET", "POST"])
def call_route_mtabes():
    form = FormSearch()
    # page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    if form.validate_on_submit():
        # if form.limit.data < 1 or form.limit.data > 100:
        #     flash("limit: integer from 1 to 100", "danger")
        #     limit = 20
        # else:
        #     limit = form.limit.data
        criteria = {
            "query": form.query.data,
            "mode": form.mode.data,
            "lang": form.language.data,
            "efficient": form.efficient.data,
            "limit": form.limit.data,
        }
        # if not interface.session_criteria or criteria != interface.session_criteria:
        responds, run_time = call_mtabes(
            query=form.query.data,
            mode=criteria["mode"],
            lang=criteria["lang"],
            expensive=not criteria["efficient"],
            limit=criteria["limit"],
        )
        interface.session_criteria = criteria
        interface.session_responds = responds
        interface.session_total = len(responds)
        interface.session_run_time = f"{run_time:.2f}"
        interface.form_es = form
        page, offset, per_page = 1, 0, 10
        pagination = None
        return render_template(
            "entity_search.html",
            form=form,
            responds=responds,
            page=page,
            per_page=per_page,
            pagination=pagination,
            run_time=f"{run_time:.2f}",
            total=f"{len(responds):d}",
        )

    # page, per_page, offset = get_page_args(page_parameter='page', per_page_parameter='per_page')
    # if interface.form_es:
    #     pagination_objs = interface.session_responds[offset:offset + per_page]
    #     pagination = Pagination(page=page, per_page=per_page, total=interface.session_total, css_framework='bootstrap4')
    #     return render_template('entity_search.html',
    #                            form=interface.form_es,
    #                            responds=pagination_objs,
    #                            page=page,
    #                            per_page=per_page,
    #                            pagination=pagination,
    #                            run_time=interface.session_run_time,
    #                            total=f"{interface.session_total:d}")

    # return redirect(url_for("call_test1"))
    # if interface.session_criteria:
    #     flash("limit: integer from 1 to 100", "danger")
    return render_template("entity_search.html", form=form)


@app.route("/", methods=["GET", "POST"])
@app.route("/mtab", methods=["GET", "POST"])
def call_mtab():
    form = FormTabAnnotation()
    if form.validate_on_submit():
        annotations, run_time, total, log = None, None, 0, None

        if form.annotation1.data and form.table_text_content.data:
            start = time()
            annotations, log = template_encode_results(
                cf.SourceType.TEXT,
                form.table_text_content.data,
                is_screen=False,
                predict_target=True,
            )
            run_time = f"{time() - start:.2f}"
            total = 1

        if form.annotation2.data and form.table_file_upload.data:
            upload_dir, f_ext = upload_data(form.table_file_upload.data)

            if form.table_file_upload.data.filename.endswith(".zip"):
                tables_dir, folder_dir = iw.prepare_input_tables(upload_dir)
                if not tables_dir:
                    return render_template("table_annotation.html", form=form, total=-1)
                limit_tables = iw.get_files_from_dir(
                    folder_dir + "/tables",
                    limit_reader=cf.LIMIT_TABLES,
                    is_sort=True,
                    reverse=False,
                )
                # limit_tables = [
                #     dir_table
                #     for dir_table in limit_tables
                #     if "1d09a099d3964602aca9425adcde89cd" in dir_table
                # ]
                start = time()
                annotations, log = template_encode_results(
                    cf.SourceType.FILE,
                    dir_tables=limit_tables,
                    dir_cea=tables_dir["cea"],
                    dir_cta=tables_dir["cta"],
                    dir_cpa=tables_dir["cpa"],
                    predict_target=True,
                    is_screen=False,
                )
                run_time = f"{time() - start:.2f}"
                total = len(annotations)
                iw.delete_folder(folder_dir)
            else:
                start = time()
                annotations, log = template_encode_results(
                    cf.SourceType.FILE, upload_dir, is_screen=False, predict_target=True
                )
                run_time = f"{time() - start:.2f}"
                total = 1
            if os.path.exists(upload_dir):
                os.remove(upload_dir)

        return render_template(
            "table_annotation.html",
            form=form,
            annotations=annotations,
            run_time=run_time,
            total=total,
            log=log,
        )

        # return redirect(url_for("call_mtab"))

    return render_template("table_annotation.html", form=form, total=-1)


def get_md_content(md_file):
    with open(md_file, encoding="utf-8") as f:
        data = f.read()
    title = data.split("\n")[0]
    content = markdown_formatter(data)
    return title, content


@app.route("/mtabes/docs")
def mtabes_api_docs():
    md_file = f"{cf.DIR_ROOT}/docs/mtabes.md"
    title, content = get_md_content(md_file)
    # content = content.replace("\"../interface/static/", "\"{{url_for('static',filename='")
    # content = content.replace(".png\"", ".png')}}\"")
    return render_template("md.html", md_title=title, md_content=content)


@app.route("/mtab/docs")
def mtab_api_docs():
    md_file = f"{cf.DIR_ROOT}/docs/mtab.md"
    title, content = get_md_content(md_file)
    return render_template("md.html", md_title=title, md_content=content)


# @app.route('/')
# def home():
#     return render_template('home.html')


# @app.route('/search')
# def search_request():
#     search_term = request.args.get("q")
#     search_mode = "a"  # a b f
#     search_lang = "en"  # en all
#     responds, run_time = call_mtabes(query=search_term, mode=search_mode, lang=search_lang, expensive=False)
#
#     if not responds:
#         template = {
#             "run_time": f"{run_time:.2f}",
#             "total": f"{len(responds):d}"
#         }
#     else:
#         template = {
#             "hits": responds,
#             "run_time": f"{run_time:.2f}",
#             "total": f"{len(responds):d}"
#         }
#     responds = jsonify(template)
#     return responds


@app.route("/api/v1/search", methods=["GET"])
def module_search():
    start = time()
    search_term = request.args.get("q")
    search_mode = request.args.get("m")
    search_lang = request.args.get("lang")

    def get_bool_args(arg_name, default_v=False):
        if request.args.get(arg_name) is None:  # 0 False, 1 True
            tmp = default_v
        else:
            if int(request.args.get(arg_name)):
                tmp = True
            else:
                tmp = False
        return tmp

    search_info = get_bool_args("info")
    search_expensive = get_bool_args("expensive", default_v=True)

    search_limit = 20
    if request.args.get("limit"):
        search_limit = int(request.args.get("limit"))

    if not search_mode:
        search_mode = "a"
    if not search_lang:
        search_lang = "en"

    temp_res = defaultdict()

    if not search_term:
        temp_res["status"] = "Error: No query"
        return jsonify(temp_res)

    responds, responds_bm25, responds_fuzz = m_entity_search.search(
        search_term,
        lang=search_lang,
        mode=search_mode,
        limit=search_limit,
        expensive=search_expensive,
    )
    responds = m_f.m_items().get_search_info(responds, get_info=search_info)
    n_responds = len(responds)
    if search_mode == "a":
        for i in range(len(responds)):
            responds[i]["score"] = responds[i]["score"]
    run_time = time() - start
    temp_res["run_time"] = run_time
    temp_res["status"] = "Success"
    iw.print_status(
        f"MTabES[{search_lang}|{search_mode}|{search_limit}|{search_info}|{search_expensive}]: "
        f"{n_responds} results in {run_time:.4f}s - {search_term}"
    )

    if not responds:
        temp_res["total"] = f"0"

    else:
        temp_res["total"] = f"{len(responds):d}"
        temp_res["hits"] = responds
    return jsonify(temp_res)


def upload_data(from_file):
    filename = from_file.filename
    if (
        "." not in filename
        or filename.rsplit(".", 1)[1].lower() not in cf.ALLOWED_EXTENSIONS
    ):
        return None

    _, f_ext = os.path.splitext(filename)

    upload_dir = f"{cf.DIR_ROOT}/data/users/uploads/{secure_filename(filename)}"
    from_file.save(upload_dir)
    return upload_dir, f_ext


@app.route("/api/v1/mtab", methods=["GET", "POST", "PUT"])
def module_table_annotation():
    temp_res = defaultdict()

    if "file" not in request.files:
        temp_res["status"] = "Error: No input file"
        return jsonify(temp_res)

    cea_tar_limit = cf.LIMIT_CEA_TAR
    if request.args.get("limit"):
        try:
            cea_tar_limit = int(request.args.get("limit"))
        except:
            pass
    from_file = request.files["file"]

    _, f_ext = os.path.splitext(from_file.filename)
    # iw.print_status(f_ext)
    if f_ext[1:].lower() not in cf.ALLOWED_EXTENSIONS:
        temp_res["status"] = "Error: Incorrect format. We support %s." % ", ".join(
            [i.upper() for i in cf.ALLOWED_EXTENSIONS]
        )
        return jsonify(temp_res)

    # Check format
    upload_dir, f_ext = upload_data(from_file)
    if f_ext == ".zip":
        # Uncompress format:
        tables_dir, folder_dir = iw.prepare_input_tables(upload_dir)

        if tables_dir:
            temp_res["status"] = "Success"
            limit_tables = iw.get_files_from_dir(
                folder_dir + "/tables",
                limit_reader=cf.LIMIT_TABLES,
                is_sort=True,
                reverse=False,
            )
            temp_res["n_tables"] = len(limit_tables)
            results, log = annotate_api(
                cf.SourceType.FILE,
                limit_tables,
                tables_dir["cea"],
                tables_dir["cta"],
                tables_dir["cpa"],
                is_screen=False,
                limit_cea=cea_tar_limit,
            )
            # Call annotator
            temp_res["tables"] = results
        else:
            temp_res["status"] = "Error: Incorrect table format."
        iw.delete_folder(folder_dir)
    else:
        temp_res["n_tables"] = 1
        results, log = annotate_api(
            cf.SourceType.FILE, upload_dir, is_screen=False, limit_cea=cea_tar_limit
        )
        # Call annotator
        temp_res["status"] = "Success"
        temp_res["tables"] = results
    try:
        os.remove(upload_dir)
    except:
        pass
    return jsonify(temp_res)

    # profile = request.files['profile']
    # profile.save(os.path.join(uploads_dir, secure_filename(profile.filename)))
    #
    #     # save each "charts" file
    #     for file in request.files.getlist('charts'):
    #         file.save(os.path.join(uploads_dir, secure_filename(file.name)))
    #
    #     return redirect(url_for('upload'))


@app.route("/api/v1.1/mtab", methods=["POST"])
def module_mtab_1_1():
    temp_res = defaultdict()

    table_content = request.json.get("table")
    if not table_content:
        temp_res["status"] = "Error: Could not parse table content"
        return jsonify(temp_res)
    error_message = ""
    tar_cea = request.json.get("tar_cea")
    if tar_cea:
        tmp_tar_cea = TargetCEA(request.json.get("table_name"))
        for row, col in tar_cea:
            tmp_tar_cea.add(row, col)
        tar_cea = tmp_tar_cea

    predict_target = False
    if request.json.get("predict_target") and request.json.get("predict_target") in [
        True,
        False,
    ]:
        predict_target = request.json.get("predict_target")

    search_mode = "b"
    if request.json.get("search_mode") and request.json.get("search_mode") in [
        "a",
        "b",
        "f",
    ]:
        search_mode = request.json.get("search_mode")

    tar_cta = request.json.get("tar_cta")
    if tar_cta:
        tmp_tar_cta = TargetCTA(request.json.get("table_name"))
        for col in tar_cta:
            tmp_tar_cta.add(col)
        tar_cta = tmp_tar_cta

    tar_cpa = request.json.get("tar_cpa")
    if tar_cpa:
        tmp_tar_cpa = TargetCPA(request.json.get("table_name"))
        for col1, col2 in tar_cpa:
            tmp_tar_cpa.add(col1, col2)
        tar_cpa = tmp_tar_cpa

    # temp_res["input"] = request.json

    try:
        table_obj, run_time = run(
            source_type=cf.SourceType.OBJ,
            source=table_content,
            table_name=request.json.get("table_name"),
            predict_target=predict_target,
            tar_cea=tar_cea,
            tar_cta=tar_cta,
            tar_cpa=tar_cpa,
            limit_cea=1e10,
            search_mode=search_mode,
        )
        if table_obj:
            temp_res["status"] = "Success"
            iw.print_status(table_obj.get("log"), is_screen=False)
            temp_res["table_name"] = request.json.get("table_name")
            temp_res["run_time"] = run_time
            temp_res["structure"] = {
                "encoding": table_obj["encoding"],
                "table type": "horizontal relational",
                "rows": table_obj["stats"]["row"],
                "columns": table_obj["stats"]["col"],
                "cells": table_obj["stats"]["cell"],
                "r_cells": table_obj["stats"]["r_cell"],
                "headers": table_obj["headers"],
                "core_attribute": table_obj["core_attribute"],
            }
            temp_res["semantic"] = {
                "cea": [[r, c, a] for t_id, r, c, a in table_obj["res_cea"]],
                "cta": [
                    [c, [ul.select_oldest_wd(a)]] for t_id, c, a in table_obj["res_cta"]
                ],
                "cpa": [[c1, c2, a] for t_id, c1, c2, a in table_obj["res_cpa"]],
            }
            return jsonify(temp_res)
    except Exception as message:
        error_message = message
        iw.print_status(request.json.get("table_name"))
        iw.print_status(message)

    if not error_message:
        temp_res["status"] = "Error: Annotation"
    else:
        temp_res["status"] = error_message

    try:
        json_res = jsonify(temp_res)
    except:
        json_res = None
    return json_res
