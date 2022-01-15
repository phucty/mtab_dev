# import os
#
# from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
# from api import m_f
# from time import time
#
# from api.annotator.m_semantic import annotate_api
# from collections import defaultdict
# import m_config as cf
# from werkzeug.utils import secure_filename
# from api.utilities import m_io as iw
# from api.lookup import m_entity_search
# import mistune
# from pygments import highlight
# from pygments.lexers import get_lexer_by_name
# from pygments.formatters import html
# from interface.form import FormSearch
#
#
# m_f.init()
# app = Flask(__name__)
# app.config["SECRET_KEY"] = 'ecbc8d594eb974ebb9e076cdaae3c633'
#
#
# class HighlightRenderer(mistune.Renderer):
#     def block_code(self, code, lang):
#         if not lang:
#             return '\n<pre><code>%s</code></pre>\n' % \
#                 mistune.escape(code)
#         lexer = get_lexer_by_name(lang, stripall=True)
#         formatter = html.HtmlFormatter()
#         return highlight(code, lexer, formatter)
#
#
# markdown_formatter = mistune.Markdown(renderer=HighlightRenderer())
#
#
# @app.route('/test1', methods=["GET", "POST"])
# def call_test1():
#     form_search = FormSearch()
#     if form_search.validate_on_submit():
#         flash(f"{form_search.query.label.text}: {form_search.query.data} - "
#               f"{form_search.efficient.label.text}: {form_search.efficient.data} - "
#               f"{form_search.language.label.text}: {form_search.language.data} - "
#               f"{form_search.mode.label.text}: {form_search.mode.data}",
#               "success")
#         return redirect(url_for("call_test1"))
#     return render_template('entity_search.html', title="Entity Search", form=form_search)
#
#
# def get_md_content(md_file):
#     with open(md_file) as f:
#         data = f.read()
#     title = data.split("\n")[0]
#     content = markdown_formatter(data)
#     return title, content
#
#
# @app.route('/mtabes/docs')
# def mtabes_api_docs():
#     md_file = f"{cf.DIR_ROOT}/docs/mtabes.md"
#     title, content = get_md_content(md_file)
#     return render_template('md.html', md_title=title, md_content=content)
#
#
# @app.route('/mtab/docs')
# def mtab_api_docs():
#     md_file = f"{cf.DIR_ROOT}/docs/mtab.md"
#     title, content = get_md_content(md_file)
#     return render_template('md.html', md_title=title, md_content=content)
#
#
# @app.route('/')
# def home():
#     return render_template('home.html')
#
#
# @app.route('/search')
# def search_request():
#     # index()
#     search_term = request.args.get("q")
#     start = time()
#     # responds_bm25, responds_fuzz = [], []
#     # # Aggregation
#     # responds_bm25 = m_f.m_search_e().search_wd(search_term, lang="en")
#     # responds_fuzz = m_f.m_search_f().search_wd(search_term)
#
#     # Multilingual
#     # responds_bm25 = m_f.m_search_e().search_wd(search_term, lang="en")
#
#     # responds = ul.merge_ranking([responds_fuzz, responds_bm25], weight=[1, 1], is_sorted=True)
#
#     search_mode = "a"  # a b f
#     search_lang = "en"  # en all
#     responds, responds_bm25, responds_fuzz = m_entity_search.search(search_term, lang=search_lang, mode=search_mode,
#                                                                     limit=20, expensive=False)
#     iw.print_status(
#         f"MTabES[{search_lang}|{search_mode}|{20}|{True}|{False}]: "
#         f"{len(responds)} results in {time() - start:.4f}s - {search_term}")
#
#     responds = m_f.m_items().get_search_info(responds)
#
#     # responds_bm25 = {r: r_s for r, r_s in responds_bm25}
#     # responds_fuzz = {r: r_s for r, r_s in responds_fuzz}
#     for i in range(len(responds)):
#         if search_mode == "a":
#             try:
#                 responds[i]["score_bm25"] = responds_bm25.get(responds[i]["id"], 0)
#                 responds[i]["score_fuzzy"] = responds_fuzz.get(responds[i]["id"], 0)
#             except:
#                 responds[i]["score_bm25"] = 0
#                 responds[i]["score_fuzzy"] = 0
#             responds[i]["score_bm25"] = "%.4f" % (responds[i]["score_bm25"])
#             responds[i]["score_fuzzy"] = "%.4f" % (responds[i]["score_fuzzy"])
#
#         responds[i]["score"] = "%.4f" % (responds[i]["score"])
#
#     if not responds:
#         template = {
#             "run_time": f"{(time() - start):.2f}",
#             "total": f"{len(responds):d}"
#         }
#     else:
#         template = {
#             "hits": responds,
#             "run_time": f"{(time() - start):.2f}",
#             "total": f"{len(responds):d}"
#         }
#     responds = jsonify(template)
#     return responds
#
#
# @app.route('/api/v1/search', methods=['GET'])
# def module_search():
#     start = time()
#     search_term = request.args.get("q")
#     search_mode = request.args.get("m")
#     search_lang = request.args.get("lang")
#
#     def get_bool_args(arg_name, default_v=False):
#         if request.args.get(arg_name) is None:  # 0 False, 1 True
#             tmp = default_v
#         else:
#             if int(request.args.get(arg_name)):
#                 tmp = True
#             else:
#                 tmp = False
#         return tmp
#
#     search_info = get_bool_args("info")
#     search_expensive = get_bool_args("expensive", default_v=True)
#
#     search_limit = 20
#     if request.args.get("limit"):
#         search_limit = int(request.args.get("limit"))
#
#     if not search_mode:
#         search_mode = "a"
#     if not search_lang:
#         search_lang = "en"
#
#     temp_res = defaultdict()
#
#     if not search_term:
#         temp_res["status"] = "Error: No query"
#         return jsonify(temp_res)
#
#     responds, responds_bm25, responds_fuzz = m_entity_search.search(search_term, lang=search_lang, mode=search_mode,
#                                                                     limit=search_limit, expensive=search_expensive)
#     # if search_mode == "f":  # fuzzy search
#     #     responds = m_f.m_search_f().search_wd(search_term)
#     # elif search_mode == "b":  # bm25
#     #     responds = m_f.m_search_e().search_wd(search_term, lang="all")
#     # else: # aggregation
#     #     responds_bm25 = m_f.m_search_e().search_wd(search_term, lang="en")
#     #     responds_fuzz = m_f.m_search_f().search_wd(search_term)
#     #
#     #     if len(responds_bm25) == 1:
#     #         weight = [1, 0]
#     #     else:
#     #         weight = [2, 1]
#     #     responds = ul.merge_ranking([responds_fuzz, responds_bm25], weight=weight, is_sorted=True)
#     responds = m_f.m_items().get_search_info(responds, get_info=search_info)
#     n_responds = len(responds)
#     if search_mode == "a":
#         # responds_bm25 = {r: r_s for r, r_s in responds_bm25}
#         # responds_fuzz = {r: r_s for r, r_s in responds_fuzz}
#         for i in range(len(responds)):
#             # responds[i]["score_bm25"] = responds_bm25.get(responds[i]["id"], 0)
#             # responds[i]["score_fuzzy"] = responds_fuzz.get(responds[i]["id"], 0)
#             responds[i]["score"] = responds[i]["score"]
#     run_time = time() - start
#     temp_res["run_time"] = run_time
#     temp_res["status"] = "Success"
#     iw.print_status(
#         f"MTabES[{search_lang}|{search_mode}|{search_limit}|{search_info}|{search_expensive}]: "
#         f"{n_responds} results in {run_time:.4f}s - {search_term}")
#
#     if not responds:
#         temp_res["total"] = f"0"
#
#     else:
#         temp_res["total"] = f"{len(responds):d}"
#         temp_res["hits"] = responds
#     return jsonify(temp_res)
#
#
# def upload_data(from_file):
#     filename = from_file.filename
#     if '.' not in filename or filename.rsplit('.', 1)[1].lower() not in cf.ALLOWED_EXTENSIONS:
#         return None
#
#     _, f_ext = os.path.splitext(filename)
#
#     upload_dir = f"{cf.DIR_ROOT}/data/users/uploads/{secure_filename(filename)}"
#     from_file.save(upload_dir)
#
#     # Uncompress format:
#     tables_dir, folder_dir = iw.prepare_input_tables(upload_dir)
#     os.remove(upload_dir)
#     return tables_dir, folder_dir
#
#
# @app.route('/api/v1/mtab', methods=['GET', 'POST', 'PUT'])
# def module_table_annotation():
#     temp_res = defaultdict()
#
#     if 'file' not in request.files:
#         temp_res["status"] = "Error: No input file"
#         return jsonify(temp_res)
#
#     from_file = request.files['file']
#
#     # Check format
#     input_tables, folder_dir = upload_data(from_file)
#     if input_tables:
#         temp_res["status"] = "Success"
#         temp_res["n_tables"] = len(input_tables["tables"])
#         temp_res["tables"], temp_res["log"] = annotate_api(input_tables["tables"],
#                                           input_tables["cea"],
#                                           input_tables["cta"],
#                                           input_tables["cpa"], is_screen=True)
#         # Call annotator
#     else:
#         temp_res["status"] = "Error: Incorrect Zip files or table format."
#     iw.delete_folder(folder_dir)
#     return jsonify(temp_res)
#
#     # profile = request.files['profile']
#     # profile.save(os.path.join(uploads_dir, secure_filename(profile.filename)))
#     #
#     #     # save each "charts" file
#     #     for file in request.files.getlist('charts'):
#     #         file.save(os.path.join(uploads_dir, secure_filename(file.name)))
#     #
#     #     return redirect(url_for('upload'))
#
#
# if __name__ == '__main__':
#     # iw.prepare_input_tables(cf.DIR_SAMPLE_ZIP)
#     app.run(host='0.0.0.0', port=5000, debug=True)
#     # curl -X GET "http://119.172.242.147/api/v1/search?q=Hideaki%20Takead"
#     # curl -X POST -F file=@"/Users/mtab/git/mtab/data/semtab/mytables.zip" http://119.172.242.147/api/v1/mtab
#     # curl -X POST -F file=@"YOUR_ZIP_FILE_LOCATION/mytables.zip" http://119.172.242.147/api/v1/mtab
#     # curl -X GET "http://119.172.242.147/api/v1/search?m=a&q=Hideaki%20Takead"
#     # curl -X GET "http://localhost:5000/api/v1/search?m=a&q=Hideaki%20Takead"
#     # curl -X GET "http://localhost:5000/api/v1/search?m=f&q=2MASS%20J10540655-0031018"
#     # curl -X POST -F "file=@" http://localhost:5000/api/v1/mtab/
#     # -F "file=@/Users/chuck/Desktop/33823.xml"
#     # curl -X POST -F file=@"/Users/phucnguyen/git/mtab/data/semtab/mytables.zip" http://localhost:5000/api/v1/mtab
#     # curl -X POST -F file=@"/Users/mtab/git/mtab/data/semtab/mytables.zip" http://localhost:5000/api/v1/mtab
#     # curl -T "/Users/mtab/git/mtab/data/semtab/mytables.zip" http://localhost:5000/api/v1/mtab
#     # Ikyua Yamada
#     # "武田英明",
#     # "Tokio",
#     # {irconium-83
#     # R. Millar
#     # 2MASS J10540655-0031018
