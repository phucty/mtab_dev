import mistune
from flask import Flask
from pygments import highlight
from pygments.formatters import html
from pygments.lexers import get_lexer_by_name
from api import m_f
import http.client

http.client._MAXLINE = 655360


m_f.init(is_log=True)

app = Flask(__name__)

# pagination = Pagination(app)
app.config["SECRET_KEY"] = "YOUR KEY :) I do not forget to hide this"
app.config["PAGE_SIZE"] = 20
app.config["VISIBLE_PAGE_COUNT"] = 10


class HighlightRenderer(mistune.Renderer):
    def block_code(self, code, lang):
        if not lang:
            return "\n<pre><code>%s</code></pre>\n" % mistune.escape(code)
        lexer = get_lexer_by_name(lang, stripall=True)
        formatter = html.HtmlFormatter()
        return highlight(code, lexer, formatter)


markdown_formatter = mistune.Markdown(renderer=HighlightRenderer())

search_c1 = True
search_c2 = True
search_c3 = True

session_criteria = None
session_responds = None
session_run_time = None
session_total = None

form_es = None
form_tab = None

from interface import routes
