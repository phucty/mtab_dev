from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from wtforms import (
    StringField,
    SubmitField,
    BooleanField,
    SelectField,
    IntegerField,
    validators,
    TextAreaField,
)
from wtforms.validators import Length

import m_config as cf


class FormSearch(FlaskForm):
    query = StringField(
        "Query", validators=[Length(min=1, max=512)], default="2MASS J10540655-0031018"
    )
    limit = IntegerField(
        "Limit", [validators.NumberRange(min=1, max=100)], default=20
    )  # ,
    efficient = BooleanField("Efficient", default="checked")
    language = SelectField(
        "Language", choices=[("en", "English"), ("all", "Multilingual")], default="en"
    )
    mode = SelectField(
        "Language",
        choices=[("a", "Aggregation"), ("b", "BM25"), ("f", "Fuzzy")],
        default="a",
    )
    # submit_value = Markup('<span class="fa fa-search" title="Submit"></span>')
    search = SubmitField("Search")


class FormTabAnnotation(FlaskForm):
    table_file_upload = FileField(
        "Upload table", validators=[FileAllowed(["zip"] + list(cf.TABLE_EXTENSIONS))]
    )
    table_text_content = TextAreaField(
        "Paste table data",
        default="""col0,col1,col2,col3
2MASS J10540655-0031018,-5.7,19.3716366,13.635635128508735
2MASS J0464841+0715177,-2.7747499999999996,26.671235999999997,11.818755055646479
2MAS J08351104+2006371,72.216,3.7242887999999996,128.15196099865955
2MASS J08330994+186328,-6.993,6.0962562,127.64996294136303
""",
    )
    annotation1 = SubmitField("Annotate")
    annotation2 = SubmitField("Annotate")
