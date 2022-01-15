import f
from utilities import io_worker as iw
from googleapiclient.discovery import build
import setting as st
from collections import defaultdict


class Google(object):
    def __init__(self):
        self.DEV_KEY = "YOUR KEY :) I do not forget to hide this"
        self.APP_KEY = "YOUR KEY :) I do not forget to hide this"
        self.engine = build("customsearch", "v1", developerKey=self.DEV_KEY)
        self.count = 0
        self.DIR_LOCAL = "%s/temp/Round%d/gg_spell.pkl" % (st.DIR_ROOT, st.ROUND)
        try:
            self.local_data = iw.load_obj_pkl(self.DIR_LOCAL)
        except Exception as message:
            iw.print_status(message)
            self.local_data = defaultdict()

    def save(self):
        iw.save_obj_pkl(self.DIR_LOCAL, self.local_data)

    def do_you_mean(self, text, num=10, lang=None):
        corrected_text = self.local_data.get(text)
        if not corrected_text:
            self.count += 1
            corrected_text = text
            try:
                if lang:
                    responds = (
                        self.engine.cse()
                        .list(q=text, cx=self.APP_KEY, num=num, lr="lang_%s" % lang)
                        .execute()
                    )
                else:
                    responds = (
                        self.engine.cse()
                        .list(q=text, cx=self.APP_KEY, num=num)
                        .execute()
                    )
                if responds.get("spelling", None):
                    corrected_text = responds["spelling"]["correctedQuery"]
            except Exception as message:
                iw.print_status(message)
                if "Daily Limit Exceeded" in message:
                    raise SystemExit()
        return corrected_text


import httplib
import xml.dom.minidom

data = """
<spellrequest textalreadyclipped="0" ignoredups="0" ignoredigits="1" ignoreallcaps="1">
<text> %s </text>
</spellrequest>
"""


def spellCheck(word_to_spell):

    con = httplib.HTTPSConnection("www.google.com")
    con.request("POST", "/tbproxy/spell?lang=en", data % word_to_spell)
    response = con.getresponse()

    dom = xml.dom.minidom.parseString(response.read())
    dom_data = dom.getElementsByTagName("spellresult")[0]

    if dom_data.childNodes:
        for child_node in dom_data.childNodes:
            result = child_node.firstChild.data.split()
        for word in result:
            if word_to_spell.upper() == word.upper():
                return True
        return False
    else:
        return True


if __name__ == "__main__":
    f.init()
    from autocorrect import Speller

    spell = Speller()
    tmp = spell("this is a vanicualtion")

    gg = Google()
    # tmp = gg.do_you_mean('this is a vanicualtion')
    # print(f.google_search().do_you_mean('this is a vanicualtion'))
    # print(f.google_search().get('this is a vanicualtion'))
