import requests
import json
import m_config as cf
from api.utilities import m_io as iw
from collections import Counter


class MDuck(object):
    def __init__(self):
        self.url = "http://0.0.0.0:8001/parse"
        self.dim = cf.NUM_DUCK

    def is_number(self, text, alpha=0.5):
        is_num = False
        num_len = 0
        for r in self._parse(text):
            num_len += len(r["body"])
        if num_len > alpha * len(text):
            is_num = True
        return is_num

    def is_text(self, text, alpha=0.5):
        return not self.is_number(text, alpha)

    def get_most_type(self, text, lang="en_GB"):
        duck_type = "text"
        try:
            num_len = 0
            num_types = Counter()
            for r in self._parse(text, lang):
                num_len += len(r["body"])
                num_types[r["dim"]] += 1
            if num_len > 0.5 * len(text):
                duck_type = num_types.most_common(1)[0][0]
        except Exception as message:
            iw.print_status(message, is_screen=False)
        return duck_type

    def _parse(self, text, lang="en_GB"):
        if len(text):
            data = {'text': text, 'locale': lang}
            response = requests.post(self.url, data=data)
            if len(response.text[1:-1]) > 0:
                return json.loads(response.text)
        return []


if __name__ == "__main__":
    mduck = MDuck()
    print(mduck.get_most_type("1989 12 01"))
    print(mduck.get_most_type("01 12 1929"))
    print(mduck.get_most_type("ogre battle: the march of the black queen (900 wii points)"))
    print(mduck.get_most_type('1 The Secret of the Old Clock'))
    print(mduck.is_number('1 The Secret of the Old Clock'))
    v1 = mduck._parse("1101")
    v2 = mduck._parse("1101-1-1")
    v3 = mduck._parse("42€")
    v4 = mduck._parse('1 The Secret of the Old Clock')
    print(mduck.get_most_type('This is a sentence'))
    print(mduck.get_most_type('4111-1111-1111-1111'))
    print(mduck.get_most_type('6 miles'))
    print(mduck.get_most_type('3 mins'))
    print(mduck.get_most_type('duckling-team@fb.com'))
    print(mduck.get_most_type('eighty eight'))
    print(mduck.get_most_type('33rd'))
    print(mduck.get_most_type('+1 (650) 123-4567'))
    print(mduck.get_most_type('3 cups of sugar'))
    print(mduck.get_most_type('80F'))
    print(mduck.get_most_type('today at 9am'))
    print(mduck.get_most_type('42€ = 12$'))
    print(mduck.get_most_type('6 miles in 7 mins'))
    print(mduck.get_most_type('https://api.wit.ai/message?q=hi'))
    print(mduck.get_most_type('4 gallons'))