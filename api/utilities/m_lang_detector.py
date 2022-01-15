import fasttext
import m_config as cf
from slugify import slugify


class LangDetector(object):
    def __init__(self, model_dir=cf.DIR_PRE_LANG):
        self._model = fasttext.load_model(model_dir)

    def predict(self, text):
        y = self._model.predict(text)
        if y:
            y = y[0][0].replace("__label__", "")
        return y

    def is_english(self, text):
        if self.predict(text) == "en":
            return True
        return False

    def get_text_en_n_multilingual(self, strings):
        strings_en = set()
        string_multilingual = set()
        for s in strings:
            if self.is_english(s):
                strings_en.add(s)
            else:
                s_en = slugify(s, lowercase=False, separator=" ")
                strings_en.add(s_en)
                if s_en != s:
                    string_multilingual.add(s)
        return list(strings_en), list(string_multilingual)


if __name__ == "__main__":
    lang_detector = LangDetector()
    print(lang_detector.predict("Anna Maria Luisa de' Medici"))
    print(lang_detector.predict("Friedrich von Öttingen"))
    print(lang_detector.predict("oganiation"))
    print(lang_detector.predict("oganiation"))
    print(lang_detector.predict("Mateřská škola"))
    print(lang_detector.predict("제주 유나이티드"))
    print(lang_detector.predict("বিবেকানন্দ যুবভারতী ক্রীড়াঙ্গন"))
    print(lang_detector.predict("Αθλητική Ένωσις Κωνσταντινουπόλεως"))
    print(lang_detector.predict("Футбольный клуб Краснодар"))
    print(lang_detector.predict("استقلال"))
    print(lang_detector.predict("অ্যাটলেটিকো ডি কলকাতা"))
    print(lang_detector.predict("نادي مولودية وهران"))
    print(lang_detector.predict("오연교"))
    print(lang_detector.predict("김용용대"))
