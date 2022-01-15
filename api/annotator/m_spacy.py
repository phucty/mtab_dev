from collections import Counter
import m_config as cf
import spacy
import api.utilities.m_utils as ul
from api.utilities.parse_number import representsFloat


class MSpacy(object):
    def __init__(self):
        self.model_names = {
            "en": "en_core_web_sm",  # sm, md, lg, trf
            "all": "xx_sent_ud_sm",  # python -m spacy download xx_sent_ud_sm
        }
        self.models = {
            "en": None,
            "all": None,
        }
        self.load_model(lang="en")
        self.load_model(lang="all")
        self.dbpedia_mapping = {
            "PERSON": ["Person"],
            "NORP": ["Country", "Religious", "PoliticalParty"],
            "FAC": ["PopulatedPlace", "Building", "RouteOfTransportation", "Airport"],
            "ORG": ["Organization"],
            "LOC": ["PopulatedPlace"],
            "GPE": ["PopulatedPlace"],
            "PRODUCT": ["Device", "Food"],
            "EVENT": ["Event"],
            "WORK_OF_ART": ["Work"],
            "LAW": ["LawFirm"],
            "LANGUAGE": ["Language"],
        }
        self.dims_text = {
            "PERSON",
            "NORP",
            "FAC",
            "ORG",
            "LOC",
            "GPE",
            "PRODUCT",
            "EVENT",
            "WORK_OF_ART",
            "LAW",
            "LANGUAGE",
        }
        self.dims_num = {
            "CARDINAL",
            "PERCENT",
            "MONEY",
            "ORDINAL",
            "QUANTITY",
            "TIME",
            "DATE",
        }

    def load_model(self, lang):
        """Load SpaCy model with the language of [lang]

        Args:
            lang (string): language id
        """
        if lang != "en":
            lang = "all"
        print("Loaded: Type entity model (%s)" % self.model_names[lang])
        self.models[lang] = spacy.load(self.model_names[lang])

    def _parse(self, text, lang="en"):
        """[summary]

        Args:
            text ([type]): [description]
            lang (str, optional): [description]. Defaults to "en".

        Returns:
            [type]: [description]
        """

        spacy_model = self.models.get(lang, None)
        if not spacy_model:
            self.load_model(lang)
            spacy_model = self.models[lang]
        response = spacy_model(text)
        return response

    def get_majority_type(self, text, lang="en"):
        """[summary]

        Args:
            text ([type]): [description]
            lang (str, optional): [description]. Defaults to "en".

        Returns:
            [type]: [description]
        """
        types = cf.DataType.TEXT
        text = str(text)
        if text:
            # count_numbers = sum(c.isdigit() for c in text)
            # if count_numbers > len(text):
            response = self._parse(text, lang)
            if len(response.ents):
                num_len = 0
                num_types = Counter()
                text_types = Counter()
                for r in response.ents:
                    if r.label_ in self.dims_num:
                        num_len += len(r.text)
                        num_types[r.label_] += 1
                    else:
                        text_types[r.label_] += 1
                if num_len > 0.5 * len(text):
                    types = num_types.most_common(1)[0][0]
                elif len(text_types) > 0:
                    types = text_types.most_common(1)[0][0]
        return types

    def get_type(self, text, lang="en"):
        if not text or text.lower() in cf.NONE_CELLS:
            return cf.DataType.NONE

        if text[0] == "〒" or text[:2] == "代表":
            return cf.DataType.NUM

        txt_tmp = text.replace("%", "").replace(" ", "")
        if ul.convert_num(txt_tmp) is not None:
            return cf.DataType.NUM

        if ul.is_date(text):
            return cf.DataType.NUM

        main_type = self.get_majority_type(text, lang)

        if main_type in cf.NUM_SPACY:
            return cf.DataType.NUM

        return cf.DataType.TEXT

    def is_number(self, text, lang="en"):
        is_num = False
        if len(text):
            response = self._parse(text, lang)
            if len(response.ents):
                num_len = 0
                for r in response.ents:
                    if r.label_ in self.dims_num:
                        num_len += len(r.text)
                if num_len > 0.5 * len(text):
                    is_num = True
        return is_num

    def is_text(self, text, lang="en"):
        return not self.is_number(text, lang)

    def get_entity_text(self, text, lang="en"):
        results = []
        if len(text):
            response = self._parse(text, lang)
            if len(response.ents):
                for r in response.ents:
                    if r.label_ not in self.dims_num:
                        results.append((r.text, r.label_))
        return results

    # def get_dbpedia_mapping_types(self, text, lang="en"):
    #     dbpedia_types = set()
    #     for entity, entity_type in self.get_entity_text(text, lang):
    #         for db_type in self.dbpedia_mapping.get(entity_type, []):
    #             dbpedia_types.add(db_type)
    #             all_parents = f.dbpedia_owl().get_parents(db_type)
    #             dbpedia_types.update(set(all_parents))
    #     return dbpedia_types


if __name__ == "__main__":
    import os

    os.environ["KMP_DUPLICATE_LIB_OK"] = "True"
    obj_spacy = MSpacy()
    print(obj_spacy.get_type("1 The Secret of the Old Clock"))
    # print(obj_spacy.get_dbpedia_mapping_types(
    #     "the house of the spirits, isabel allende"))
    print(
        obj_spacy.get_entity_text(
            "ogre battle: the march of the black queen (900 wii points)"
        )
    )
    print(obj_spacy.get_majority_type("1 The Secret of the Old Clock"))
    print(obj_spacy.get_majority_type("This is a sentence"))
    print(obj_spacy.get_majority_type("4111-1111-1111-1111"))
    print(obj_spacy.get_majority_type("6 miles"))
    # print(obj_spacy.get_types('6 Meilen', "de"))
    print(obj_spacy.get_majority_type("6 Meilen"))
    print(obj_spacy.get_majority_type("3 mins"))
    print(obj_spacy.get_majority_type("duckling-team@fb.com"))
    print(obj_spacy.get_majority_type("eighty eight"))
    print(obj_spacy.get_majority_type("33rd"))
    print(obj_spacy.get_majority_type("+1 (650) 123-4567"))
    print(obj_spacy.get_majority_type("3 cups of sugar"))
    print(obj_spacy.get_majority_type("80F"))
    print(obj_spacy.get_majority_type("today at 9am"))
    print(obj_spacy.get_majority_type("42€ = 12$"))
    print(obj_spacy.get_majority_type("6 miles in 7 mins"))
    print(obj_spacy.get_majority_type("https://api.wit.ai/message?q=hi"))
    print(obj_spacy.get_majority_type("4 gallons"))
