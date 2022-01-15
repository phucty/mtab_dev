from api.resources.m_mapping_id_v2 import MappingID


if __name__ == "__main__":
    # 1. Extract redirects, redirect of and mapping between Wikipedia, Wikidata, DBpedia
    items = MappingID(read_only=False)
    items.build()
