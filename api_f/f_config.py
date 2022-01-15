"""
Descriptions of API, and functions
"""

func_specs = [
    {
        "name": "get_entity_info",
        "description": "Retrieve entity including of labels, descriptions, aliases, entity and literal facts.",
    },
    {
        "name": "entity_search",
        "description": "MTabES: Search relevant entities from Wikidata, Wikipedia, and DBpedia",
        "externalDocs": {
            "description": "Entity search external docs",
            "url": "https://mtab.app/mtabes/docs",
        },
    },
    {
        "name": "table_annotation",
        "description": "MTab: Tabular Data Annotation",
        "externalDocs": {
            "description": "Table annotation external docs",
            "url": "https://mtab.app/mtab/docs",
        },
    },
]

api_info = {
    "title": "MTab",
    "description": "MTab: Entity Search and Tabular Annotation with Knowledge Graphs (Wikidata, Wikipedia, DBpedia)",
    "version": "1.1",
    "contact": {"name": "Phuc Nguyen", "email": "phucnt@nii.ac.jp"},
    "license_info": {
        "name": "MIT License",
        "url": "https://github.com/phucty/mtab_tool/blob/master/LICENSE",
    },
    "documents": "http://localhost:8000/docs",
    "github": "https://github.com/phucty/mtab_tool",
}

api_funcs_info = {**api_info, "openapi_tags": func_specs}
