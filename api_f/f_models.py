"""
Define the input and output schema for FastAPI
"""

from enum import Enum
from typing import List, Optional, Dict

from pydantic import BaseModel


# EntitySearch InputModel Language
class SearchInputLang(str, Enum):
    english = "en"
    multilingual = "all"


# EntitySearch InputModel SearchModel
class SearchInputMode(str, Enum):
    aggregation = "a"
    keywords = "b"
    fuzzy = "f"


# EntitySearch OutputModel EntityInfoReturn
class SearchOutputEntity(BaseModel):
    id: str
    score: float
    label: Optional[str]
    description: Optional[str]
    wikidata: Optional[str]
    wikipedia: Optional[str]
    dbpedia: Optional[str]


# EntitySearch OutputModel EntitySearchRespond
class SearchOutput(BaseModel):
    run_time: float
    total: int
    hits: List[SearchOutputEntity]


class ItemInfo(BaseModel):
    wikipedia_title: Optional[str]
    dbpedia_title: Optional[str]
    label: Optional[str]
    description: Optional[str]
    aliases: Optional[List[str]]
    aliases_multilingual: Optional[List[str]]
    instance_of: Optional[List[str]]
    all_types: Optional[List[str]]
    dbpedia_types: Optional[List[str]]
    # types: Optional[List[str]]
    entity_facts: Optional[Dict]
    literal_facts: Optional[Dict]
    entity_facts_others: Optional[Dict]
    literal_facts_others: Optional[Dict]
    pagerank: Optional[float]
    inv_entity_facts: Optional[Dict]
    inv_entity_facts_others: Optional[Dict]
