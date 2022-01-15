"""
Define the API functions
"""
from time import time

from fastapi import status, Query
from fastapi.responses import JSONResponse
from api import m_f
from api.lookup import m_entity_search

from api_f import app, api_info
from api_f.f_models import *


@app.get("/", tags=["introduction"])
async def root():
    return api_info


@app.get(
    "/entity_info/{wikidata_id}",
    tags=["get_entity_info"],
    response_model=ItemInfo,
    responses={
        404: {"description": "The item was not found"},
        200: {"description": "Item requested by Wikidata ID, example: Q1490"},
    },
    response_model_exclude_none=True,
)
async def get_entity_info(
    wikidata_id: str = Query("Q1490", description="wikidata_id"),
    wikipedia_title: Optional[bool] = Query(
        None, description="Wikipedia title. The default value is `True`."
    ),
    dbpedia_title: Optional[bool] = Query(
        None, description="DBpedia title. The default value is `True`."
    ),
    label: Optional[bool] = Query(
        None, description="Entity label. The default value is `True`."
    ),
    description: Optional[bool] = Query(
        None, description="Entity description. The default value is `True`."
    ),
    aliases: Optional[bool] = Query(
        None, description="Entity aliases in English. The default value is False."
    ),
    aliases_multilingual: Optional[bool] = Query(
        None, description="Entity aliases in multilingual. The default value is False."
    ),
    instance_of: Optional[bool] = Query(
        None, description="Entity direct types. The default value is `True`."
    ),
    all_types: Optional[bool] = Query(
        None,
        description="Entity all types including direct and transitive types. The default value is False.",
    ),
    entity_facts: Optional[bool] = Query(
        None,
        description="Entity facts: [**this entity**, **property**, **other entities**]. The default value is False.",
    ),
    literal_facts: Optional[bool] = Query(
        None,
        description="Literal facts: [**this entity**, **property**, **literal** (string, time, quantity, media, links)]. The default value is False.",
    ),
    pagerank: Optional[bool] = Query(
        None,
        description="Entity pagerank score, used to estimate entity popularity. The default value is `True`.",
    ),
    dbpedia_types: Optional[bool] = Query(
        None,
        description="Entity types mapping from DBpedia ontology. The default value is `False`.",
    ),
    entity_facts_others: Optional[bool] = Query(
        None,
        description="Other entity facts: [**this entity**, **property** (Wikipedia Section title, attribute name in Wikipedia infobox, DBpedia property names), **other entities**]. The default value is `False`.",
    ),
    literal_facts_others: Optional[bool] = Query(
        None,
        description="Other literal facts: [**this entities**, **property** (attribute name in Wikipedia infobox, DBpedia property names), **literal** (string, time, quantity, media, links)]. The default value is `False`.",
    ),
    inv_entity_facts: Optional[bool] = Query(
        None,
        description="Inverse of entities facts: [**other entities**, **property**, **this entities**]. **Warning:** Please use it when you actually need it, since it could return a large respond for high popularity entities, for example Q30 [United States of America]. The default value is `False`.",
    ),
    inv_entity_facts_others: Optional[bool] = Query(
        None,
        description="Inverse of other entities facts: [**other entities**, **property** (Wikipedia Section title, attribute name in Wikipedia infobox, DBpedia property names), **this entities**]. **Warning:** Please use it when you actually need it, since it could return a large respond for high popularity entities, for example Q30 [United States of America]. The default value is `False`.",
    ),
):
    kwargs = {"wikidata_id": wikidata_id}
    if wikipedia_title is not None:
        kwargs["wikipedia_title"] = wikipedia_title
    if dbpedia_title is not None:
        kwargs["dbpedia_title"] = dbpedia_title
    if label is not None:
        kwargs["label"] = label
    if description is not None:
        kwargs["description"] = description
    if aliases is not None:
        kwargs["aliases"] = aliases
    if aliases_multilingual is not None:
        kwargs["aliases_multilingual"] = aliases_multilingual
    if instance_of is not None:
        kwargs["instance_of"] = instance_of
    if all_types is not None:
        kwargs["all_types"] = all_types
    if entity_facts is not None:
        kwargs["entity_facts"] = entity_facts
    if literal_facts is not None:
        kwargs["literal_facts"] = literal_facts
    if pagerank is not None:
        kwargs["pagerank"] = pagerank
    if dbpedia_types is not None:
        kwargs["dbpedia_types"] = dbpedia_types
    if entity_facts_others is not None:
        kwargs["entity_facts_others"] = entity_facts_others
    if literal_facts_others is not None:
        kwargs["literal_facts_others"] = literal_facts_others
    if inv_entity_facts is not None:
        kwargs["inv_entity_facts"] = inv_entity_facts
    if inv_entity_facts_others is not None:
        kwargs["inv_entity_facts_others"] = inv_entity_facts_others

    item = m_f.m_items().get_item(**kwargs)

    if item:
        return ItemInfo(**item)
    else:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND)


@app.get(
    "/entity_search/",
    tags=["entity_search"],
    response_model=SearchOutput,
    response_model_exclude_none=True,
    responses={
        400: {
            "description": "Bad request. Please enter at least one of q (query), attr (attribute) parameter."
        },
    },
)
async def search_entity(
    q: Optional[str] = Query(None, description="Enter a search query."),
    attr: Optional[str] = Query(
        None,
        description="""
Search entity by its attribute. Given a triple of subject, predicate, and object. Find subject given predicate and object `p:PREDICATE_LABEL_q:OBJECT_LABEL` or just object `q:OBJECT_LABEL`.  

For example: 
* Search entities that instance of human: `p:instance_of_q:human` or `q:human`  
* Search entities that are human and was born in Japan: `p:instance_of_q:human p:was_born_q:Japan` or `p:P31_q:human p:was_born_q:Q17`
    """,
    ),
    lang: Optional[SearchInputLang] = Query(
        None,
        description="""
Search entity with its label in a language (select one of [`en`(English), `all` (Multilingual)]). The default value is `en`. """,
    ),
    mode: Optional[SearchInputMode] = Query(
        None,
        description="""
Entity search module (select one of three value [`b`, `f`, `a`]). The default value is `a`.
* `b`: keywords search with BM25 (hyper-kwargs: b=0.75, k1=1.2).
* `f`: fuzzy search with an edit-distance (Damerau–Levenshtein distance).
* `a`: the weighted aggregation of keyword search and fuzzy search. This model yields slightly better performance (1-3 % accuracy improvement) than fuzzy search. The current weight is equal between fuzzy and keyword search""",
    ),
    limit: Optional[int] = Query(
        None,
        ge=0,
        description="""Search limit: maximum number of relevant entities to be returned. The value should be from 0 to 1000. The default value is 20.""",
    ),
    expensive: Optional[bool] = Query(
        None,
        description="""
Fuzzy search parameter: Select one of two value [`True`, `False`]. The default value is `False`.
* `True`: efficiency mode. Perform early stopping in the fuzzy search.
* `False`: Brute-force search. This mode could slightly improve search performance (improve 1-2% accuracy), but it might take a long time to get answers (about ten times longer than the efficiency mode).
""",
    ),
    fuzzy: Optional[bool] = None,
    get_info: Optional[bool] = Query(
        None,
        description="""
Select one of two value [`True`, `False`]. The default value is `False`.
* `False`: do not return entity labels, description, mapping URLs of DBpedia and Wikipedia.
* `True`: return entity labels, description, mapping URLs of DBpedia and Wikipedia.
""",
    ),
):
    kwargs = {"query": q}
    if attr is not None:
        kwargs["attr"] = attr
    if lang is not None:
        kwargs["lang"] = lang
    if mode is not None:
        kwargs["mode"] = mode
    if limit is not None:
        kwargs["limit"] = limit
    if expensive is not None:
        kwargs["expensive"] = expensive
    if fuzzy is not None:
        kwargs["fuzzy"] = fuzzy
    if get_info is not None:
        kwargs["get_info"] = get_info

    if not kwargs.get("query") and not kwargs.get("attr"):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "message": "Please enter at least one of q (query), attr (attribute) parameter."
            },
        )

    start = time()

    responds, _, _ = m_entity_search.search(**kwargs)
    is_get_info = True if kwargs.get(get_info) is None else kwargs.get(get_info)
    responds = m_f.m_items().get_search_info(responds, get_info=is_get_info)
    is_aggregation = (
        SearchInputMode.aggregation if kwargs.get(mode) is None else kwargs.get(mode)
    )
    if is_aggregation and is_aggregation == SearchInputMode.aggregation:
        for i in range(len(responds)):
            responds[i]["score"] = responds[i]["score"]
    run_time = time() - start

    output = {
        "run_time": run_time,
        "total": len(responds) if responds else 0,
        "hits": responds if responds else [],
    }

    return SearchOutput(**output)


@app.get("/mtab/", tags=["table_annotation"])
async def table_annotation():
    return {"Todo": "To do work"}
