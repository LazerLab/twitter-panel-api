"""
Module for interacting with an Elasticsearch backend.
"""
from datetime import date
from typing import Optional

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Match, Range

from .config import Config


def elastic_query_for_keyword(
    keyword: str, before: Optional[date] = None, after: Optional[date] = None
):
    """
    Given a string (keyword), return all tweets in the tweets index that contain
    that string.

    Return as raw ES output.
    """
    es_conf = Config()["elasticsearch"]
    es_handle = Elasticsearch(
        **es_conf,
        scheme="https",
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    search = Search(using=es_handle, index="tweets").query(Match(full_text=keyword))
    range_query = {}
    range_query.update(
        {"lte": before.strftime("%Y-%m-%d")} if before is not None else {}
    )
    range_query.update({"gte": after.strftime("%Y-%m-%d")} if after is not None else {})
    if len(range_query > 0):
        search = search.query(Range(created_at=range_query))
    res = search.scan()
    return res


def elastic_query_users(users: list[str]):
    """
    Given a list of users (as user Twitter profile IDs),
    pull all users from ES that have an ID in the list.
    Return as raw ES output.
    """
    es_handle = Elasticsearch(
        **Config()["elasticsearch"],
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    search = Search(using=es_handle, index="voters").query(
        "terms", twProfileID=[str(u) for u in users]
    )
    res = [hit.to_dict() for hit in search.scan()]
    return res
