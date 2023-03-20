import elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch import Elasticsearch

from elasticsearch_dsl import (
    analyzer,
    Search,
    tokenizer,
    Date,
    Document,
    Text,
    Integer,
    Boolean,
)
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.query import Match, Range

from typing import Optional, Tuple

from .config import Config


def elastic_query_for_keyword(keyword: str, between: Optional[Tuple[str, str]] = None):
    """
    Given a string (keyword), return all tweets in the tweets index that contain that string.
    Return as raw ES output.
    """
    es_conf = Config()["elasticsearch"]
    es = Elasticsearch(
        **es_conf,
        scheme="https",
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    s = Search(using=es, index="tweets").query(Match(full_text=keyword))
    if between is not None:
        s = s.query(Range(created_at={"gte": between[0], "lte": between[1]}))
    res = s.scan()
    return res


def elastic_query_users(users: list[str]):
    """
    Given a list of users (as user Twitter profile IDs),
    pull all users from ES that have an ID in the list.
    Return as raw ES output.
    """
    es = Elasticsearch(
        **Config()["elasticsearch"],
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    s = Search(using=es, index="voters").query(
        "terms", twProfileID=[str(u) for u in users]
    )
    res = [hit.to_dict() for hit in s.scan()]
    return res
