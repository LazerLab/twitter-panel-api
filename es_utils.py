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

from .config import ES_PASSWORD


def elastic_query_for_keyword(keyword: str):
    """
    Given a string (keyword), return all tweets in the tweets index that contain that string.
    Return as raw ES output.
    """
    es = Elasticsearch(
        "https://elastic:{}@localhost:9200".format(ES_PASSWORD),
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    s = Search(using=es).query("match", text=keyword)
    res = s.execute()
    return res


def elastic_query_users(users: list[str]):
    """
    Given a list of users (as user Twitter profile IDs),
    pull all users from ES that have an ID in the list.
    Return as raw ES output.
    """
    es = Elasticsearch(
        "https://elastic:{}@localhost:9200".format(ES_PASSWORD),
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    s = Search(using=es, index="users").query(
        "terms", twProfileID=[str(u) for u in users]
    )
    res = s.execute()
    res2 = []
    for hit in res:
        res2.append(hit.to_dict())
    return res2
