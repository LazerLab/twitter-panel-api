"""
Module for managing database connections for the application.
"""
import psycopg2
from elasticsearch import Elasticsearch
from psycopg2 import extensions

from .config import get_config_value


def elasticsearch_connection(**kwargs) -> Elasticsearch:
    """Provide a connection to the Elasticsearch database."""
    if not kwargs:
        kwargs = get_config_value("elasticsearch")
    es_handle = Elasticsearch(**kwargs)
    return es_handle


def postgresql_connection(**kwargs) -> extensions.connection:
    """Provide a connection to the PostgreSQL database."""
    if not kwargs:
        kwargs = get_config_value("postgresql")
    conn: extensions.connection = psycopg2.connect(**kwargs)
    return conn
