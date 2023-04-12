"""
Module for managing database connections for the application.
"""
import psycopg2
from elasticsearch import Elasticsearch
from flask import current_app
from psycopg2 import extensions


def elasticsearch_connection() -> Elasticsearch:
    """Provide a connection to the Elasticsearch cluster."""
    host = current_app.config.get("ELASTICSEARCH_URL")
    es_handle = Elasticsearch([host])
    return es_handle


def postgresql_connection() -> extensions.connection:
    """Provide a connection to the PostgreSQL database."""
    url = current_app.config.get("DATABASE_URL")
    conn: extensions.connection = psycopg2.connect(url)
    return conn
