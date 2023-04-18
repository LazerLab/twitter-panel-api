"""
Module defining information source types.
"""
from enum import Enum


class SourceType(str, Enum):
    """
    Types of data sources that have unique methods of collecting data from them.
    """

    ELASTICSEARCH = "elasticsearch"  # Elasticsearch cluster
    DATABASE = "database"  # SQL databases (just postgres for now, will add more later)
    ATTACHED = "attached"  # Data attached alongside, in the config, for testing
