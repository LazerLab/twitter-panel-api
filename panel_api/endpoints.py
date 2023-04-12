"""
Main Flask application endpoints file. Creates the Flask app on import.
"""
from flask import Blueprint, request

from .api_utils import KeywordQuery
from .config import get_config_value
from .sources import (
    CensoredSource,
    CompositeSource,
    ElasticsearchTwitterPanelSource,
    PostgresDemographicSource,
)

public_api = Blueprint("public_api", __name__)


@public_api.route("/keyword_search", methods=["GET", "POST"])
def keyword_search():
    """
    basic endpoint for querying the ES-indexed portion of the twitter panel and their
    tweets
    """
    message = "unknown error"
    request_json = request.get_json()
    query = KeywordQuery.from_raw_query(request_json)
    if query is not None:
        source = CensoredSource(
            CompositeSource(
                ElasticsearchTwitterPanelSource(), PostgresDemographicSource()
            ),
            privacy_threshold=get_config_value("user_count_privacy_threshold"),
        )
        results = source.query_from_api(query, fill_zeros=True)
        return {"query": request_json, "response_data": results}

    message = "invalid query"
    return {
        "query": request_json,
        "response_data": message,
    }
