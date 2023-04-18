"""
Main Flask application endpoints file. Creates the Flask app on import.
"""
from flask import Blueprint, current_app, request

from panel_api.query.keyword_query import KeywordQuery

public_api = Blueprint("public_api", __name__)


@public_api.route("/keyword_search", methods=["GET", "POST"])
def keyword_search():
    """
    basic endpoint for querying the ES-indexed portion of the twitter panel and their
    tweets
    """
    message = "unknown error"
    request_json = request.get_json()
    query = KeywordQuery.from_raw_query(
        request_json, max_cross_sections=current_app.config.get("MAX_CROSS_SECTIONS")
    )
    if query is not None:
        results = (
            query.execute().censor(current_app.config["MIN_DISPLAYED_USERS"]).to_list()
        )
        return {"query": request_json, "response_data": results}

    message = "invalid query"
    return {
        "query": request_json,
        "response_data": message,
    }
