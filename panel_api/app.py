"""
Main Flask application endpoints file. Creates the Flask app on import.
"""
from flask import Flask, request

from .api_utils import KeywordQuery
from .config import Config
from .sources import CensoredSource, ElasticsearchTwitterPanelSource

app = Flask(__name__)
app.config.update(Config()["flask"])


@app.route("/keyword_search", methods=["GET", "POST"])
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
            ElasticsearchTwitterPanelSource(),
            privacy_threshold=Config()["user_count_privacy_threshold"],
        )
        results = source.query_from_api(query, fill_zeros=True)
        return {"query": request_json, "response_data": results}

    message = "invalid query"
    return {
        "query": request_json,
        "response_data": message,
    }
