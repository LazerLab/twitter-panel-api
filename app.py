from flask import Flask, jsonify, request
import pandas as pd

from .config import ES_URL, VALID_AGG_TERMS, AGG_TO_ROUND_KEY
from .es_utils import elastic_query_for_keyword, elastic_query_users
from .api_utils import int_or_nan, validate_keyword_search_input
from .sources import CSVSource, ElasticsearchTwitterPanelSource

app = Flask(__name__)
lines = []
with open("flask_secret_key.txt", "r") as f:
    for line in f.readlines():
        lines.append(line.strip())

app.config["SECRET_KEY"] = lines[0]


@app.route("/keyword_search", methods=["GET", "POST"])
def keyword_search():
    """
    basic endpoint for querying the ES-indexed portion of the twitter panel and their tweets
    """
    search_query = request.get_json()["keyword_query"]
    agg_by = request.get_json()["aggregate_time_period"]
    if validate_keyword_search_input(search_query, agg_by):
        results = ElasticsearchTwitterPanelSource(ES_URL).query_from_api(
            search_query=search_query, agg_by=agg_by
        )
        return {
            "query": search_query,
            "agg_time_period": agg_by,
            "response_data": results,
        }
    else:
        message = "invalid query"
        return {
            "query": search_query,
            "agg_time_period": agg_by,
            "response_data": message,
        }

@app.route('/csv_keyword_search', methods=["GET", "POST"])
def csv_keyword_search():
    search_query = request.get_json()["keyword_query"]
    agg_by = request.get_json()["aggregate_time_period"]
    if validate_keyword_search_input(search_query, agg_by):
        results = CSVSource('').query_from_api(
            search_query=search_query, agg_by=agg_by
        )
        return {
            "query": search_query,
            "agg_time_period": agg_by,
            "response_data": results,
        }
    else:
        message = "invalid query"
        return {
            "query": search_query,
            "agg_time_period": agg_by,
            "response_data": message,
        }
