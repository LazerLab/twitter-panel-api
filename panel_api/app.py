from flask import Flask, request

from .config import Config
from .api_utils import (
    validate_keyword_search_input,
    censor_keyword_search_output,
)
from .sources import CSVSource, ElasticsearchTwitterPanelSource

app = Flask(__name__)
app.config.update(Config()["flask"])


@app.route("/keyword_search", methods=["GET", "POST"])
def keyword_search():
    """
    basic endpoint for querying the ES-indexed portion of the twitter panel and their tweets
    """
    message = "unknown error"
    request_json = request.get_json()
    search_query = request_json.get("keyword_query")
    agg_by = request_json.get("aggregate_time_period")
    group_by = request_json.get("cross_sections")
    if validate_keyword_search_input(search_query, agg_by, group_by):
        results = ElasticsearchTwitterPanelSource().query_from_api(
            search_query=search_query, agg_by=agg_by, group_by=group_by, fill_zeros=True
        )
        return {
            "query": search_query,
            "agg_time_period": agg_by,
            "response_data": censor_keyword_search_output(results),
        }
    else:
        message = "invalid query"

    return {
        "query": search_query,
        "agg_time_period": agg_by,
        "response_data": message,
    }


@app.route("/csv_keyword_search", methods=["GET", "POST"])
def csv_keyword_search():
    search_query = request.get_json()["keyword_query"]
    agg_by = request.get_json()["aggregate_time_period"]
    if validate_keyword_search_input(search_query, agg_by):
        results = CSVSource("").query_from_api(search_query=search_query, agg_by=agg_by)
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
