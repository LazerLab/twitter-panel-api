from flask import Flask, request

from .config import Config
from .api_utils import (
    censor_keyword_search_output,
    KeywordQuery,
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
    query = KeywordQuery.from_raw_query(request_json)
    if query is not None:
        results = ElasticsearchTwitterPanelSource().query_from_api(
            query, fill_zeros=True
        )
        return {
            "query": request_json,
            "response_data": censor_keyword_search_output(
                results, remove_censored_values=False
            ),
        }
    else:
        message = "invalid query"

    return {
        "query": request_json,
        "response_data": message,
    }


# @app.route("/csv_keyword_search", methods=["GET", "POST"])
# def csv_keyword_search():
#     search_query = request.get_json()["keyword_query"]
#     agg_by = request.get_json()["aggregate_time_period"]
#     if validate_keyword_search_input(search_query, agg_by):
#         results = CSVSource("").query_from_api(search_query=search_query, agg_by=agg_by)
#         return {
#             "query": search_query,
#             "agg_time_period": agg_by,
#             "response_data": results,
#         }
#     else:
#         message = "invalid query"
#         return {
#             "query": search_query,
#             "agg_time_period": agg_by,
#             "response_data": message,
#         }
