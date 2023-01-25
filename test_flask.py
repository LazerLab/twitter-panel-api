import requests

z = requests.post(
    "http://localhost:5010/keyword_search",
    json={"keyword_query": "dog", "aggregate_time_period": "day"},
)
print(z.json())
z = requests.post(
    "http://localhost:5010/csv_keyword_search",
    json={"keyword_query": "trump", "aggregate_time_period": "week"},
)
print(z.json())
