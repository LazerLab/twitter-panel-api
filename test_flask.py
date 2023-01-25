import requests

z = requests.post(
    "http://localhost:5010/keyword_search",
    json={"keyword_query": "dog", "aggregate_time_period": "day"},
)
print(z.json())
