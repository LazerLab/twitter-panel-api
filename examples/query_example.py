import requests

z = requests.get(
    "http://localhost:5010/keyword_search",
    json={
        "keyword_query": "optimus prime",
        "aggregate_time_period": "day",
        "cross_sections": ["voterbase_race", "voterbase_gender"],
    },
)
print(z.json())
