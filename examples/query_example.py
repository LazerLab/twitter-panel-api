import requests

z = requests.get(
    "http://localhost:5010/keyword_search",
    json={"keyword_query": "optimus prime", "aggregate_time_period": "day", "cross_sections": ["voterbase_race", "voterbase_gender"]},
)
print(z.json())
#z = requests.post(
#    "http://localhost:5010/csv_keyword_search",
#    json={"keyword_query": "trump", "aggregate_time_period": "week"},
#)
#print(z.json())
