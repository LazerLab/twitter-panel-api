TWEET_SCHEMA = "/net/data/twitter-covid/tweet_schema.json"
VOTER_FILE_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/lab-lazer/TSmart-cleaner-Oct2017-rawFormat.csv"
)
PANEL_TWEETS_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/nir/panel_tweets/created_at_bucket=2022-06-16"
)

lines = []
with open("es_password.txt", "r") as f:
    for line in f.readlines():
        lines.append(line.strip())

ES_PASSWORD = lines[0]
ES_URL = "https://elastic:{}@localhost:9200".format(ES_PASSWORD)

VALID_AGG_TERMS = {"day", "week", "month"}
AGG_TO_ROUND_KEY = {"day": "d", "week": "w", "month": "m"}
