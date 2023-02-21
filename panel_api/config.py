from configparser import ConfigParser
from typing import Any, Mapping

TWEET_SCHEMA = "/net/data/twitter-covid/tweet_schema.json"
VOTER_FILE_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/lab-lazer/TSmart-cleaner-Oct2017-rawFormat.csv"
)
PANEL_TWEETS_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/nir/panel_tweets/created_at_bucket=2022-06-16"
)

FLASK = {"SECRET_KEY": "flask_secret_key"}

ELASTICSEARCH = {
    "host": "achtung16",
    "port": "9200",
    "basic_auth": ("elastic", "password"),
}

POSTGRESQL = {
    "host": "achtung16",
    "port": "5200",
    "database": "voters",
    "user": "postgres",
}

VALID_AGG_TERMS = {"day", "week", "month"}
AGG_TO_ROUND_KEY = {"day": "D", "week": "W", "month": "M"}

CSV_DATA_LOC = "/home/asmithh/god_all_tweets.tsv"
CSV_TEXT_COL = "text"

DEMOGRAPHIC_FIELDS = [
    "tsmart_state",
    "vb_age_decade",
    "voterbase_gender",
    "voterbase_race",
]

USER_COUNT_PRIVACY_THRESHOLD = 10
