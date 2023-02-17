from configparser import ConfigParser
from typing import Any, Mapping

TWEET_SCHEMA = "/net/data/twitter-covid/tweet_schema.json"
VOTER_FILE_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/lab-lazer/TSmart-cleaner-Oct2017-rawFormat.csv"
)
PANEL_TWEETS_LOC = (
    "hdfs://megatron.ccs.neu.edu/user/nir/panel_tweets/created_at_bucket=2022-06-16"
)


def config(section: str, filename: str='config.ini') -> Mapping[str, Any]:
    """Get a config file section as kwargs"""
    parser = ConfigParser()
    parser.read(filename)

    if parser.has_section(section):
        args = dict([(i[0], i[1]) for i in parser.items(section)])
    else:
        raise Exception(
            f"Config file '{filename}' missing section '{section}'")

    return args


ES_PASSWORD = config('elasticsearch')['password']
ES_URL = "https://elastic:{}@localhost:9200".format(ES_PASSWORD)

VALID_AGG_TERMS = {"day", "week", "month"}
AGG_TO_ROUND_KEY = {"day": "D", "week": "W", "month": "M"}

CSV_DATA_LOC = "/home/asmithh/god_all_tweets.tsv"
CSV_TEXT_COL = "text"

DEMOGRAPHIC_FIELDS = ["tsmart_state", "vb_age_decade", "voterbase_gender", "voterbase_race"]

USER_COUNT_PRIVACY_THRESHOLD = 10
