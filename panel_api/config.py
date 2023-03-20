from typing import Any, Mapping
import os
import json
from enum import Enum

_default_config = {
    "tweet_schema": "/net/data/twitter-covid/tweet_schema.json",
    "voter_file_loc": "hdfs://megatron.ccs.neu.edu/user/lab-lazer/TSmart-cleaner-Oct2017-rawFormat.csv",
    "panel_tweets_loc": "hdfs://megatron.ccs.neu.edu/user/nir/panel_tweets/created_at_bucket=2022-06-16",
    "csv_data_loc": "/home/asmithh/god_all_tweets.tsv",
    "csv_text_col": "text",
    "user_count_privacy_threshold": 10,
    "cross_sections_limit": 2,
    "flask": {"SECRET_KEY": "flask_secret_key"},
    "elasticsearch": {
        "hosts": [{"host": "localhost", "port": 9200}],
        "http_auth": ("elastic", "password"),
    },
    "postgresql": {
        "host": "localhost",
        "port": "5200",
        "database": "voters",
        "user": "postgres",
    },
}


class Config(dict):
    def __init__(self, *, path=None, env=True, fallback=True):
        if fallback:
            self.update(_default_config)
        if env:
            env_config = os.environ.get("CONFIG")
            if env_config and os.path.exists(env_config):
                self.update(Config.parse_file(env_config))
        if path and os.path.exists(path):
            self.update(Config.parse_file(path))

    def parse_file(path: str) -> Mapping[str, Any]:
        return json.load(open(path))


VALID_AGG_TERMS = {"day", "week", "month"}
AGG_TO_ROUND_KEY = {"day": "D", "week": "W", "month": "M"}


class Demographic(str, Enum):
    STATE = "tsmart_state"
    AGE = "vb_age_decade"
    GENDER = "voterbase_gender"
    RACE = "voterbase_race"

    def __str__(self):
        return self.value

    def values(self):
        return DEMOGRAPHIC_VALUES[self]


DEMOGRAPHIC_VALUES = {
    Demographic.RACE: [
        "Caucasian",
        "African-American",
        "Hispanic",
        "Uncoded",
        "Asian",
        "Other",
        "Native American",
    ],
    Demographic.AGE: [
        "10 - 20",
        "20 - 30",
        "30 - 40",
        "40 - 50",
        "50 - 60",
        "60 - 70",
        "70 - 80",
        "80 - 90",
        "90 - 100",
        "100 - 110",
        "110 - 120",
        "120 - 130",
        "130 - 140",
        "140 - 150",
    ],
    Demographic.GENDER: ["Female", "Male", "Unknown"],
    Demographic.STATE: [
        "CA",
        "TX",
        "NY",
        "FL",
        "OH",
        "IL",
        "PA",
        "MI",
        "GA",
        "NC",
        "WA",
        "MA",
        "MN",
        "NJ",
        "IN",
        "VA",
        "CO",
        "WI",
        "TN",
        "AZ",
        "MO",
        "OR",
        "MD",
        "IA",
        "KY",
        "LA",
        "AL",
        "SC",
        "OK",
        "KS",
        "CT",
        "NV",
        "NE",
        "AR",
        "UT",
        "MS",
        "DC",
        "WV",
        "ME",
        "NM",
        "NH",
        "RI",
        "ID",
        "HI",
        "SD",
        "MT",
        "ND",
        "DE",
        "AK",
        "VT",
        "WY",
    ],
}
