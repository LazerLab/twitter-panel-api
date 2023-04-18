import pandas as pd
import pytest

from panel_api.api_values import Demographic


@pytest.fixture
def tweet_data():
    return pd.DataFrame(
        [
            {"created_at": "2023-02-17", "userid": "0"},
            {"created_at": "2023-02-17", "userid": "1"},
            {"created_at": "2023-02-19", "userid": "2"},
            {"created_at": "2023-02-19", "userid": "3"},
            {"created_at": "2023-02-19", "userid": "9"},
            {"created_at": "2023-02-20", "userid": "4"},
            {"created_at": "2023-02-21", "userid": "5"},
            {"created_at": "2023-02-21", "userid": "6"},
            {"created_at": "2023-02-21", "userid": "7"},
            {"created_at": "2023-02-21", "userid": "8"},
            {"created_at": "2023-02-21", "userid": "9"},
            {"created_at": "2023-02-21", "userid": "9"},
            {"created_at": "2023-02-22", "userid": "9"},
        ]
    )


@pytest.fixture
def voter_data():
    return pd.DataFrame(
        [
            ("0", "AL", "Male", "under 30", "Caucasian"),
            ("1", "GA", "Female", "under 30", "Uncoded"),
            ("2", "PA", "Male", "30 - 40", "Caucasian"),
            ("3", "MA", "Female", "50 - 60", "Asian"),
            ("4", "MA", "Male", "70+", "African-American"),
            ("5", "IA", "Female", "70+", "Hispanic"),
            ("6", "IL", "Male", "40 - 50", "Caucasian"),
            ("7", "CO", "Female", "30 - 40", "Native American"),
            ("8", "KS", "Male", "40 - 50", "Uncoded"),
            ("9", "CT", "Unknown", "50 - 60", "Caucasian"),
        ],
        columns=[
            "userid",
            Demographic.STATE,
            Demographic.GENDER,
            Demographic.AGE,
            Demographic.RACE,
        ],
    )


@pytest.fixture
def tweet_voter_data(tweet_data, voter_data):
    tweets = tweet_data.set_index("userid")
    voters = voter_data.set_index("userid")
    voter_tweets = tweets.merge(voters, left_index=True, right_index=True).reset_index()
    return voter_tweets
