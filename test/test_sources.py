from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from panel_api.api_utils import KeywordQuery
from panel_api.api_values import Demographic
from panel_api.sources import CompositeSource, MediaSource

from .utils import list_equals_ignore_order, period_equals


@pytest.fixture
def raw_tweet_data():
    """
    Data entries have been truncated for brevity and my sanity. If more of the actual
    structure becomes relevent, it should be added.
    """
    data = [
        {"created_at": "2023-02-17", "user": {"id": "0"}},
        {"created_at": "2023-02-17", "user": {"id": "1"}},
        {"created_at": "2023-02-19", "user": {"id": "2"}},
        {"created_at": "2023-02-19", "user": {"id": "3"}},
        {"created_at": "2023-02-19", "user": {"id": "9"}},
        {"created_at": "2023-02-20", "user": {"id": "4"}},
        {"created_at": "2023-02-21", "user": {"id": "5"}},
        {"created_at": "2023-02-21", "user": {"id": "6"}},
        {"created_at": "2023-02-21", "user": {"id": "7"}},
        {"created_at": "2023-02-21", "user": {"id": "8"}},
        {"created_at": "2023-02-21", "user": {"id": "9"}},
        {"created_at": "2023-02-21", "user": {"id": "9"}},
        {"created_at": "2023-02-22", "user": {"id": "9"}},
    ]
    hits_mock = []
    for entry in data:
        m = MagicMock()
        m.to_dict = MagicMock(return_value=entry)
        hits_mock.append(m)
    return hits_mock


@pytest.fixture
def tweet_data(raw_tweet_data):
    hits = [hit.to_dict() for hit in raw_tweet_data]
    print("HITS:", hits)
    return pd.DataFrame(
        [{"created_at": hit["created_at"], "userid": hit["user"]["id"]} for hit in hits]
    )


@pytest.fixture
def voter_data():
    return pd.DataFrame(
        [
            ("0", "AL", "Male", 20, "Caucasian"),
            ("1", "GA", "Female", 21, "Uncoded"),
            ("2", "PA", "Male", 30, "Caucasian"),
            ("3", "MA", "Female", 55, "Asian"),
            ("4", "MA", "Male", 72, "African-American"),
            ("5", "IA", "Female", 147, "Hispanic"),
            ("6", "IL", "Male", 47, "Caucasian"),
            ("7", "CO", "Female", 32, "Native American"),
            ("8", "KS", "Male", 43, "Uncoded"),
            ("9", "CT", "Unknown", 58, "Caucasian"),
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
def voter_tweets():
    return pd.DataFrame(
        [
            ("2023-02-17", "0", "0", "AL", "Male", 20, "Caucasian"),
            ("2023-02-17", "1", "1", "GA", "Female", 21, "Uncoded"),
            ("2023-02-19", "2", "2", "PA", "Male", 30, "Caucasian"),
            ("2023-02-19", "3", "3", "MA", "Female", 55, "Asian"),
            ("2023-02-19", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-20", "4", "4", "MA", "Male", 72, "African-American"),
            ("2023-02-21", "5", "5", "IA", "Female", 147, "Hispanic"),
            ("2023-02-21", "6", "6", "IL", "Male", 47, "Caucasian"),
            ("2023-02-21", "7", "7", "CO", "Female", 32, "Native American"),
            ("2023-02-21", "8", "8", "KS", "Male", 43, "Uncoded"),
            ("2023-02-21", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-21", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-22", "9", "9", "CT", "Unknown", 58, "Caucasian"),
        ],
        columns=[
            "created_at",
            "userid",
            "twProfileID",
            Demographic.STATE,
            Demographic.GENDER,
            Demographic.AGE,
            Demographic.RACE,
        ],
    )


@pytest.fixture
def mock_es_search():
    with patch("panel_api.sources.elastic_query_for_keyword") as m:
        yield m


@pytest.fixture
def mock_voter_db(voter_data):
    with patch("panel_api.sources.collect_voters") as m:
        m.return_value = voter_data
        yield m


@pytest.fixture
def mock_twitter_source():
    return MagicMock()


@pytest.fixture
def mock_demographic_source():
    return MagicMock()


def test_query_daily(
    tweet_data,
    voter_data,
    mock_twitter_source,
    mock_demographic_source,
):
    mock_twitter_source.match_keyword.return_value = tweet_data
    mock_demographic_source.get_demographics.return_value = voter_data
    results = CompositeSource(
        mock_twitter_source, mock_demographic_source
    ).query_from_api(KeywordQuery("dinosaur", time_aggregation="day"))
    mock_twitter_source.match_keyword.assert_called_once_with(
        keyword="dinosaur", time_range=(None, None)
    )

    expected_results = [
        {
            "ts": datetime(2023, 2, 17),
            "n_tweets": 2,
            "n_tweeters": 2,
            "tsmart_state": {"AL": 1, "GA": 1},
            "voterbase_gender": {"Male": 1, "Female": 1},
            "vb_age_decade": {"20 - 30": 2},
            "voterbase_race": {"Caucasian": 1, "Uncoded": 1},
        },
        {
            "ts": datetime(2023, 2, 19),
            "n_tweets": 3,
            "n_tweeters": 3,
            "tsmart_state": {"PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 1, "Female": 1, "Unknown": 1},
            "vb_age_decade": {"30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {"Caucasian": 2, "Asian": 1},
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 1,
            "n_tweeters": 1,
            "tsmart_state": {"MA": 1},
            "voterbase_gender": {"Male": 1},
            "vb_age_decade": {"70 - 80": 1},
            "voterbase_race": {"African-American": 1},
        },
        {
            "ts": datetime(2023, 2, 21),
            "n_tweets": 6,
            "n_tweeters": 5,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"30 - 40": 1, "40 - 50": 2, "50 - 60": 1, "140 - 150": 1},
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "Uncoded": 1,
            },
        },
        {
            "ts": datetime(2023, 2, 22),
            "n_tweets": 1,
            "n_tweeters": 1,
            "tsmart_state": {"CT": 1},
            "voterbase_gender": {"Unknown": 1},
            "vb_age_decade": {"50 - 60": 1},
            "voterbase_race": {"Caucasian": 1},
        },
    ]

    print(results)
    assert list_equals_ignore_order(expected_results, results, period_equals)


def test_query_weekly(
    tweet_data,
    voter_data,
    mock_twitter_source,
    mock_demographic_source,
):
    mock_twitter_source.match_keyword.return_value = tweet_data
    mock_demographic_source.get_demographics.return_value = voter_data
    results = CompositeSource(
        mock_twitter_source, mock_demographic_source
    ).query_from_api(KeywordQuery("dinosaur", time_aggregation="week"))
    mock_twitter_source.match_keyword.assert_called_once_with(
        keyword="dinosaur", time_range=(None, None)
    )

    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"20 - 30": 2, "30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
            },
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 8,
            "n_tweeters": 6,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1, "MA": 1},
            "voterbase_gender": {"Male": 3, "Female": 2, "Unknown": 1},
            "vb_age_decade": {
                "30 - 40": 1,
                "40 - 50": 2,
                "50 - 60": 1,
                "70 - 80": 1,
                "140 - 150": 1,
            },
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "African-American": 1,
                "Uncoded": 1,
            },
        },
    ]

    assert list_equals_ignore_order(expected_results, results, period_equals)


def test_table_group_by(voter_tweets):
    results = MediaSource.aggregate_tabular_data(
        voter_tweets,
        "created_at",
        time_agg="week",
        group_by=["voterbase_race", "voterbase_gender"],
    )

    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"20 - 30": 2, "30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
            },
            "groups": [
                {"voterbase_race": "Caucasian", "voterbase_gender": "Male", "count": 2},
                {"voterbase_race": "Asian", "voterbase_gender": "Female", "count": 1},
                {"voterbase_race": "Uncoded", "voterbase_gender": "Female", "count": 1},
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Unknown",
                    "count": 1,
                },
            ],
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 8,
            "n_tweeters": 6,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1, "MA": 1},
            "voterbase_gender": {"Male": 3, "Female": 2, "Unknown": 1},
            "vb_age_decade": {
                "30 - 40": 1,
                "40 - 50": 2,
                "50 - 60": 1,
                "70 - 80": 1,
                "140 - 150": 1,
            },
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "African-American": 1,
                "Uncoded": 1,
            },
            "groups": [
                {"voterbase_race": "Caucasian", "voterbase_gender": "Male", "count": 1},
                {
                    "voterbase_race": "African-American",
                    "voterbase_gender": "Male",
                    "count": 1,
                },
                {"voterbase_race": "Uncoded", "voterbase_gender": "Male", "count": 1},
                {
                    "voterbase_race": "Hispanic",
                    "voterbase_gender": "Female",
                    "count": 1,
                },
                {
                    "voterbase_race": "Native American",
                    "voterbase_gender": "Female",
                    "count": 1,
                },
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Unknown",
                    "count": 1,
                },
            ],
        },
    ]

    assert list_equals_ignore_order(expected_results, results, period_equals)
