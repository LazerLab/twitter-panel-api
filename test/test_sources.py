from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from panel_api.api_utils import KeywordQuery
from panel_api.api_values import Demographic, TimeAggregation
from panel_api.sources import CompositeSource, MediaSource

from .fixtures.data import tweet_data, tweet_voter_data, voter_data  # noqa: F401
from .utils import list_equals_ignore_order, period_equals


@pytest.fixture
def mock_es_search():
    with patch("panel_api.sources.elastic_query_for_keyword") as m:
        yield m


def mock_voter_db(voter_data):
    with patch("panel_api.sources.collect_voters") as m:
        m.return_value = pd.DataFrame(voter_data)
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
    mock_demographic_source.get_demographics.return_value = pd.DataFrame(voter_data)
    results = CompositeSource(
        mock_twitter_source, mock_demographic_source
    ).query_from_api(KeywordQuery("dinosaur", time_aggregation=TimeAggregation.DAY))
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
            "vb_age_decade": {"under 30": 2},
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
            "vb_age_decade": {"70+": 1},
            "voterbase_race": {"African-American": 1},
        },
        {
            "ts": datetime(2023, 2, 21),
            "n_tweets": 6,
            "n_tweeters": 5,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"30 - 40": 1, "40 - 50": 2, "50 - 60": 1, "70+": 1},
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
    mock_demographic_source.get_demographics.return_value = pd.DataFrame(voter_data)
    results = CompositeSource(
        mock_twitter_source, mock_demographic_source
    ).query_from_api(KeywordQuery("dinosaur", time_aggregation=TimeAggregation.WEEK))
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
            "vb_age_decade": {"under 30": 2, "30 - 40": 1, "50 - 60": 2},
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
                "70+": 2,
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


def test_table_group_by(tweet_voter_data):
    results = MediaSource.aggregate_tabular_data(
        tweet_voter_data,
        "created_at",
        time_agg=TimeAggregation.WEEK,
        group_by=[Demographic.RACE, Demographic.GENDER],
    )

    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"under 30": 2, "30 - 40": 1, "50 - 60": 2},
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
                "70+": 2,
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
