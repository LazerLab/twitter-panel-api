import copy
from datetime import datetime

import pandas as pd
import pytest

from panel_api import api_utils
from panel_api.api_utils import KeywordQuery
from panel_api.config import Demographic

from .utils import list_equals_ignore_order, period_equals


def test_parse_query_valid():
    valid_inputs = [
        {"keyword_query": "keyword", "aggregate_time_period": "day"},
        {
            "keyword_query": "keyword",
            "aggregate_time_period": "week",
        },
        {
            "keyword_query": "key word",
            "aggregate_time_period": "day",
            "cross_sections": [
                "voterbase_race",
                "tsmart_state",
            ],
        },
        {
            "keyword_query": "keyword",
            "aggregate_time_period": "day",
            "after": "2019-01-01",
        },
        {
            "keyword_query": "keyword",
            "aggregate_time_period": "day",
            "before": "2019-01-01",
        },
        {
            "keyword_query": "keyword",
            "aggregate_time_period": "day",
            "after": "2019-01-01",
            "before": "2020-01-01",
        },
    ]

    parsed_queries = [KeywordQuery.from_raw_query(input) for input in valid_inputs]
    expected_queries = [
        KeywordQuery("keyword", "day"),
        KeywordQuery("keyword", "week"),
        KeywordQuery(
            "key word",
            "day",
            cross_sections=[
                Demographic.RACE,
                Demographic.STATE,
            ],
        ),
    ]

    for expected, actual in zip(expected_queries, parsed_queries):
        assert expected == actual


def test_parse_query_invalid():
    invalid_inputs = [
        {"keyword_query": None, "aggregate_time_period": "day"},  # Missing search query
        {
            "keyword_query": "keyword",
            "aggregate_time_period": "dy",
        },  # Invalid time aggregation
        {
            "keyword_query": "keyword",
            "aggregate_time_period": None,
        },  # Missing time aggregation
        {
            "keyword_query": "key word",
            "time_agg": "week",
            "cross_sections": ["age", "v_gender"],
        },  # Invalid demographic
        {
            "keyword_query": "keyword",
            "time_agg": "day",
            "after": "2020-10-10",
            "before": "2020-10-01",
        },  # Invalid time range
    ]

    for input in invalid_inputs:
        assert KeywordQuery.from_raw_query(input) is None


def test_keyword_search_output_valid(valid_outputs):
    privacy_threshold = 10

    for output in valid_outputs:
        assert api_utils.validate_keyword_search_output(output, privacy_threshold)


def test_keyword_search_output_invalid(invalid_outputs):
    privacy_threshold = 10

    for output in invalid_outputs:
        assert not api_utils.validate_keyword_search_output(output, privacy_threshold)


def test_keyword_search_censor_output(valid_outputs, invalid_outputs):
    privacy_threshold = 10

    for output in valid_outputs:
        assert output == api_utils.censor_keyword_search_output(
            copy.deepcopy(output), 10, remove_censored_values=True
        )
        assert output == api_utils.censor_keyword_search_output(
            copy.deepcopy(output), 10, remove_censored_values=False
        )

    for output in invalid_outputs:
        validated_output_removed = api_utils.censor_keyword_search_output(
            copy.deepcopy(output), privacy_threshold, remove_censored_values=True
        )
        validated_output_replaced = api_utils.censor_keyword_search_output(
            copy.deepcopy(output), privacy_threshold, remove_censored_values=False
        )
        assert api_utils.validate_keyword_search_output(
            validated_output_removed, privacy_threshold
        )
        assert api_utils.validate_keyword_search_output(
            validated_output_replaced, privacy_threshold
        )


def test_fill_zeros():
    sparse_results = [
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
    ]
    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": (
                pd.Series([1, 1, 1, 1, 1], index=["AL", "GA", "PA", "MA", "CT"])
                .reindex(Demographic.STATE.values(), fill_value=0)
                .to_dict()
            ),
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": (
                pd.Series([2, 1, 2], index=["20 - 30", "30 - 40", "50 - 60"])
                .reindex(Demographic.AGE.values(), fill_value=0)
                .to_dict()
            ),
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
                "African-American": 0,
                "Hispanic": 0,
                "Other": 0,
                "Native American": 0,
            },
            "groups": pd.DataFrame(
                [
                    ("Caucasian", "Male", 2),
                    ("Asian", "Female", 1),
                    ("Uncoded", "Female", 1),
                    ("Caucasian", "Unknown", 1),
                    ("Caucasian", "Female", 0),
                    ("African-American", "Female", 0),
                    ("African-American", "Male", 0),
                    ("African-American", "Unknown", 0),
                    ("Hispanic", "Female", 0),
                    ("Hispanic", "Male", 0),
                    ("Hispanic", "Unknown", 0),
                    ("Uncoded", "Male", 0),
                    ("Uncoded", "Unknown", 0),
                    ("Asian", "Male", 0),
                    ("Asian", "Unknown", 0),
                    ("Other", "Female", 0),
                    ("Other", "Male", 0),
                    ("Other", "Unknown", 0),
                    ("Native American", "Female", 0),
                    ("Native American", "Male", 0),
                    ("Native American", "Unknown", 0),
                ],
                columns=[Demographic.RACE, Demographic.GENDER, "count"],
            ).to_dict("records"),
        },
    ]
    filled_results = api_utils.fill_zeros(sparse_results)

    assert list_equals_ignore_order(expected_results, filled_results, period_equals)


@pytest.fixture
def valid_outputs():
    return [
        [
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
            }
        ],
        [
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "10 - 20", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "10 - 20", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "CA", "count": 40},
                ],
            }
        ],
    ]


@pytest.fixture
def invalid_outputs():
    return [
        [  # Race demographic under threshold
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 9,
                    "Hispanic": 11,
                },
            }
        ],
        [  # Age/state cross-section under threshold
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "10 - 20", "tsmart_state": "AL", "count": 9},
                    {"vb_age_decade": "10 - 20", "tsmart_state": "CA", "count": 41},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "AL", "count": 11},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "CA", "count": 39},
                ],
            }
        ],
        [  # Age/state cross-section under threshold, groups OK
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 11,
                    "Hispanic": 9,
                },
                "groups": [
                    {"vb_age_decade": "10 - 20", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "10 - 20", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "CA", "count": 40},
                ],
            }
        ],
        [  # Second period invalid (no groups)
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
            },
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 9,
                    "Hispanic": 11,
                },
            },
        ],
        [  # Second period invalid (with groups)
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "10 - 20", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "10 - 20", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "CA", "count": 40},
                ],
            },
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"10 - 20": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "10 - 20", "tsmart_state": "AL", "count": 9},
                    {"vb_age_decade": "10 - 20", "tsmart_state": "CA", "count": 41},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "AL", "count": 11},
                    {"vb_age_decade": "80 - 90", "tsmart_state": "CA", "count": 39},
                ],
            },
        ],
    ]
