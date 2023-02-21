import panel_api.api_utils as api_utils
import pytest
from unittest.mock import patch


def test_keyword_search_input_valid():
    valid_inputs = [
        {"search_query": "keyword", "time_agg": "day"},
        {"search_query": "keyword", "time_agg": "week", "group_by": None},
        {
            "search_query": "key word",
            "time_agg": "day",
            "group_by": [
                "voterbase_race",
                "voterbase_gender",
                "tsmart_state",
                "vb_age_decade",
            ],
        },
    ]

    for input in valid_inputs:
        assert api_utils.validate_keyword_search_input(**input)


def test_keyword_search_input_invalid():
    invalid_inputs = [
        {"search_query": None, "time_agg": "day"},  # Missing search query
        {"search_query": "keyword", "time_agg": "dy"},  # Invalid time aggregation
        {"search_query": "keyword", "time_agg": None},  # Missing time aggregation
        {
            "search_query": "key word",
            "time_agg": "week",
            "group_by": ["v_age", "v_gender"],
        },  # Invalid demographics
    ]

    for input in invalid_inputs:
        assert not api_utils.validate_keyword_search_input(**input)


def test_keyword_search_output_valid():
    privacy_threshold = 10
    valid_outputs = [
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

    for output in valid_outputs:
        assert api_utils.validate_keyword_search_output(output, privacy_threshold)


def test_keyword_search_output_invalid():
    privacy_threshold = 10
    invalid_outputs = [
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

    for output in invalid_outputs:
        assert not api_utils.validate_keyword_search_output(output, privacy_threshold)
