import copy
from typing import Any, Iterable, Mapping

import pytest

from panel_api.api_values import Demographic
from panel_api.sources import CensoredSource


def validate_keyword_search_output(
    response_data: Iterable[Mapping[str, Any]], privacy_threshold: int
) -> bool:
    """
    Check whether output respects a certain privacy threshold.
    """
    for period in response_data:
        if "groups" in period:
            if any(
                (
                    group["count"] < privacy_threshold
                    for group in period["groups"]
                    if group["count"] is not None
                )
            ):
                return False
        if any(
            (
                any(
                    count < privacy_threshold
                    for count in period[dem].values()
                    if count is not None
                )
                for dem in [*Demographic]
            )
        ):
            return False
    return True


def test_keyword_search_output_valid(valid_outputs):
    privacy_threshold = 10

    for output in valid_outputs:
        assert validate_keyword_search_output(output, privacy_threshold)


def test_keyword_search_output_invalid(invalid_outputs):
    privacy_threshold = 10

    for output in invalid_outputs:
        assert not validate_keyword_search_output(output, privacy_threshold)


def test_keyword_search_censor_output(valid_outputs, invalid_outputs):
    privacy_threshold = 10

    for output in valid_outputs:
        assert output == CensoredSource.censor_keyword_search_output(
            copy.deepcopy(output), 10, remove_censored_values=True
        )
        assert output == CensoredSource.censor_keyword_search_output(
            copy.deepcopy(output), 10, remove_censored_values=False
        )

    for output in invalid_outputs:
        validated_output_removed = CensoredSource.censor_keyword_search_output(
            copy.deepcopy(output), privacy_threshold, remove_censored_values=True
        )
        validated_output_replaced = CensoredSource.censor_keyword_search_output(
            copy.deepcopy(output), privacy_threshold, remove_censored_values=False
        )
        assert validate_keyword_search_output(
            validated_output_removed, privacy_threshold
        )
        assert validate_keyword_search_output(
            validated_output_replaced, privacy_threshold
        )


@pytest.fixture
def valid_outputs():
    return [
        [
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"under 30": 50, "70+": 50},
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
                "vb_age_decade": {"under 30": 50, "80 - 90": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "under 30", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "under 30", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "70+", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "70+", "tsmart_state": "CA", "count": 40},
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
                "vb_age_decade": {"under 30": 50, "70+": 50},
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
                "vb_age_decade": {"under 30": 50, "70+": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "under 30", "tsmart_state": "AL", "count": 9},
                    {"vb_age_decade": "under 30", "tsmart_state": "CA", "count": 41},
                    {"vb_age_decade": "70+", "tsmart_state": "AL", "count": 11},
                    {"vb_age_decade": "70+", "tsmart_state": "CA", "count": 39},
                ],
            }
        ],
        [  # Age/state cross-section under threshold, groups OK
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"under 30": 50, "70+": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 11,
                    "Hispanic": 9,
                },
                "groups": [
                    {"vb_age_decade": "under 30", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "under 30", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "70+", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "70+", "tsmart_state": "CA", "count": 40},
                ],
            }
        ],
        [  # Second period invalid (no groups)
            {
                "n_tweets": 1000,
                "n_tweeters": 100,
                "vb_age_decade": {"under 30": 50, "70+": 50},
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
                "vb_age_decade": {"under 30": 50, "70+": 50},
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
                "vb_age_decade": {"under 30": 50, "70+": 50},
                "tsmart_state": {"AL": 20, "CA": 80},
                "voterbase_gender": {"Male": 40, "Female": 50, "Unknown": 10},
                "voterbase_race": {
                    "Caucasian": 80,
                    "African-American": 10,
                    "Hispanic": 10,
                },
                "groups": [
                    {"vb_age_decade": "under 30", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "under 30", "tsmart_state": "CA", "count": 40},
                    {"vb_age_decade": "70+", "tsmart_state": "AL", "count": 10},
                    {"vb_age_decade": "70+", "tsmart_state": "CA", "count": 40},
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
                    {"vb_age_decade": "under 30", "tsmart_state": "AL", "count": 9},
                    {"vb_age_decade": "under 30", "tsmart_state": "CA", "count": 41},
                    {"vb_age_decade": "70+", "tsmart_state": "AL", "count": 11},
                    {"vb_age_decade": "70+", "tsmart_state": "CA", "count": 39},
                ],
            },
        ],
    ]
