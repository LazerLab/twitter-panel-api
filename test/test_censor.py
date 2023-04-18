from test.utils import list_equals_ignore_order, period_equals
from typing import Any, Hashable, Iterable, Mapping, Optional

import pandas as pd
import pytest

from panel_api.aggregation.user_demographics import TimeSlicedUserDemographicAggregation
from panel_api.api_values import Demographic, TimeAggregation


class ManualTimeSlicedUserDemographicAggregation(TimeSlicedUserDemographicAggregation):
    """
    Aggregation that allows manually specifying internal tables for testing.
    """

    def __init__(
        self,
        counts_table: pd.DataFrame,
        demographic_distributions: dict[Demographic, pd.Series],
        time_aggregation: TimeAggregation,
        cross_sections: Optional[list[Demographic]] = None,
        cross_sections_table: Optional[pd.DataFrame] = None,
        time_slice_column: str = "ts",
    ):
        self.manual_counts = counts_table
        self.manual_demographic_distributions = demographic_distributions
        self.manual_cross_sections_table = cross_sections_table

        empty_tweet_data = pd.DataFrame(columns=["created_at", "userid"])
        empty_voter_data = pd.DataFrame(columns=["userid", *Demographic])

        super().__init__(
            empty_tweet_data,
            empty_voter_data,
            time_aggregation=time_aggregation,
            cross_sections=cross_sections,
            time_slice_column=time_slice_column,
        )

    def _get_counts(self, _: pd.DataFrame) -> pd.DataFrame:
        return self.manual_counts

    def _user_demographic_counts(self, _: pd.DataFrame) -> dict[Demographic, pd.Series]:
        return self.manual_demographic_distributions

    def _user_cross_sections(self, _: pd.DataFrame) -> pd.DataFrame:
        if self.manual_cross_sections_table is None:
            return pd.DataFrame()
        return self.manual_cross_sections_table

    @staticmethod
    def from_list(list_data, time_slice_column: str, time_aggregation: TimeAggregation):
        data = pd.DataFrame(list_data)
        counts_table = data[[time_slice_column, "n_tweets", "n_tweeters"]].set_index(
            time_slice_column
        )

        demographic_distributions = {}
        for dem in Demographic:
            dem_tables = []
            for slice_record in data[[time_slice_column, dem]].to_dict("records"):
                dem_table = pd.Series(slice_record[dem])
                dem_table.name = "count"
                dem_table.index.name = dem
                dem_table = (
                    dem_table.to_frame()
                    .assign(**{time_slice_column: slice_record[time_slice_column]})
                    .reset_index()
                    .set_index([time_slice_column, dem])["count"]
                )
                dem_tables.append(dem_table)
            demographic_distributions[dem] = pd.concat(dem_tables)

        if "groups" in data:
            group_tables = [
                pd.DataFrame(row["groups"]).assign(
                    **{time_slice_column: row[time_slice_column]}
                )
                for _, row in data.iterrows()
            ]
            cross_sections_table = pd.concat(group_tables)
            cross_sections = list(
                c
                for c in cross_sections_table.columns
                if c != time_slice_column and c != "count"
            )
            cross_sections_table = cross_sections_table.set_index(
                [time_slice_column, *cross_sections]
            )
        else:
            cross_sections_table = None
            cross_sections = None

        return ManualTimeSlicedUserDemographicAggregation(
            counts_table=counts_table,
            demographic_distributions=demographic_distributions,
            time_aggregation=time_aggregation,
            time_slice_column=time_slice_column,
            cross_sections=cross_sections,
            cross_sections_table=cross_sections_table,
        )


def validate_keyword_search_output(
    response_data: Iterable[Mapping[Hashable, Any]], privacy_threshold: int
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


def timeslice_list_equals(expected, actual):
    return list_equals_ignore_order(expected, actual, item_equality=period_equals)


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
        assert timeslice_list_equals(
            output,
            ManualTimeSlicedUserDemographicAggregation.from_list(
                output, time_slice_column="ts", time_aggregation=TimeAggregation.DAY
            )
            .censor(privacy_threshold)
            .to_list(),
        )

    for output in invalid_outputs:
        validated_output = (
            ManualTimeSlicedUserDemographicAggregation.from_list(
                output, time_slice_column="ts", time_aggregation=TimeAggregation.DAY
            )
            .censor(privacy_threshold)
            .to_list()
        )
        assert validate_keyword_search_output(validated_output, privacy_threshold)


@pytest.fixture
def valid_outputs():
    return [
        [
            {
                "ts": "2022-10-20",
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
                "ts": "2022-10-21",
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
                "ts": "2022-10-20",
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
                "ts": "2022-10-21",
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
                "ts": "2022-10-22",
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
                "ts": "2022-10-20",
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
                "ts": "2022-10-21",
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
                "ts": "2022-10-20",
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
                "ts": "2022-10-21",
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
