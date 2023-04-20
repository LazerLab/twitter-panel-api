"""
Modulde defining an aggregation of just user demographics.
"""
from __future__ import annotations

import itertools
from typing import Any, Hashable, Optional, Tuple

import pandas as pd

from panel_api.api_utils import censor_table
from panel_api.api_values import Demographic, TimeAggregation


class TimeSlicedUserDemographicAggregation:
    """
    Data aggregation that provides a time-sliced aggregation of user demographics.
    These times are the times a user posted, and each user will only be counted once
    per time slice.
    """

    def __init__(
        self,
        user_post_times: pd.DataFrame,
        user_demographics: pd.DataFrame,
        time_aggregation: TimeAggregation,
        cross_sections: Optional[list[Demographic]] = None,
        time_slice_column: str = "ts",
    ):
        """
        Create this data aggregation.

        Parameters:
        user_post_times (DataFrame): When users posted tweets. This DataFrame
            must contain columns ["created_at", "userid"]
        user_demographics (DataFrame): Demographic information of the users. Must
            contain columns for each Demographic and "userid"
        time_aggregation (TimeAggregation): size of time slices
        cross_sections (list[Demographic]): Optional. Specify Demographics for a
            cross-sectional distribution per time slice
        time_slice_column (str): Optional. Name of the time-slice field to create
        """
        self.time_slice_column: str = time_slice_column
        if cross_sections is None or len(cross_sections) == 0:
            self.cross_sections: Optional[list[Demographic]] = None
        else:
            self.cross_sections = list(cross_sections)

        user_demographics = user_demographics.rename(
            columns={"userid": "userid_demographics"}
        )
        data = user_post_times.merge(
            user_demographics, left_on="userid", right_on="userid_demographics"
        )
        data[self.time_slice_column] = (
            pd.to_datetime(data["created_at"])
            .dt.to_period(time_aggregation.round_key())
            .dt.start_time
        )

        self.counts: pd.DataFrame = self._get_counts(data)
        self.demographic_distributions: dict[
            Demographic, pd.Series
        ] = self._user_demographic_counts(data)
        self.cross_sections_table = self._user_cross_sections(data)

    def _get_counts(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data[[self.time_slice_column, "userid"]].assign(count=0)
        table = (
            data.groupby([self.time_slice_column, "userid"])
            .count()
            .reset_index("userid")["count"]
        )
        ts_groups = table.groupby(self.time_slice_column)
        counts = ts_groups.sum().to_frame().rename(columns={"count": "n_tweets"})
        counts["n_tweeters"] = ts_groups.count()
        return counts

    def _user_demographic_counts(
        self, data: pd.DataFrame
    ) -> dict[Demographic, pd.Series]:
        demographic_counts = {}
        distinct_data = data.drop_duplicates([self.time_slice_column, "userid"])
        for dem in Demographic:
            demographic_counts[dem] = (
                distinct_data.assign(count=0)
                .groupby([self.time_slice_column, dem])
                .count()["count"]
            )
        return demographic_counts

    def _user_cross_sections(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.cross_sections is None:
            return pd.DataFrame()
        distinct_data = data.drop_duplicates([self.time_slice_column, "userid"])
        cross_sections_table = (
            distinct_data.assign(count=0)
            .groupby([self.time_slice_column, *self.cross_sections])
            .count()["count"]
            .reset_index()
        )
        return cross_sections_table

    def censor(self, min_displayed_users: int) -> TimeSlicedUserDemographicAggregation:
        """
        Remove demographic counds below a minimum display threshold.
        """
        for dem in self.demographic_distributions.keys():
            self.demographic_distributions[dem] = censor_table(
                self.demographic_distributions[dem],
                min_displayed_count=min_displayed_users,
            ).dropna()

        if self.cross_sections is not None:
            self.cross_sections_table["count"] = censor_table(
                self.cross_sections_table["count"],
                min_displayed_count=min_displayed_users,
            )
            self.cross_sections_table = self.cross_sections_table.dropna()

        return self

    def to_list(self) -> list[dict[Hashable, Any]]:
        """
        Convert this aggregation into a JSON serializable Python list.
        """
        records = self.counts.reset_index().to_dict("records")
        for record in records:
            record.update(**self._base_record_tables())
        indexed_records = {record[self.time_slice_column]: record for record in records}
        for dem in Demographic:
            for ts, table in (
                self.demographic_distributions[dem]
                .reset_index()
                .groupby(self.time_slice_column)
            ):
                indexed_records[ts][dem] = table.set_index(dem)["count"].to_dict()
        if self.cross_sections is not None:
            for ts, table in self.cross_sections_table.reset_index().groupby(
                self.time_slice_column
            ):
                indexed_records[ts]["groups"] = table[
                    [*self.cross_sections, "count"]
                ].to_dict("records")
        return records

    def _base_record_tables(self) -> dict[str, Any]:
        base_record: dict[str, Any] = {}
        for dem in Demographic:
            base_record[dem] = {}
        if self.cross_sections is not None:
            base_record["groups"] = []
        return base_record

    def _fill_zeros(self) -> Tuple:
        time_slices = self.counts.reset_index()[
            self.time_slice_column
        ].drop_duplicates()
        demographic_distributions = {}
        for dem in Demographic:
            table = self.demographic_distributions[dem]
            indices = list(zip(time_slices, dem.values()))
            demographic_distributions[dem] = table.reindex(indices, fill_value=0)
        if self.cross_sections is None:
            cross_sections_table = pd.DataFrame()
        else:
            index_columns = [self.time_slice_column, *self.cross_sections]
            indices = list(
                zip(
                    time_slices,
                    itertools.chain(
                        *[
                            Demographic.values(Demographic(dem))
                            for dem in index_columns[1:]
                        ]
                    ),
                )
            )
            cross_sections_table = (
                self.cross_sections_table.reset_index()
                .set_index(index_columns)
                .reindex(indices)
            )

        return demographic_distributions, cross_sections_table
