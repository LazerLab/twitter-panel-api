"""
This module specifies structures and functions for a keyword search query.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Mapping, Optional, Tuple

from panel_api.aggregation.user_demographics import TimeSlicedUserDemographicAggregation
from panel_api.source.tweets import TweetSource
from panel_api.source.voters import DemographicSource

from ..api_utils import demographic_from_name, parse_api_date
from ..api_values import Demographic, TimeAggregation
from ..helpers import if_present


class KeywordQuery:
    """
    Represents a keyword search query.
    """

    def __init__(
        self,
        keyword: str,
        time_aggregation: TimeAggregation,
        cross_sections: Optional[Iterable[Demographic]] = None,
        time_range: Tuple[Optional[date], Optional[date]] = (None, None),
        max_cross_sections: Optional[int] = None,
    ):
        self.keyword = keyword
        self.time_aggregation = TimeAggregation(time_aggregation)
        self.cross_sections = [*cross_sections] if cross_sections else []
        self.time_range = time_range
        if not self.validate(max_cross_sections):
            raise ValueError()

    @staticmethod
    def from_raw_query(
        raw_query: Mapping, max_cross_sections: Optional[int] = None
    ) -> Optional[KeywordQuery]:
        """
        Try to create a KeywordQuery from an API query dict. Returns None on failure.
        """
        search_query = raw_query.get("keyword_query")
        agg_by = raw_query.get("aggregate_time_period")
        group_by = raw_query.get("cross_sections")
        if group_by:
            group_by = [*map(demographic_from_name, group_by)]
        before = raw_query.get("before")
        after = raw_query.get("after")
        time_range = (
            if_present(parse_api_date, after),
            if_present(parse_api_date, before),
        )

        if search_query and agg_by:
            try:
                return KeywordQuery(
                    keyword=search_query,
                    time_aggregation=agg_by,
                    cross_sections=group_by,
                    time_range=time_range,
                    max_cross_sections=max_cross_sections,
                )
            except ValueError:
                return None
        return None

    def validate(self, max_cross_sections: Optional[int] = None) -> bool:
        """
        for the keyword_search endpoint, validate the two inputs from the user.
        search_query should be a string of length greater than 1,
        and agg_by should be in the set of valid aggregation terms.
        returns a boolean value (True if valid).
        """
        if not self.keyword:
            return False
        if self.time_aggregation not in TimeAggregation:
            return False
        if self.cross_sections and (
            (
                max_cross_sections is not None
                and len(self.cross_sections) > max_cross_sections
            )
            or len(self.cross_sections) > len(set(self.cross_sections))
            or any((d not in [*Demographic] for d in self.cross_sections))
        ):
            return False
        if len(self.time_range) != 2:
            return False
        if self.time_range[0] is not None and self.time_range[1] is not None:
            if self.time_range[1] - self.time_range[0] < timedelta(0):
                return False
        return True

    def execute(self) -> TimeSlicedUserDemographicAggregation:
        """
        Collect and aggregate the response data for this query.
        """
        twitter_data = TweetSource().match_keyword(
            keyword=self.keyword, time_range=self.time_range
        )
        demographic_data = DemographicSource().get_demographics(twitter_data["userid"])
        return TimeSlicedUserDemographicAggregation(
            user_post_times=twitter_data,
            user_demographics=demographic_data,
            time_aggregation=self.time_aggregation,
            cross_sections=self.cross_sections,
        )

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, self.__class__):
            return self.__dict__ == __o.__dict__
        return False

    def __ne__(self, __o: object) -> bool:
        return not self.__eq__(__o)
