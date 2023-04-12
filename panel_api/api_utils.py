"""
Module for API-specific utilities.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Mapping, Optional, Tuple

import numpy as np

from .api_values import Demographic, TimeAggregation
from .data_utils import fill_record_counts, fill_value_counts
from .helpers import if_present


def int_or_nan(num) -> int:
    """
    hopefully b is a string that converts nicely to an int.
    if it is not, we return 0.
    a sketchy way to coerce strings to ints.
    i'm just using this for user IDs (need a better solution for prod)
    """
    try:
        return int(num)
    except ValueError:
        return 0


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

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, KeywordQuery):
            return self.__dict__ == __o.__dict__
        return False

    def __ne__(self, __o: object) -> bool:
        return not self.__eq__(__o)


def parse_api_date(date_string: str) -> date:
    """Parse this API's date string format to a date object."""
    return date.fromisoformat(date_string)


def demographic_from_name(name: str) -> Demographic:
    """
    Translate human-readable names for demographics to the Demographic enumeration.
    """
    if name in [str(Demographic.RACE), "race"]:
        return Demographic.RACE
    if name in [str(Demographic.GENDER), "gender"]:
        return Demographic.GENDER
    if name in [str(Demographic.AGE), "age"]:
        return Demographic.AGE
    if name in [str(Demographic.STATE), "state"]:
        return Demographic.STATE
    raise ValueError(f"Name '{name}' is not a recognized demographic")


def fill_zeros(results):
    """
    Add explicit zeros to a data response.

    Parameters:
    results: Data response

    Returns:
    Equivalent data response with explicit zeros
    """
    filled_results = []

    for period in results:
        filled_period = period.copy()
        for dem in Demographic:
            filled_period[dem] = fill_value_counts(
                filled_period[dem], all_values=Demographic.values(dem)
            )
        if "groups" in period:
            group_by = [
                field
                for field in filled_period["groups"][0].keys()
                if not field == "count"
            ]
            filled_period["groups"] = fill_record_counts(
                filled_period["groups"],
                values_mapping={dem: Demographic.values(dem) for dem in group_by},
            )
        filled_results.append(filled_period)
    return filled_results


def categorize_age(age: int) -> str:
    """
    Bucket ages into age categories, found in api_values.py.
    """
    if np.isnan(age):
        category = "Unknown"
    elif age < 30:
        category = "under 30"
    elif age >= 70:
        category = "70+"
    else:
        category = str(10 * int(age / 10)) + " - " + str(10 + 10 * int(age / 10))

    return category
