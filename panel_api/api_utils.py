"""
Module for API-specific utilities.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Optional, Tuple

from .config import VALID_AGG_TERMS, Config, Demographic
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
        time_aggregation: str,
        cross_sections: Optional[Iterable[Demographic]] = None,
        time_range: Tuple[Optional[date], Optional[date]] = (None, None),
    ):
        self.keyword = keyword
        self.time_aggregation = time_aggregation
        self.cross_sections = [*cross_sections] if cross_sections else []
        self.time_range = tuple(time_range)
        if not self.validate():
            raise ValueError()

    @staticmethod
    def from_raw_query(raw_query: Mapping) -> Optional[KeywordQuery]:
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
                )
            except ValueError:
                return None
        return None

    def validate(self) -> bool:
        """
        for the keyword_search endpoint, validate the two inputs from the user.
        search_query should be a string of length greater than 1,
        and agg_by should be in the set of valid aggregation terms.
        returns a boolean value (True if valid).
        """
        if not self.keyword:
            return False
        if self.time_aggregation not in VALID_AGG_TERMS:
            return False
        if self.cross_sections and (
            len(self.cross_sections) > Config()["cross_sections_limit"]
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
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def validate_keyword_search_output(
    response_data: Iterable[Mapping[str, Any]], privacy_threshold: int = -1
) -> bool:
    """
    Check whether returned aggregate results are reasonably protective of privacy.
    Specifically, this function ensures no returned demographic counts are below the
    given privacy threshold. If no threshold is given, then
    USER_COUNT_PRIVACY_THRESHOLD will be used.
    """
    if privacy_threshold < 0:
        privacy_threshold = Config()["user_count_privacy_threshold"]

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


def censor_keyword_search_output(
    response_data: Iterable[dict[str, Any]],
    privacy_threshold: int = -1,
    remove_censored_values=True,
) -> Iterable[Mapping[str, Any]]:
    """
    Enforce that returned aggregate result counts must equal or exceed the privacy
    threshold. If no threshold is given, then USER_COUNT_PRIVACY_THRESHOLD will be
    used from the config.

    Note: This mutates the provided response_data
    """
    if privacy_threshold < 0:
        privacy_threshold = Config()["user_count_privacy_threshold"]

    if remove_censored_values:
        for period in response_data:
            if "groups" in period:
                period["groups"] = [
                    *filter(
                        lambda group: group["count"] >= privacy_threshold,
                        period["groups"],
                    )
                ]
            for dem in [*Demographic]:
                period[dem] = {
                    category: count
                    for category, count in period[dem].items()
                    if count >= privacy_threshold
                }
    else:
        for period in response_data:
            if "groups" in period:
                groups = period["groups"]
                period["groups"] = []
                for group in groups:
                    if group["count"] < privacy_threshold:
                        group["count"] = None
                    period["groups"].append(group)
            for dem in [*Demographic]:
                period[dem] = {
                    category: count if count >= privacy_threshold else None
                    for category, count in period[dem].items()
                }
    return response_data


def demographic_from_name(name) -> Demographic | None:
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
    return None


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
