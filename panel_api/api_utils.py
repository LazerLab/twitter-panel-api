from .config import Config, VALID_AGG_TERMS, Demographic
import itertools
from typing import Any, Iterable, Mapping


def int_or_nan(b) -> int:
    """
    hopefully b is a string that converts nicely to an int.
    if it is not, we return 0.
    a sketchy way to coerce strings to ints.
    i'm just using this for user IDs (need a better solution for prod)
    """
    try:
        return int(b)
    except:
        return 0


def validate_keyword_search_input(
    search_query: str, agg_by: str, group_by: Iterable[Demographic] = None
) -> bool:
    """
    for the keyword_search endpoint, validate the two inputs from the user.
    search_query should be a string of length greater than 1,
    and agg_by should be in the set of valid aggregation terms.
    returns a boolean value (True if valid).
    """
    if search_query == "":
        return False
    elif search_query is None:
        return False
    if agg_by not in VALID_AGG_TERMS:
        return False
    if group_by is not None:
        if len([*group_by]) > len(set(group_by)) or any(
            (d not in [*Demographic] for d in group_by)
        ):
            return False
    return True


def validate_keyword_search_output(
    response_data: Iterable[Mapping[str, Any]], privacy_threshold: int = None
) -> bool:
    """
    Check whether returned aggregate results are reasonably protective of privacy. Specifically,
    this function ensures no returned demographic counts are below the given privacy threshold.
    If no threshold is given, then USER_COUNT_PRIVACY_THRESHOLD will be used.
    """
    if privacy_threshold is None:
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
    response_data: Iterable[Mapping[str, Any]],
    privacy_threshold: int = None,
    remove_censored_values=True,
) -> Iterable[Mapping[str, Any]]:
    """
    Enforce that returned aggregate result counts must equal or exceed the privacy threshold.
    If no threshold is given, then USER_COUNT_PRIVACY_THRESHOLD will be used from the config.

    Note: This mutates the provided response_data
    """
    if privacy_threshold is None:
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


def parse_query(raw_query):
    search_query = raw_query.get("keyword_query")
    agg_by = raw_query.get("aggregate_time_period")
    group_by = raw_query.get("cross_sections")
    if group_by is not None:
        group_by = [*map(demographic_from_name, group_by)]

    parsed_query = {
        "search_query": search_query,
        "agg_by": agg_by,
        "group_by": group_by,
    }

    if validate_keyword_search_input(**parsed_query):
        return parsed_query
    else:
        return None


def demographic_from_name(name) -> Demographic | None:
    if name in [str(Demographic.RACE), "race"]:
        return Demographic.RACE
    elif name in [str(Demographic.GENDER), "gender"]:
        return Demographic.GENDER
    elif name in [str(Demographic.AGE), "age"]:
        return Demographic.AGE
    elif name in [str(Demographic.STATE), "state"]:
        return Demographic.STATE
    else:
        return None
