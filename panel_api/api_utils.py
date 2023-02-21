from .config import VALID_AGG_TERMS, DEMOGRAPHIC_FIELDS, USER_COUNT_PRIVACY_THRESHOLD
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
    search_query: str, time_agg: str, group_by: Iterable[str] = None
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
    if time_agg not in VALID_AGG_TERMS:
        return False
    if group_by is not None:
        if len([*group_by]) > len(set(group_by)) or any(
            (d not in DEMOGRAPHIC_FIELDS for d in group_by)
        ):
            return False
    return True


def validate_keyword_search_output(
    response_data: Iterable[Mapping[str, Any]], privacy_threshold: int = None
) -> bool:
    """
    Ensure that returned aggregate results are reasonably protective of privacy. Specifically,
    this function ensures no returned demographic counts are below the given privacy threshold.
    If no threshold is given, then USER_COUNT_PRIVACY_THRESHOLD will be used.
    """
    if privacy_threshold is None:
        privacy_threshold = USER_COUNT_PRIVACY_THRESHOLD

    for period in response_data:
        if "groups" in period:
            if any((group["count"] < privacy_threshold for group in period["groups"])):
                return False
        elif any(
            (
                any(count < privacy_threshold for count in period[dem].values())
                for dem in DEMOGRAPHIC_FIELDS
            )
        ):
            return False
    return True
