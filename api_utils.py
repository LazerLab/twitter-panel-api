from .config import VALID_AGG_TERMS, DEMOGRAPHIC_FIELDS
from typing import Iterable

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


def validate_keyword_search_input(search_query: str, time_agg: str, group_by: Iterable[str] = None) -> bool:
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
        if any((d not in DEMOGRAPHIC_FIELDS for d in group_by)):
            return False
    return True
