from .config import VALID_AGG_TERMS


def int_or_nan(b):
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


def validate_keyword_search_input(search_query: str, agg_by: str):
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
    return True
