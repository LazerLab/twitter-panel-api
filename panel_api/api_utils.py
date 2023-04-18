"""
Module for API-specific utilities.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from .api_values import Demographic
from .data_utils import fill_record_counts, fill_value_counts


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


def censor_table(table: pd.Series, min_displayed_count: int) -> pd.Series:
    """
    Remove all values of a numeric column under a display threshold.
    """

    def censor_function(count):
        return -1 if count < min_displayed_count else count

    table = table.apply(censor_function).replace(-1, None).dropna()

    return table


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
