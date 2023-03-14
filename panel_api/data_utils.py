import pandas as pd
from typing import Mapping
import itertools


def fill_value_counts(counts, all_values, fill_value=0):
    """
    Fill missing values of Series-like dictionary of value counts with a single value.

    Parameters:
    counts: Series-like dictionary of counts
    all_values: List of all possible values to count
    fill_value: What to fill missing values with (Default: 0)

    Returns:
    Filled Series-like dictionary of counts
    """
    return pd.Series(counts).reindex(all_values, fill_value=fill_value).to_dict()


def fill_record_counts(
    records, values_mapping: Mapping, count_label="count", fill_value=0
):
    """
    Fill missing values in a records-formatted list of dicts with a single value.

    Parameters:
    records: Count records
    values_mapping: List of all possible values per key. The keys of this dict should match the keys in the records
    count_label: The key of the count field in the records
    fill_value: What to fill missing values with

    Returns:
    Filled records (all combinations of values of provided fields)
    """
    index_columns = list(values_mapping.keys())
    all_indices = itertools.product(*[values_mapping[value] for value in index_columns])
    df = (
        pd.DataFrame.from_records(records)
        .set_index(index_columns)
        .reindex(all_indices, fill_value=fill_value)
        .reset_index()
        .to_dict("records")
    )
    return df
