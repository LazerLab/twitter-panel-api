"""
Module containing classes for backend data sources.
"""
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

from panel_api import api_utils

from .api_utils import KeywordQuery, censor_keyword_search_output, int_or_nan
from .config import AGG_TO_ROUND_KEY, Demographic
from .es_utils import elastic_query_for_keyword
from .sql_utils import collect_voters


class MediaSource(ABC):
    """
    Generic superclass for media sources.
    Supports very basic POST functionality to a base URL
    but your query had best be valid according to whatever you're querying.

    ***DON'T USE THIS CLASS DIRECTLY***
    """

    def query_from_api(self, query: KeywordQuery, fill_zeros=False, **kwargs):
        """
        Create a data response for a keyword search query on a particular MediaSource.
        This is intended to be used directly by the Flask API when it wants data.

        Parameters:
        search_query: The keyword to search for
        agg_by: Time period aggregation ("day"/"week")
        group_by: Cross-sectional demographics to record
            (Ex. [Demographic.RACE, Demographic.GENDER])
        fill_zeros: Return explicit zeros in the response (Default: False)
        **kwargs: Source-specific kwargs

        Returns:
        Keyword search response
        """
        data = self._query_from_api(
            search_query=query.keyword, time_range=query.time_range, **kwargs
        )
        data = MediaSource.add_demographics(data, id_column="userid")
        return MediaSource.aggregate_tabular_data(
            full_df=data,
            ts_col_name="created_at",
            time_agg=query.time_aggregation,
            group_by=query.cross_sections,
            fill_zeros=fill_zeros,
        )

    @abstractmethod
    def _query_from_api(self, search_query, time_range) -> pd.DataFrame:
        """
        Collect keyword search data from this source. Must be implemented by subclasses.

        The resulting DataFrame must contain the following columns:
        "created_at": The time of the tweet
        "userid": The Twitter user ID of the person who tweeted
        """

    @staticmethod
    def aggregate_tabular_data(
        full_df,
        ts_col_name,
        time_agg,
        group_by: list[str] = None,
        fill_zeros=False,
    ):
        """
        Aggregate data into time-sliced records.
        """
        agg_freq_str = AGG_TO_ROUND_KEY[time_agg]
        full_df[f"{time_agg}_rounded"] = (
            pd.to_datetime(full_df[ts_col_name])
            .dt.to_period(agg_freq_str)
            .dt.start_time
        )
        # bucket age by decade
        full_df[Demographic.AGE] = full_df[Demographic.AGE].apply(
            lambda b: str(10 * int(b / 10)) + " - " + str(10 + 10 * int(b / 10))
            if not np.isnan(b)
            else "unknown"
        )
        # aggregate by day
        table = full_df.groupby(f"{time_agg}_rounded")
        # get all value counts for each day
        results = []
        for timestamp, ts_table in table:
            t_dict = {"ts": timestamp, "n_tweets": len(ts_table)}
            ts_table = ts_table.drop_duplicates(["userid"])
            t_dict["n_tweeters"] = len(ts_table)
            for i in Demographic:
                t_dict[i] = ts_table[i].value_counts().to_dict()
            if group_by:
                count_label = group_by[0]
                t_dict["groups"] = (
                    ts_table.groupby(group_by)[count_label]
                    .count()
                    .to_frame()
                    .rename(columns={count_label: "count"})
                    .reset_index()
                    .to_dict("records")
                )
            results.append(t_dict)
        if fill_zeros:
            results = api_utils.fill_zeros(results)
        return results

    @staticmethod
    def add_demographics(tweet_data: pd.DataFrame, id_column):
        """
        Merge tweet data with the demographic information
        """
        # grab all the users who tweeted
        users = collect_voters(set(tweet_data[id_column]))
        users_df = pd.DataFrame(users)

        # coerce user IDs
        users_df["twProfileID"] = users_df["twProfileID"].apply(int_or_nan)
        # need them to both be ints to do the join
        # join results with the user demographics by twitter user ID
        full_df = tweet_data.merge(users_df, left_on=id_column, right_on="twProfileID")
        full_df = full_df.rename(
            columns={
                "vf_source_state": Demographic.STATE,
                "voterbase_age": Demographic.AGE,
            }
        )
        return full_df


class ElasticsearchTwitterPanelSource(MediaSource):
    """
    Class for an Elasticsearch backend.
    """

    def _query_from_api(self, search_query, time_range):
        """
        query function for the API to pull data out of Elasticsearch based on a search
        query. the data will then be aggregated at the level specified by agg_by.

        search_query: should be validated by functions upstream; is a string of length
            at least 1.
        agg_by: one of the valid aggregation levels

        returns: list of nested dicts, one for each time period, with data about who
            tweeted with the search query.
        """
        res = elastic_query_for_keyword(search_query, time_range)
        # querying ES for the query (no booleans or whatever exist yet!!)
        results = []
        # we make a dataframe out of the results.
        for hit in res:
            results.append(hit.to_dict())

        if len(results) == 0:
            return pd.DataFrame(columns=["created_at", "userid"])
        # otherwise we make a dataframe
        res_df = pd.DataFrame(results)

        res_df["userid"] = res_df["user"].apply(lambda u: int_or_nan(u["id"]))

        return res_df


class CensoredSource(MediaSource):
    """
    Wrapper for other MediaSource classes that censors privacy-violating output
    from submitted queries.
    """

    def __init__(self, source: MediaSource, privacy_threshold: int = 10):
        self.source = source
        self.privacy_threshold = privacy_threshold

    def query_from_api(self, query: KeywordQuery, fill_zeros=False, **kwargs):
        return censor_keyword_search_output(
            self.source.query_from_api(query, fill_zeros, **kwargs),
            self.privacy_threshold,
            remove_censored_values=not fill_zeros,
        )

    def _query_from_api(self, search_query, time_range) -> pd.DataFrame:
        raise NotImplementedError()
