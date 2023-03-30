"""
Module containing classes for backend data sources.
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import Collection, Optional, Tuple, Union

import pandas as pd

from panel_api import api_utils

from .api_utils import (
    KeywordQuery,
    categorize_age,
    censor_keyword_search_output,
    int_or_nan,
)
from .api_values import Demographic, TimeAggregation
from .es_utils import elastic_query_for_keyword
from .sql_utils import collect_voters


class MediaSource(ABC):
    """
    Generic superclass for media sources.
    Supports very basic POST functionality to a base URL
    but your query had best be valid according to whatever you're querying.

    ***DON'T USE THIS CLASS DIRECTLY***
    """

    @abstractmethod
    def query_from_api(self, query: KeywordQuery, fill_zeros=False):
        """
        Create a data response for a keyword search query on a particular MediaSource.
        This is intended to be used directly by the Flask API when it wants data.

        Parameters:
        search_query: The keyword to search for
        agg_by: Time period aggregation ("day"/"week")
        group_by: Cross-sectional demographics to record
            (Ex. [Demographic.RACE, Demographic.GENDER])
        fill_zeros: Return explicit zeros in the response (Default: False)

        Returns:
        Keyword search response
        """

    @staticmethod
    def aggregate_tabular_data(
        full_df,
        ts_col_name,
        time_agg: TimeAggregation,
        group_by: Optional[list[Demographic]] = None,
        fill_zeros=False,
    ):
        """
        Aggregate data into time-sliced records.
        """
        agg_freq_str = TimeAggregation.round_key(time_agg)
        full_df[f"{time_agg}_rounded"] = (
            pd.to_datetime(full_df[ts_col_name])
            .dt.to_period(agg_freq_str)
            .dt.start_time
        )
        # bucket age by decade
        full_df[Demographic.AGE] = full_df[Demographic.AGE].apply(categorize_age)
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


class TwitterSource(ABC):
    """
    Source of Twitter information.
    """

    @abstractmethod
    def match_keyword(
        self,
        keyword: str,
        time_range: Union[Tuple[Optional[date], Optional[date]], list[Optional[date]]],
    ) -> pd.DataFrame:
        """
        Pull tweets from the source, containing a keyword, over a specific time range.

        keyword: str of len>=1 to search for
        time_range: start and end dates of the time range. Either may be None to
            represent "no boundary"

        returns: DataFrame with "created_at" and "userid" columns
        """


class DemographicSource(ABC):
    """
    Source of demographic information about Twitter users.
    """

    @abstractmethod
    def get_demographics(self, twitter_user_ids: Collection[str]) -> pd.DataFrame:
        """
        Get demographic information of Twitter users.

        Parameters:
        twitter_user_ids: ids to collect demographics for

        Returns:
        Pandas Dataframe, with the following columns:
        - "userid": Twitter user id
        - One column for each demographic in `api_utils.Demographic`
        """


class CompositeSource(MediaSource):
    """
    Base MediaSource implementation.
    Specify a source Twitter and demographic information.
    """

    def __init__(
        self, twitter_source: TwitterSource, demographic_source: DemographicSource
    ) -> None:
        self.twitter_source = twitter_source
        self.demographic_source = demographic_source

    def query_from_api(self, query: KeywordQuery, fill_zeros=False):
        twitter_data = self.twitter_source.match_keyword(
            keyword=query.keyword, time_range=query.time_range
        )
        print(twitter_data)
        demographic_data = self.demographic_source.get_demographics(
            twitter_data["userid"]
        ).rename(columns={"userid": "userid_demographics"})
        print(demographic_data)
        data = twitter_data.merge(
            demographic_data, left_on="userid", right_on="userid_demographics"
        )
        return MediaSource.aggregate_tabular_data(
            full_df=data,
            ts_col_name="created_at",
            time_agg=query.time_aggregation,
            group_by=query.cross_sections,
            fill_zeros=fill_zeros,
        )


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


class ElasticsearchTwitterPanelSource(TwitterSource):
    """
    Class for an Elasticsearch backend for Twitter data.
    """

    def match_keyword(self, keyword, time_range):
        res = elastic_query_for_keyword(
            keyword, before=time_range[1], after=time_range[0]
        )
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


class PostgresDemographicSource(DemographicSource):
    """
    PostgreSQL source of demographic information about Twitter users.
    """

    def __init__(self, **kwargs) -> None:
        """
        Create a PostgreSQL demographic source.

        Parameters:
        kwargs: PostgreSQL connection parameters
        """
        self.connection_params = kwargs

    def get_demographics(self, twitter_user_ids: Collection[str]) -> pd.DataFrame:
        voters = collect_voters(
            twitter_ids=twitter_user_ids, connection_params=self.connection_params
        )
        voters_df = pd.DataFrame(dict(voter) for voter in voters)
        voters_df.rename(
            columns={
                "twProfileID": "userid",
                "vf_source_state": Demographic.STATE,
                "voterbase_age": Demographic.AGE,
            },
            inplace=True,
        )
        return voters_df
