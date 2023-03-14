import requests
import pandas as pd
import numpy as np
import itertools

from .api_utils import int_or_nan, demographic_from_name
from .config import Config, AGG_TO_ROUND_KEY, Demographic
from .es_utils import elastic_query_for_keyword, elastic_query_users
from .sql_utils import collect_voters


class MediaSource(object):
    """
    Generic superclass for media sources.
    Supports very basic POST functionality to a base URL
    but your query had best be valid according to whatever you're querying.

    ***DON'T USE THIS CLASS DIRECTLY***
    """

    def query_from_api(
        self, search_query, agg_by="day", group_by=None, fill_zeros=False, **kwargs
    ):
        """
        Create a data response for a keyword search query on a particular MediaSource.
        This is intended to be used directly by the Flask API when it wants data.

        Parameters:
        search_query: The keyword to search for
        agg_by: Time period aggregation ("day"/"week")
        group_by: Cross-sectional demographics to record (Ex. [Demographic.RACE, Demographic.GENDER])
        fill_zeros: Return explicit zeros in the response (Default: False)
        **kwargs: Source-specific kwargs

        Returns:
        Keyword search response
        """
        data = self._query_from_api(search_query=search_query, **kwargs)
        return MediaSource.aggregate_tabular_data(
            full_df=data,
            ts_col_name="created_at",
            time_agg=agg_by,
            group_by=group_by,
            fill_zeros=fill_zeros,
        )

    def _query_from_api(self, search_query) -> pd.DataFrame:
        """
        Collect keyword search data from this source. Must be implemented by subclasses.

        The resulting DataFrame must contain the following columns:
        "created_at": The time of the tweet
        "userid": The Twitter user ID of the person who tweeted
        All values of the `Demographic` enum: The demographic information of the user
        """
        pass

    @staticmethod
    def aggregate_tabular_data(
        full_df,
        ts_col_name,
        time_agg,
        group_by: list[str] = None,
        fill_zeros=False,
    ):
        agg_freq_str = AGG_TO_ROUND_KEY[time_agg]
        full_df["{}_rounded".format(time_agg)] = (
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
        table = full_df.groupby("{}_rounded".format(time_agg))
        # get all value counts for each day
        results = []
        for ts, t in table:
            t_dict = {"ts": ts, "n_tweets": len(t)}
            t = t.drop_duplicates(["userid"])
            t_dict["n_tweeters"] = len(t)
            for i in Demographic:
                t_dict[i] = t[i].value_counts().to_dict()
            if group_by is not None:
                count_label = group_by[0]
                t_dict["groups"] = (
                    t.groupby(group_by)[count_label]
                    .count()
                    .to_frame()
                    .rename(columns={count_label: "count"})
                    .reset_index()
                    .to_dict("records")
                )
            results.append(t_dict)
        if fill_zeros:
            results = fill_zeros(results)
        return results

    @staticmethod
    def fill_zeros(results):
        filled_results = []

        for period in results:
            filled_period = period.copy()
            for dem in Demographic:
                filled_period[dem] = {
                    value: period[dem][value] if value in period[dem] else 0
                    for value in Demographic.values(dem)
                }
            if "groups" in period:
                groups = pd.DataFrame.from_records(period["groups"])
                group_by = [col for col in groups.columns if not col == "count"]
                all_groups = itertools.product(
                    *[Demographic.values(field) for field in group_by]
                )
                groups = (
                    groups.set_index(group_by)
                    .reindex(all_groups, fill_value=0)
                    .reset_index()
                )
                filled_period["groups"] = groups.to_dict("records")
            filled_results.append(filled_period)
        return filled_results


class ElasticsearchTwitterPanelSource(MediaSource):
    def _query_from_api(self, search_query=""):
        """
        query function for the API to pull data out of Elasticsearch based on a search query.
        the data will then be aggregated at the level specified by agg_by.
        search_query: should be validated by functions upstream; is a string of length at least 1.
        agg_by: one of the valid aggregation levels
        returns: list of nested dicts, one for each time period, with data about who tweeted with the search query.
        """
        res = elastic_query_for_keyword(search_query)
        # querying ES for the query (no booleans or whatever exist yet!!)
        results = []
        # we make a dataframe out of the results.
        for hit in res:
            results.append(hit.to_dict())

        if len(results) == 0:
            results = []
            return results
        else:
            res_df = pd.DataFrame(results)
            # otherwise we make a dataframe

        res_df["userid"] = res_df["user"].apply(lambda u: int_or_nan(u["id"]))

        # grab all the users who tweeted
        users = collect_voters(set(res_df["userid"]))
        users_df = pd.DataFrame(users)

        # coerce user IDs
        users_df["twProfileID"] = users_df["twProfileID"].apply(lambda b: int_or_nan(b))
        # need them to both be ints to do the join
        # join results with the user demographics by twitter user ID
        full_df = res_df.merge(users_df, left_on="userid", right_on="twProfileID")
        full_df = full_df.rename(
            columns={
                "vf_source_state": Demographic.STATE,
                "voterbase_age": Demographic.AGE,
            }
        )

        return full_df


class CSVSource(MediaSource):
    def query_from_api(self, search_query="", agg_by="day"):
        cfg = Config()
        df = pd.read_csv(cfg["csv_data_loc"], sep="\t")
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        df["to_take"] = df[cfg["csv_text_col"]].apply(
            lambda b: search_query in b.lower()
        )
        full_df = df[df["to_take"]]
        return self.aggregate_tabular_data(full_df, "created_at", agg_by)
