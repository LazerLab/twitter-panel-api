import requests
import pandas as pd
import numpy as np
import itertools

from .api_utils import int_or_nan, demographic_from_name
from .config import Config, AGG_TO_ROUND_KEY, Demographic, DEMOGRAPHIC_VALUES
from .es_utils import elastic_query_for_keyword, elastic_query_users
from .sql_utils import collect_voters


class MediaSource(object):
    """
    Generic superclass for media sources.
    Supports very basic POST functionality to a base URL
    but your query had best be valid according to whatever you're querying.

    ***DON'T USE THIS CLASS DIRECTLY***
    """

    def query_from_api(self, **kwargs):
        # implement this in the subclass.
        # intended to be used as the function called by the Flask API when it wants data.
        pass

    def aggregate_tabular_data(
        self,
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
        full_df[Demographic.AGE.value] = full_df["voterbase_age"].apply(
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
                    t.drop_duplicates(["userid"])
                    .groupby(group_by)[count_label]
                    .count()
                    .to_frame()
                    .rename(columns={count_label: "count"})
                    .reset_index()
                    .to_dict("records")
                )
            results.append(t_dict)
        if fill_zeros:
            results = self.fill_zeros(results)
        return results

    def fill_zeros(self, results):
        filled_results = []

        def nested_put(d, *keys, value):
            for key in keys[:-1]:
                if key not in d:
                    d[key] = {}
                d = d[key]
            d[keys[-1]] = value

        for period in results:
            filled_period = period.copy()
            for dem in Demographic:
                filled_period[dem] = {
                    value: period[dem][value] if value in period[dem] else 0
                    for value in DEMOGRAPHIC_VALUES[dem]
                }
            if "groups" in period:
                group_by = [
                    demographic_from_name(field)
                    for field in period["groups"][0].keys()
                    if not field == "count"
                ]
                all_groups = itertools.product(
                    *[DEMOGRAPHIC_VALUES[field] for field in group_by]
                )
                all_groups = [
                    {field: value for field, value in zip(group_by, group)}
                    for group in all_groups
                ]
                group_tree = {}
                for group in all_groups:
                    group["count"] = 0
                    group_values = [group[field] for field in group_by]
                    nested_put(group_tree, *group_values, value=group)
                for group in period["groups"]:
                    group_values = [group[field] for field in group_by]
                    nested_put(group_tree, *group_values, "count", value=group["count"])
                filled_period["groups"] = all_groups
            filled_results.append(filled_period)
        return filled_results


class ElasticsearchTwitterPanelSource(MediaSource):
    def query_from_api(
        self, search_query="", agg_by="day", group_by=None, fill_zeros=False
    ):
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
        full_df = full_df.rename(columns={"vf_source_state": "tsmart_state"})

        # right now we're aggregating results by day. this can change later.

        return self.aggregate_tabular_data(
            full_df, "created_at", agg_by, group_by, fill_zeros=fill_zeros
        )


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
