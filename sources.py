import requests
import pandas as pd

from .api_utils import int_or_nan
from .config import AGG_TO_ROUND_KEY
from .es_utils import elastic_query_for_keyword, elastic_query_users


class MediaSource(object):
    """
    Generic superclass for media sources.
    Supports very basic POST functionality to a base URL
    but your query had best be valid according to whatever you're querying.

    ***DON'T USE THIS CLASS DIRECTLY***
    """

    def __init__(self, source_url: str):
        """
        Instantiates the class and assigns it a source URL that can be used for manual queries.
        """
        self.source_url = source_url

    def query_raw_request(self, query_text: str):
        """
        Return the result of a POST request with query_text as the payload.
        query_text should be a string;
        this will return a requests.Response object.
        """
        return requests.post(self.source_url, query_test)

    def query_from_api(self, **kwargs):
        # implement this in the subclass.
        # intended to be used as the function called by the Flask API when it wants data.
        pass


class ElasticsearchTwitterPanelSource(MediaSource):
    def query_from_api(self, search_query="", agg_by="day"):
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

        if len(res) == 0:
            results = []
            return results
        else:
            res_df = pd.DataFrame(results)
            # otherwise we make a dataframe

        # grab all the users who tweeted
        users = elastic_query_users(res_df["userid"])
        users_df = pd.DataFrame(users)

        # coerce user IDs
        res_df["userid"] = res_df["userid"].apply(lambda b: int_or_nan(b))
        users_df["twProfileID"] = users_df["twProfileID"].apply(lambda b: int_or_nan(b))
        # need them to both be ints to do the join
        # join results with the user demographics by twitter user ID
        full_df = res_df.merge(users_df, left_on="userid", right_on="twProfileID")

        # right now we're aggregating results by day. this can change later.
        agg_freq_str = AGG_TO_ROUND_KEY[agg_by]
        full_df["day_rounded"] = pd.to_datetime(full_df["timestamp"]).dt.floor("d")
        # bucket age by decade
        full_df["vb_age_decade"] = full_df["voterbase_age"].apply(
            lambda b: str(10 * int(b / 10)) + " - " + str(10 + 10 * int(b / 10))
        )
        # aggregate by day
        table = full_df.groupby("{}_rounded".format(agg_by))
        # get all value counts for each day
        results = []
        for ts, t in table:
            t_dict = {"ts": ts, "n_tweets": len(t), "n_tweeters": len(set(t["userid"]))}
            for i in [
                "tsmart_state",
                "vb_age_decade",
                "voterbase_gender",
                "voterbase_race",
            ]:
                t_dict[i] = t[i].value_counts().to_dict()
            results.append(t_dict)

        return results
