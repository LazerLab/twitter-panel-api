"""
Module defining sources of Twitter information, relevant to this API.
"""
from datetime import date
from typing import Iterable, Optional, Tuple, Union

import pandas as pd
from flask import current_app

from ..es_utils import elastic_query_for_keyword
from .types import SourceType


class TweetSource:
    """
    Source of Twitter information.
    """

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
        source = current_app.config["TWEETS"]["SOURCE"]
        if source == SourceType.ELASTICSEARCH:
            return ElasticsearchTweetSource().match_keyword(keyword, time_range)
        elif source == SourceType.ATTACHED:
            return pd.DataFrame(current_app.config["TWEETS"]["ATTACHED_DATA"])
        else:
            raise NotImplementedError(f"TweetSource is not implemented for '{source}'")


class ElasticsearchTweetSource(TweetSource):
    """
    Class for an Elasticsearch backend for Twitter data.
    """

    def match_keyword(self, keyword, time_range):
        res = elastic_query_for_keyword(
            keyword, before=time_range[1], after=time_range[0]
        )
        return self._raw_data_to_dataframe(res)

    def _raw_data_to_dataframe(self, es_data: Iterable[dict]):
        df = pd.DataFrame(es_data)
        if len(df) == 0:
            return pd.DataFrame(columns=["created_at", "userid"])

        df["userid"] = df["user"].apply(lambda u: str(u["id"]))

        return df
