"""
Module defining sources of demographic information.
"""
from typing import Collection

import pandas as pd
from flask import current_app

from ..api_utils import categorize_age
from ..api_values import Demographic
from ..sql_utils import collect_voters
from .types import SourceType


class DemographicSource:
    """
    Source of demographic information about Twitter users.
    """

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
        source = current_app.config["VOTERS"]["SOURCE"]
        if source == SourceType.DATABASE:
            return PostgresDemographicSource().get_demographics(twitter_user_ids)
        elif source == SourceType.ATTACHED:
            return pd.DataFrame(current_app.config["VOTERS"]["ATTACHED_DATA"])
        else:
            raise NotImplementedError(
                f"DemographicSource not implemented for '{source}'"
            )


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
        voters_df[Demographic.AGE] = voters_df[Demographic.AGE].apply(categorize_age)
        return voters_df
