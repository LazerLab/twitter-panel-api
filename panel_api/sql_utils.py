"""
Module for interacting with a PostgreSQL data backend.
"""
from typing import Any, Iterable, Mapping, Optional

import psycopg2

from .config import Config


def collect_voters(
    twitter_ids: Iterable[str], connection_params: Optional[Mapping] = None
) -> Iterable[Mapping[str, Any]]:
    """
    Collect panel voters' information from their Twitter user IDs.
    """
    if connection_params is None:
        connection_params = Config()["postgresql"]
    temp_table_command = """
    CREATE TABLE temp (
        id varchar(255)
    )
    """
    fill_table_command = """
    INSERT INTO temp (id) VALUES (%s)
    """
    collect_voters_command = """
    SELECT voters.data
    FROM voters INNER JOIN temp
    ON voters.twProfileID=temp.id
    """

    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()

    cur.execute(temp_table_command)
    cur.executemany(fill_table_command, [(id,) for id in twitter_ids])
    cur.execute(collect_voters_command)

    voters = [x[0] for x in cur.fetchall()]

    return voters
