import psycopg2
from .config import POSTGRESQL
from typing import Any, Iterable, Mapping


def collect_voters(twitter_ids: Iterable[str]) -> Iterable[Mapping[str, Any]]:
    """
    Collect panel voters' information from their Twitter user IDs.
    """
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

    conn = psycopg2.connect(**POSTGRESQL)
    cur = conn.cursor()

    cur.execute(temp_table_command)
    cur.executemany(fill_table_command, [(id,) for id in twitter_ids])
    cur.execute(collect_voters_command)

    voters = [x[0] for x in cur.fetchall()]

    return voters
