import psycopg2
from .config import config

def collect_voters(ids):
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

    conn = psycopg2.connect(**config('postgresql'))
    cur = conn.cursor()

    cur.execute(temp_table_command)
    cur.executemany(fill_table_command, [(id,) for id in ids])
    cur.execute(collect_voters_command)

    voters = [x[0] for x in cur.fetchall()]

    return voters
