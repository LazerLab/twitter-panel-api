import os
import sys
import time
import datetime
import numpy as np
import ujson as json

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
import findspark

findspark.init()
findspark.os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
findspark.os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml

from .config import ES_PASSWORD, TWEET_SCHEMA, VOTER_FILE_LOC, PANEL_TWEETS_LOC

# setting global variables
JOB_NAME = "twitter_dashboard"
DEBUG = False


class TweetDocument(Document):
    """
    This is an elasticsearch document definition
    basically specifying the fields involved in one searchable unit
    in this case it's a tweet; we can add other info later as needed.
    """

    url = Text()
    retweet = Integer()
    userid = Text()
    text = Text(analyzer="snowball")
    reply = Integer()
    timestamp = Date()

    class Index:
        # this is the index where the tweets live (we just give it a name)
        name = "tweets"


def port_table_to_elastic():
    """
    DO NOT RUN ME WITHOUT CHANGING THE DATA SOURCE!!!!!
    Right now, this function takes in data from a .tsv of tweets and the .tsv containing voter data.
    It puts this data in a format Elasticsearch likes and then puts the data into Elasticsearch.

    Note that portions involving the Spark cluster are currently commented out.
    This is because the Spark cluster is in use so frequently, I didn't want to use it as my data source
    for a test version of the website.
    The Spark code may need some revision.

    ** IF YOU RERUN THIS YOU WILL OVERWRITE THE EXISTING ELASTICSEARCH DATA **
    This shouldn't be a problem but will take a long time.
    """
    # this is connecting to the ES instance.
    es = Elasticsearch(
        "https://elastic:{}@localhost:9200".format(PASSWORD),
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    connection = connections.create_connection(
        hosts=["https://elastic:+pisVt6RnlJNTjSpFm0t@localhost:9200"],
        verify_certs=False,
        ssl_show_warn=False,
    )
    # here's where we'd connect to Spark.
    #     spark = (
    #         SparkSession.builder.appName(f"{JOB_NAME} (baseline)")
    #         .master("local")
    #         .config("PYSPARK_PYTHON", "python3")
    #         .config("PYSPARK_DRIVER_PYTHON", "python3")
    #         .getOrCreate()
    #     )
    TweetDocument.init()

    # we're getting tweets from the panel (this is a sample TSV)
    panel_tweets = pd.read_csv(
        "/net/data/twitter-voters/text_samples/2021_09.tsv", sep="\t"
    )
    panel_tweets.columns = [
        "tweetid",
        "url",
        "retweet",
        "userid",
        "text",
        "reply",
        "timestamp",
    ]
    print(panel_tweets.head(10))
    # this is code leftover from Sumukh's work;
    # it should load panel tweets but will need modification for our purposes
    # Load panel tweets (use same columns as health_info)
    #     panel_tweets = (
    #         spark.read.json(
    #             PANEL_TWEETS_LOC, schema=StructType.fromJson(json.load(open(TWEET_SCHEMA)))
    #         )
    #         .selectExpr(
    #             "id_str",
    #             "created_at",
    #             "user.id_str as userid",
    #             "full_text as full_text",
    #         )
    #     )
    if DEBUG:
        panel_tweets = panel_tweets.sample(frac=0.1)
    # this is populating the users database (so we can get demographic info on people who tweet)
    # note that this only needs to run once; the index with users should be good to go now!
    #     panel_data = pd.read_csv('/net/data/twitter-voters/TSmart-cleaner-Oct2017-rawFormat.tsv', sep='\t')
    #
    # don't run these again; they will overwrite all the data in the indices and make new indices.
    # es.indices.create(index='tweets')
    # es.indices.create(index='users')

    # this code populates the users index.
    #     users = 0
    #     tot_users = len(panel_data)
    #     panel_data = panel_data[['twProfileID', 'tsmart_state', 'voterbase_gender', 'voterbase_race', 'voterbase_age', 'vf_party', 'tsmart_partisan_score']]
    #     print(tot_users)
    #     for row in panel_data.to_dict(orient='records'):
    #         if users % 1000 == 0:
    #             print('users', users / tot_users)
    #         users += 1
    #         if str(row['voterbase_age']) == 'nan':
    #             row['voterbase_age'] = 0.0
    #         if str(row['vf_party']) == 'nan':
    #             row['vf_party'] = 'UNKNOWN'
    #         try:
    #             es.index(index='users', id=int(row['twProfileID']), body=row)
    #         except Exception as e:
    #             print(row)
    #             row = {str(k): str(v) for k, v in row.items()}
    #             es.index(index='users', id=int(row['twProfileID']), body=row)
    #

    # this ingests the tweets into ES.
    # takes a couple hours for a 10% sample of a month's data.
    tweets = 0
    tot_tweets = len(panel_tweets)
    panel_tweets["timestamp"] = panel_tweets["timestamp"].apply(
        lambda b: pd.to_datetime(b)
    )
    print(tot_tweets)
    for row in panel_tweets.to_dict(orient="records"):
        row["url"] = str(row["url"])
        row["userid"] = str(row["userid"])
        row["text"] = str(row["text"])
        if tweets % 1000 == 0:
            print("tweets", tweets / tot_tweets)
        tweets += 1
        try:
            es.index(index="tweets", id=int(row["tweetid"]), body=row)
        except Exception as e:
            print(e)
            print(row)
    return True


def query_spark_for_keyword(keyword):
    """
    This should query Spark for all tweets containing a keyword
    However, since Spark is in use a lot, I haven't been able to properly test it.
    The idea is to use this to test timing (is ES faster than Spark?)
    Input: keyword: a string; what we're searching for
    Output: dataframe of all tweets containing keyword
    """
    # Initialize spark session
    spark = (
        SparkSession.builder.appName(f"{JOB_NAME} (baseline)")
        .config("PYSPARK_PYTHON", "python3")
        .config("PYSPARK_DRIVER_PYTHON", "python3")
        .getOrCreate()
    )

    # Load panel tweets (use same columns as health_info)
    # will need to grab more columns than what we grab here
    panel_tweets = spark.read.json(
        PANEL_TWEETS_LOC, schema=StructType.fromJson(json.load(open(TWEET_SCHEMA)))
    ).selectExpr(
        "id_str",
        "created_at",
        "user.id_str as userid",
        "full_text as full_text",
    )
    if DEBUG:
        panel_tweets = panel_tweets.sample(fraction=0.01)
    # filtering based on keyword presence
    panel_tweets = panel_tweets.filter(
        panel_tweets["full_text"].rlike("*{}*".format(keyword))
    )
    # pulling in demographic info
    panel_data = read_panel_data(spark, VOTER_FILE_LOC)
    # doing a join to associate tweets w/ demographic info
    panel_tweets = (
        panel_tweets.join(
            panel_data, panel_tweets.userid == panel_data.twProfileID, "left"
        )
        .drop("twProfileID")
        .select(
            "id_str",
            "created_at",
            "userid",
            "status_text",
            "rt_status_text",
            "qt_status_text",
            "state",
            "county",
            "fips_code",
            "dma",
            "race",
            "gender",
            "age",
            "binned_age",
            "party_score",
            "party_reg",
            "registered_voter",
        )
    )
    # should return a dataframe
    return panel_tweets.toPandas()
