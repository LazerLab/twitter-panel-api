import pytest
from unittest.mock import patch, MagicMock
import panel_api.sources as sources
from datetime import datetime
import pandas as pd
from .utils import period_equals, list_equals_ignore_order
from panel_api.config import Demographic


@pytest.fixture
def tweet_data():
    """
    Data entries have been truncated for brevity and my sanity. If more of the actual structure becomes relevent, it should be added.
    """
    data = [
        {"created_at": "2023-02-17", "user": {"id": "0"}},
        {"created_at": "2023-02-17", "user": {"id": "1"}},
        {"created_at": "2023-02-19", "user": {"id": "2"}},
        {"created_at": "2023-02-19", "user": {"id": "3"}},
        {"created_at": "2023-02-19", "user": {"id": "9"}},
        {"created_at": "2023-02-20", "user": {"id": "4"}},
        {"created_at": "2023-02-21", "user": {"id": "5"}},
        {"created_at": "2023-02-21", "user": {"id": "6"}},
        {"created_at": "2023-02-21", "user": {"id": "7"}},
        {"created_at": "2023-02-21", "user": {"id": "8"}},
        {"created_at": "2023-02-21", "user": {"id": "9"}},
        {"created_at": "2023-02-21", "user": {"id": "9"}},
        {"created_at": "2023-02-22", "user": {"id": "9"}},
    ]
    hits_mock = []
    for entry in data:
        m = MagicMock()
        m.to_dict = MagicMock(return_value=entry)
        hits_mock.append(m)
    return hits_mock


@pytest.fixture
def voter_data():
    return pd.DataFrame(
        [
            ("0", "AL", "Male", 20, "Caucasian"),
            ("1", "GA", "Female", 21, "Uncoded"),
            ("2", "PA", "Male", 30, "Caucasian"),
            ("3", "MA", "Female", 55, "Asian"),
            ("4", "MA", "Male", 72, "African-American"),
            ("5", "IA", "Female", 147, "Hispanic"),
            ("6", "IL", "Male", 47, "Caucasian"),
            ("7", "CO", "Female", 32, "Native American"),
            ("8", "KS", "Male", 43, "Uncoded"),
            ("9", "CT", "Unknown", 58, "Caucasian"),
        ],
        columns=[
            "twProfileID",
            "vf_source_state",
            "voterbase_gender",
            "voterbase_age",
            "voterbase_race",
        ],
    ).to_dict("records")


@pytest.fixture
def voter_tweets():
    return pd.DataFrame(
        [
            ("2023-02-17", "0", "0", "AL", "Male", 20, "Caucasian"),
            ("2023-02-17", "1", "1", "GA", "Female", 21, "Uncoded"),
            ("2023-02-19", "2", "2", "PA", "Male", 30, "Caucasian"),
            ("2023-02-19", "3", "3", "MA", "Female", 55, "Asian"),
            ("2023-02-19", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-20", "4", "4", "MA", "Male", 72, "African-American"),
            ("2023-02-21", "5", "5", "IA", "Female", 147, "Hispanic"),
            ("2023-02-21", "6", "6", "IL", "Male", 47, "Caucasian"),
            ("2023-02-21", "7", "7", "CO", "Female", 32, "Native American"),
            ("2023-02-21", "8", "8", "KS", "Male", 43, "Uncoded"),
            ("2023-02-21", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-21", "9", "9", "CT", "Unknown", 58, "Caucasian"),
            ("2023-02-22", "9", "9", "CT", "Unknown", 58, "Caucasian"),
        ],
        columns=[
            "created_at",
            "userid",
            "twProfileID",
            "tsmart_state",
            "voterbase_gender",
            "voterbase_age",
            "voterbase_race",
        ],
    )


@pytest.fixture
def mock_es_search():
    with patch("panel_api.sources.elastic_query_for_keyword") as m:
        yield m


@pytest.fixture
def mock_voter_db(voter_data):
    with patch("panel_api.sources.collect_voters") as m:
        m.return_value = voter_data
        yield m


def test_es_query_daily(mock_es_search, mock_voter_db, tweet_data, voter_data):
    mock_es_search.return_value = tweet_data
    mock_voter_db.return_value = voter_data
    results = sources.ElasticsearchTwitterPanelSource().query_from_api(
        search_query="dinosaur", agg_by="day"
    )
    mock_es_search.assert_called_once_with("dinosaur")

    expected_results = [
        {
            "ts": datetime(2023, 2, 17),
            "n_tweets": 2,
            "n_tweeters": 2,
            "tsmart_state": {"AL": 1, "GA": 1},
            "voterbase_gender": {"Male": 1, "Female": 1},
            "vb_age_decade": {"20 - 30": 2},
            "voterbase_race": {"Caucasian": 1, "Uncoded": 1},
        },
        {
            "ts": datetime(2023, 2, 19),
            "n_tweets": 3,
            "n_tweeters": 3,
            "tsmart_state": {"PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 1, "Female": 1, "Unknown": 1},
            "vb_age_decade": {"30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {"Caucasian": 2, "Asian": 1},
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 1,
            "n_tweeters": 1,
            "tsmart_state": {"MA": 1},
            "voterbase_gender": {"Male": 1},
            "vb_age_decade": {"70 - 80": 1},
            "voterbase_race": {"African-American": 1},
        },
        {
            "ts": datetime(2023, 2, 21),
            "n_tweets": 6,
            "n_tweeters": 5,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"30 - 40": 1, "40 - 50": 2, "50 - 60": 1, "140 - 150": 1},
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "Uncoded": 1,
            },
        },
        {
            "ts": datetime(2023, 2, 22),
            "n_tweets": 1,
            "n_tweeters": 1,
            "tsmart_state": {"CT": 1},
            "voterbase_gender": {"Unknown": 1},
            "vb_age_decade": {"50 - 60": 1},
            "voterbase_race": {"Caucasian": 1},
        },
    ]

    assert list_equals_ignore_order(expected_results, results, period_equals)


def test_es_query_weekly(mock_es_search, mock_voter_db, tweet_data, voter_data):
    mock_es_search.return_value = tweet_data
    mock_voter_db.return_value = voter_data
    results = sources.ElasticsearchTwitterPanelSource().query_from_api(
        search_query="dinosaur", agg_by="week"
    )
    mock_es_search.assert_called_once_with("dinosaur")

    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"20 - 30": 2, "30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
            },
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 8,
            "n_tweeters": 6,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1, "MA": 1},
            "voterbase_gender": {"Male": 3, "Female": 2, "Unknown": 1},
            "vb_age_decade": {
                "30 - 40": 1,
                "40 - 50": 2,
                "50 - 60": 1,
                "70 - 80": 1,
                "140 - 150": 1,
            },
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "African-American": 1,
                "Uncoded": 1,
            },
        },
    ]

    assert list_equals_ignore_order(expected_results, results, period_equals)


def test_table_group_by(voter_tweets):
    results = sources.MediaSource().aggregate_tabular_data(
        voter_tweets,
        "created_at",
        time_agg="week",
        group_by=["voterbase_race", "voterbase_gender"],
    )

    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"20 - 30": 2, "30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
            },
            "groups": [
                {"voterbase_race": "Caucasian", "voterbase_gender": "Male", "count": 2},
                {"voterbase_race": "Asian", "voterbase_gender": "Female", "count": 1},
                {"voterbase_race": "Uncoded", "voterbase_gender": "Female", "count": 1},
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Unknown",
                    "count": 1,
                },
            ],
        },
        {
            "ts": datetime(2023, 2, 20),
            "n_tweets": 8,
            "n_tweeters": 6,
            "tsmart_state": {"IA": 1, "IL": 1, "CT": 1, "KS": 1, "CO": 1, "MA": 1},
            "voterbase_gender": {"Male": 3, "Female": 2, "Unknown": 1},
            "vb_age_decade": {
                "30 - 40": 1,
                "40 - 50": 2,
                "50 - 60": 1,
                "70 - 80": 1,
                "140 - 150": 1,
            },
            "voterbase_race": {
                "Caucasian": 2,
                "Hispanic": 1,
                "Native American": 1,
                "African-American": 1,
                "Uncoded": 1,
            },
            "groups": [
                {"voterbase_race": "Caucasian", "voterbase_gender": "Male", "count": 1},
                {
                    "voterbase_race": "African-American",
                    "voterbase_gender": "Male",
                    "count": 1,
                },
                {"voterbase_race": "Uncoded", "voterbase_gender": "Male", "count": 1},
                {
                    "voterbase_race": "Hispanic",
                    "voterbase_gender": "Female",
                    "count": 1,
                },
                {
                    "voterbase_race": "Native American",
                    "voterbase_gender": "Female",
                    "count": 1,
                },
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Unknown",
                    "count": 1,
                },
            ],
        },
    ]

    assert list_equals_ignore_order(expected_results, results, period_equals)


def test_fill_zeros():
    sparse_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": {"AL": 1, "GA": 1, "PA": 1, "MA": 1, "CT": 1},
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {"20 - 30": 2, "30 - 40": 1, "50 - 60": 2},
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
            },
            "groups": [
                {"voterbase_race": "Caucasian", "voterbase_gender": "Male", "count": 2},
                {"voterbase_race": "Asian", "voterbase_gender": "Female", "count": 1},
                {"voterbase_race": "Uncoded", "voterbase_gender": "Female", "count": 1},
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Unknown",
                    "count": 1,
                },
            ],
        },
    ]
    expected_results = [
        {
            "ts": datetime(2023, 2, 13),
            "n_tweets": 5,
            "n_tweeters": 5,
            "tsmart_state": (
                pd.Series([1, 1, 1, 1, 1], index=["AL", "GA", "PA", "MA", "CT"])
                .reindex(Demographic.STATE.values(), fill_value=0)
                .to_dict()
            ),
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": (
                pd.Series([2, 1, 2], index=["20 - 30", "30 - 40", "50 - 60"])
                .reindex(Demographic.AGE.values(), fill_value=0)
                .to_dict()
            ),
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
                "African-American": 0,
                "Hispanic": 0,
                "Other": 0,
                "Native American": 0,
            },
            "groups": pd.DataFrame(
                [
                    ("Caucasian", "Male", 2),
                    ("Asian", "Female", 1),
                    ("Uncoded", "Female", 1),
                    ("Caucasian", "Unknown", 1),
                    ("Caucasian", "Female", 0),
                    ("African-American", "Female", 0),
                    ("African-American", "Male", 0),
                    ("African-American", "Unknown", 0),
                    ("Hispanic", "Female", 0),
                    ("Hispanic", "Male", 0),
                    ("Hispanic", "Unknown", 0),
                    ("Uncoded", "Male", 0),
                    ("Uncoded", "Unknown", 0),
                    ("Asian", "Male", 0),
                    ("Asian", "Unknown", 0),
                    ("Other", "Female", 0),
                    ("Other", "Male", 0),
                    ("Other", "Unknown", 0),
                    ("Native American", "Female", 0),
                    ("Native American", "Male", 0),
                    ("Native American", "Unknown", 0),
                ],
                columns=[Demographic.RACE, Demographic.GENDER, "count"],
            ).to_dict("records"),
        },
    ]
    filled_results = sources.MediaSource().fill_zeros(sparse_results)

    assert list_equals_ignore_order(expected_results, filled_results, period_equals)
