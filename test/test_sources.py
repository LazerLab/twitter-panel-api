import pytest
from unittest.mock import patch, MagicMock
import panel_api.sources as sources
from datetime import datetime
import pandas as pd
from .utils import period_equals, list_equals_ignore_order


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
    return [
        {
            "twProfileID": "0",
            "vf_source_state": "AL",
            "voterbase_gender": "Male",
            "voterbase_age": 20,
            "voterbase_race": "Caucasian",
        },
        {
            "twProfileID": "1",
            "vf_source_state": "GA",
            "voterbase_gender": "Female",
            "voterbase_age": 21,
            "voterbase_race": "Uncoded",
        },
        {
            "twProfileID": "2",
            "vf_source_state": "PA",
            "voterbase_gender": "Male",
            "voterbase_age": 30,
            "voterbase_race": "Caucasian",
        },
        {
            "twProfileID": "3",
            "vf_source_state": "MA",
            "voterbase_gender": "Female",
            "voterbase_age": 55,
            "voterbase_race": "Asian",
        },
        {
            "twProfileID": "4",
            "vf_source_state": "MA",
            "voterbase_gender": "Male",
            "voterbase_age": 72,
            "voterbase_race": "African-American",
        },
        {
            "twProfileID": "5",
            "vf_source_state": "IA",
            "voterbase_gender": "Female",
            "voterbase_age": 147,
            "voterbase_race": "Hispanic",
        },
        {
            "twProfileID": "6",
            "vf_source_state": "IL",
            "voterbase_gender": "Male",
            "voterbase_age": 47,
            "voterbase_race": "Caucasian",
        },
        {
            "twProfileID": "7",
            "vf_source_state": "CO",
            "voterbase_gender": "Female",
            "voterbase_age": 32,
            "voterbase_race": "Native American",
        },
        {
            "twProfileID": "8",
            "vf_source_state": "KS",
            "voterbase_gender": "Male",
            "voterbase_age": 43,
            "voterbase_race": "Uncoded",
        },
        {
            "twProfileID": "9",
            "vf_source_state": "CT",
            "voterbase_gender": "Unknown",
            "voterbase_age": 58,
            "voterbase_race": "Caucasian",
        },
    ]


@pytest.fixture
def voter_tweets():
    return pd.DataFrame.from_records(
        [
            {
                "created_at": "2023-02-17",
                "userid": "0",
                "twProfileID": "0",
                "tsmart_state": "AL",
                "voterbase_gender": "Male",
                "voterbase_age": 20,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-17",
                "userid": "1",
                "twProfileID": "1",
                "tsmart_state": "GA",
                "voterbase_gender": "Female",
                "voterbase_age": 21,
                "voterbase_race": "Uncoded",
            },
            {
                "created_at": "2023-02-19",
                "userid": "2",
                "twProfileID": "2",
                "tsmart_state": "PA",
                "voterbase_gender": "Male",
                "voterbase_age": 30,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-19",
                "userid": "3",
                "twProfileID": "3",
                "tsmart_state": "MA",
                "voterbase_gender": "Female",
                "voterbase_age": 55,
                "voterbase_race": "Asian",
            },
            {
                "created_at": "2023-02-19",
                "userid": "9",
                "twProfileID": "9",
                "tsmart_state": "CT",
                "voterbase_gender": "Unknown",
                "voterbase_age": 58,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-20",
                "userid": "4",
                "twProfileID": "4",
                "tsmart_state": "MA",
                "voterbase_gender": "Male",
                "voterbase_age": 72,
                "voterbase_race": "African-American",
            },
            {
                "created_at": "2023-02-21",
                "userid": "5",
                "twProfileID": "5",
                "tsmart_state": "IA",
                "voterbase_gender": "Female",
                "voterbase_age": 147,
                "voterbase_race": "Hispanic",
            },
            {
                "created_at": "2023-02-21",
                "userid": "6",
                "twProfileID": "6",
                "tsmart_state": "IL",
                "voterbase_gender": "Male",
                "voterbase_age": 47,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-21",
                "userid": "7",
                "twProfileID": "7",
                "tsmart_state": "CO",
                "voterbase_gender": "Female",
                "voterbase_age": 32,
                "voterbase_race": "Native American",
            },
            {
                "created_at": "2023-02-21",
                "userid": "8",
                "twProfileID": "8",
                "tsmart_state": "KS",
                "voterbase_gender": "Male",
                "voterbase_age": 43,
                "voterbase_race": "Uncoded",
            },
            {
                "created_at": "2023-02-21",
                "userid": "9",
                "twProfileID": "9",
                "tsmart_state": "CT",
                "voterbase_gender": "Unknown",
                "voterbase_age": 58,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-21",
                "userid": "9",
                "twProfileID": "9",
                "tsmart_state": "CT",
                "voterbase_gender": "Unknown",
                "voterbase_age": 58,
                "voterbase_race": "Caucasian",
            },
            {
                "created_at": "2023-02-22",
                "userid": "9",
                "twProfileID": "9",
                "tsmart_state": "CT",
                "voterbase_gender": "Unknown",
                "voterbase_age": 58,
                "voterbase_race": "Caucasian",
            },
        ]
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

    assert (len(expected_results)) == len(results)
    assert all(
        [
            any([expected == result for result in results])
            for expected in expected_results
        ]
    )
    assert all(
        [
            any([result == expected for expected in expected_results])
            for result in results
        ]
    )


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

    assert (len(expected_results)) == len(results)
    assert all(
        [
            any([expected == result for result in results])
            for expected in expected_results
        ]
    )
    assert all(
        [
            any([result == expected for expected in expected_results])
            for result in results
        ]
    )


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
            "tsmart_state": {
                "AL": 1,
                "GA": 1,
                "PA": 1,
                "MA": 1,
                "CT": 1,
                "CA": 0,
                "TX": 0,
                "NY": 0,
                "FL": 0,
                "OH": 0,
                "IL": 0,
                "MI": 0,
                "NC": 0,
                "WA": 0,
                "MN": 0,
                "NJ": 0,
                "IN": 0,
                "VA": 0,
                "CO": 0,
                "WI": 0,
                "TN": 0,
                "AZ": 0,
                "MO": 0,
                "OR": 0,
                "MD": 0,
                "IA": 0,
                "KY": 0,
                "LA": 0,
                "SC": 0,
                "OK": 0,
                "KS": 0,
                "NV": 0,
                "NE": 0,
                "AR": 0,
                "UT": 0,
                "MS": 0,
                "DC": 0,
                "WV": 0,
                "ME": 0,
                "NM": 0,
                "NH": 0,
                "RI": 0,
                "ID": 0,
                "HI": 0,
                "SD": 0,
                "MT": 0,
                "ND": 0,
                "DE": 0,
                "AK": 0,
                "VT": 0,
                "WY": 0,
            },
            "voterbase_gender": {"Male": 2, "Female": 2, "Unknown": 1},
            "vb_age_decade": {
                "20 - 30": 2,
                "30 - 40": 1,
                "50 - 60": 2,
                "10 - 20": 0,
                "40 - 50": 0,
                "60 - 70": 0,
                "70 - 80": 0,
                "80 - 90": 0,
                "90 - 100": 0,
                "100 - 110": 0,
                "110 - 120": 0,
                "120 - 130": 0,
                "130 - 140": 0,
                "140 - 150": 0,
            },
            "voterbase_race": {
                "Caucasian": 3,
                "Asian": 1,
                "Uncoded": 1,
                "African-American": 0,
                "Hispanic": 0,
                "Other": 0,
                "Native American": 0,
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
                {
                    "voterbase_race": "Caucasian",
                    "voterbase_gender": "Female",
                    "count": 0,
                },
                {
                    "voterbase_race": "African-American",
                    "voterbase_gender": "Female",
                    "count": 0,
                },
                {
                    "voterbase_race": "African-American",
                    "voterbase_gender": "Male",
                    "count": 0,
                },
                {
                    "voterbase_race": "African-American",
                    "voterbase_gender": "Unknown",
                    "count": 0,
                },
                {
                    "voterbase_race": "Hispanic",
                    "voterbase_gender": "Female",
                    "count": 0,
                },
                {"voterbase_race": "Hispanic", "voterbase_gender": "Male", "count": 0},
                {
                    "voterbase_race": "Hispanic",
                    "voterbase_gender": "Unknown",
                    "count": 0,
                },
                {"voterbase_race": "Uncoded", "voterbase_gender": "Male", "count": 0},
                {
                    "voterbase_race": "Uncoded",
                    "voterbase_gender": "Unknown",
                    "count": 0,
                },
                {"voterbase_race": "Asian", "voterbase_gender": "Male", "count": 0},
                {"voterbase_race": "Asian", "voterbase_gender": "Unknown", "count": 0},
                {"voterbase_race": "Other", "voterbase_gender": "Female", "count": 0},
                {"voterbase_race": "Other", "voterbase_gender": "Male", "count": 0},
                {"voterbase_race": "Other", "voterbase_gender": "Unknown", "count": 0},
                {
                    "voterbase_race": "Native American",
                    "voterbase_gender": "Female",
                    "count": 0,
                },
                {
                    "voterbase_race": "Native American",
                    "voterbase_gender": "Male",
                    "count": 0,
                },
                {
                    "voterbase_race": "Native American",
                    "voterbase_gender": "Unknown",
                    "count": 0,
                },
            ],
        },
    ]
    filled_results = sources.MediaSource().fill_zeros(sparse_results)

    assert list_equals_ignore_order(expected_results, filled_results, period_equals)
