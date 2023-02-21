import panel_api.sources
import pytest
from unittest.mock import patch, MagicMock
import panel_api.sources as sources
from datetime import datetime


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
