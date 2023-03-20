import pytest
from unittest.mock import patch
from panel_api.app import app as app_singleton
import json
from panel_api.config import Demographic
from panel_api.api_utils import KeywordQuery


@pytest.fixture
def app():
    app_singleton.config.update({"TESTING": True})

    yield app_singleton


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def mock_es_query():
    with patch("panel_api.app.ElasticsearchTwitterPanelSource.query_from_api") as m:
        yield m


@pytest.fixture
def mock_censor():
    with patch("panel_api.app.censor_keyword_search_output") as m:
        yield m


def test_keyword_search_valid_query(client, mock_es_query, mock_censor):
    mock_es_query.return_value = "mock_value"
    mock_censor.return_value = "mock_value"
    query_json = {
        "keyword_query": "test query",
        "aggregate_time_period": "week",
        "cross_sections": ["voterbase_race", "voterbase_gender"],
    }
    response = client.get(
        "/keyword_search",
        json=query_json,
    )

    # Just checking that the correct query params are forwarded.
    # Other params (e.g. 'fill_zeros') are irrelevant to this endpoint's success.
    mock_es_query.assert_called_once()
    forwarded_query = mock_es_query.call_args.kwargs.get("query")
    if forwarded_query is None:
        forwarded_query = mock_es_query.call_args.args[0]
    assert forwarded_query == KeywordQuery(
        "test query", "week", cross_sections=[Demographic.RACE, Demographic.GENDER]
    )

    mock_censor.assert_called_once()
    assert "mock_value" in mock_censor.call_args.args

    assert json.loads(response.data) == {
        "query": query_json,
        "response_data": "mock_value",
    }


def test_keyword_search_invalid_query(client):
    query_json = {
        "keyword_query": "test query",
        "aggregate_time_period": "a gazillion years",
    }
    response = client.get(
        "/keyword_search",
        json=query_json,
    )

    assert json.loads(response.data) == {
        "query": query_json,
        "response_data": "invalid query",
    }
