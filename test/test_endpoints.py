import pytest
from unittest.mock import patch
from panel_api.app import app as app_singleton
import json


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
def mock_verify_input():
    with patch("panel_api.app.validate_keyword_search_input") as m:
        yield m


@pytest.fixture
def mock_verify_output():
    with patch("panel_api.app.validate_keyword_search_output") as m:
        yield m


def test_keyword_search_valid_query(client, mock_es_query, mock_verify_output):
    mock_es_query.return_value = "mock_value"
    mock_verify_output.return_value = True

    response = client.get(
        "/keyword_search",
        json={
            "keyword_query": "test query",
            "aggregate_time_period": "week",
            "cross_sections": ["voterbase_race", "voterbase_gender"],
        },
    )

    mock_es_query.assert_called_once_with(
        search_query="test query",
        agg_by="week",
        group_by=["voterbase_race", "voterbase_gender"],
    )
    mock_verify_output.assert_called_once_with("mock_value")

    assert json.loads(response.data) == {
        "query": "test query",
        "agg_time_period": "week",
        "response_data": "mock_value",
    }


def test_keyword_search_invalid_query(client, mock_verify_input):
    mock_verify_input.return_value = False

    response = client.get(
        "/keyword_search",
        json={"keyword_query": "test query", "aggregate_time_period": "week"},
    )

    assert json.loads(response.data) == {
        "query": "test query",
        "agg_time_period": "week",
        "response_data": "invalid query",
    }


def test_keyword_search_privacy_error(client, mock_es_query, mock_verify_output):
    mock_es_query.return_value = "mock_value"
    mock_verify_output.return_value = False

    response = client.get(
        "/keyword_search",
        json={"keyword_query": "test query", "aggregate_time_period": "week"},
    )

    assert json.loads(response.data) == {
        "query": "test query",
        "agg_time_period": "week",
        "response_data": "sample too small to be statistically useful",
    }
