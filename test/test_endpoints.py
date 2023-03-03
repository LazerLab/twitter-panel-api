import pytest
from unittest.mock import patch
from panel_api.app import app as app_singleton
import json
from panel_api.config import Demographic


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
def mock_parse_input():
    with patch("panel_api.app.parse_query") as m:
        yield m


@pytest.fixture
def mock_censor():
    with patch("panel_api.app.censor_keyword_search_output") as m:
        yield m


def test_keyword_search_valid_query(client, mock_es_query, mock_censor):
    mock_es_query.return_value = "mock_value"
    mock_censor.return_value = "mock_value"

    response = client.get(
        "/keyword_search",
        json={
            "keyword_query": "test query",
            "aggregate_time_period": "week",
            "cross_sections": ["voterbase_race", "voterbase_gender"],
        },
    )

    # Just checking that the correct query params are forwarded.
    # Other params (e.g. 'fill_zeros') are irrelevant to this endpoint's success.
    mock_es_query.assert_called_once()
    query_kwargs = mock_es_query.call_args.kwargs
    for kwarg in {
        "search_query": "test query",
        "agg_by": "week",
        "group_by": [Demographic.RACE, Demographic.GENDER],
    }.items():
        assert kwarg in query_kwargs.items()

    mock_censor.assert_called_once()
    assert "mock_value" in mock_censor.call_args.args

    assert json.loads(response.data) == {
        "query": "test query",
        "agg_time_period": "week",
        "response_data": "mock_value",
    }


def test_keyword_search_invalid_query(client, mock_parse_input):
    mock_parse_input.return_value = None

    response = client.get(
        "/keyword_search",
        json={"keyword_query": "test query", "aggregate_time_period": "week"},
    )

    assert json.loads(response.data) == {
        "query": "test query",
        "agg_time_period": "week",
        "response_data": "invalid query",
    }
