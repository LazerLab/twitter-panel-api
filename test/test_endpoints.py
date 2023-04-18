import json
from unittest.mock import MagicMock, patch

import pytest

from panel_api import create_app


@pytest.fixture
def app():
    test_app = create_app(TESTING=True)

    yield test_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def mock_query():
    with patch("panel_api.endpoints.KeywordQuery.from_raw_query") as m:
        query = MagicMock()
        m.return_value = query
        yield query


def test_keyword_search_valid_query(client, mock_query):
    mock_response = MagicMock()
    mock_response.censor.return_value = mock_response
    mock_response.to_list.return_value = "mock_value"
    mock_query.execute.return_value = mock_response
    query_json = {
        "keyword_query": "test query",
        "aggregate_time_period": "week",
        "cross_sections": ["voterbase_race", "voterbase_gender"],
        "after": "2020-10-01",
        "before": "2020-12-31",
    }
    response = client.get(
        "/keyword_search",
        json=query_json,
    )

    # Just checking that the correct query params are forwarded.
    # Other params (e.g. 'fill_zeros') are irrelevant to this endpoint's success.
    mock_query.execute.assert_called_once()
    mock_response.censor.assert_called_once()

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
