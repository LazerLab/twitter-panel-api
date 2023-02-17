import pytest
from unittest.mock import patch
from app import app as app_singleton

@pytest.fixture()
def app():
    app_singleton.config.update({
        "TESTING": True
    })

    yield app_singleton

@pytest.fixture()
def client(app):
    return app.test_client()

@pytest.fixture()
def mock_es_source():
    with patch('app.ElasticsearchTwitterPanelSource') as m:
        yield m


@pytest.fixture()
def mock_verify_output():
    with patch('app.validate_keyword_search_output') as m:
        yield m

def test_keyword_search(client, mock_es_source, mock_verify_output):
    mock_es_source.query_from_api.return_value = "mock_value"
    mock_verify_output.return_value = True

    response = client.get("/keyword_search", json={
        "keyword_query": "test query",
        "aggregate_time_period": "week"
    })

    print(response)

    assert True