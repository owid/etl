import pytest
import httpx
from unittest.mock import AsyncMock


@pytest.fixture
def mock_dag():
    # Dag of active steps.
    mock_dag_dict = {
        "steps": {
            "a": set(["b", "c"]),
            "b": set(["e", "d"]),
            "c": set(),
            "d": set(["e", "f"]),
            "e": set(),
            "f": set(),
        },
        "archive": {
            "g": set(["f"]),
            "h": set(["i", "j"]),
        },
    }

    return mock_dag_dict


@pytest.fixture
def mock_expected_dependencies():
    # Expected set of all dependencies for each active step.
    mock_expected_dependencies_dict = {
        "a": set(["b", "c", "e", "d", "f"]),
        "b": set(["e", "d", "f"]),
        "c": set(),
        "d": set(["e", "f"]),
        "e": set(),
        "f": set(),
    }

    return mock_expected_dependencies_dict


@pytest.fixture
def mock_expected_usages():
    # Expected set of all usages for each active step.
    mock_expected_usages_dict = {
        "a": set(),
        "b": set(["a"]),
        "c": set(["a"]),
        "d": set(["b", "a"]),
        "e": set(["d", "b", "a"]),
        "f": set(["d", "b", "a", "g"]),
    }

    return mock_expected_usages_dict


@pytest.fixture
def mock_expected_direct_usages():
    # Expected set of direct usages for each active step.
    mock_expected_direct_usages_dict = {
        "a": set(),
        "b": set(["a"]),
        "c": set(["a"]),
        "d": set(["b"]),
        "e": set(["d", "b"]),
        "f": set(["d", "g"]),
    }

    return mock_expected_direct_usages_dict


@pytest.fixture
def mock_etl_api_responses():
    """Mock responses for ETL API semantic search based on query."""
    return {
        "population": {
            "results": [
                {
                    "title": "Population density",
                    "indicator_id": 12345,
                    "snippet": "Population per square kilometer of land area",
                    "score": 0.95,
                    "metadata": {
                        "catalog_path": "grapher/demography/2023-03-31/population/population#population_density",
                        "chart_count": 42
                    }
                }
            ],
            "query": "population",
            "total_results": 1
        },
        "cherry blossom": {
            "results": [
                {
                    "title": "Cherry blossom flowering date",
                    "indicator_id": 67890,
                    "snippet": "Date when cherry blossoms reach full bloom in Japan",
                    "score": 0.95,
                    "metadata": {
                        "catalog_path": "grapher/biodiversity/2025-06-28/cherry_blossom/cherry_blossom#cherry_blossom_date",
                        "chart_count": 5,
                        "sql_template": "SELECT country, year, cherry_blossom_date FROM '{parquet_url}' WHERE country = 'Japan'",
                        "parquet_url": "https://catalog.ourworldindata.org/biodiversity/2025-06-28/cherry_blossom/cherry_blossom.parquet",
                        "column": "cherry_blossom_date"
                    }
                }
            ],
            "query": "cherry blossom",
            "total_results": 1
        },
        "default": {
            "results": [
                {
                    "title": "Test indicator",
                    "indicator_id": 99999,
                    "snippet": "Generic test indicator",
                    "score": 0.5,
                    "metadata": {
                        "catalog_path": "grapher/test/2024-01-01/test/test#test_indicator",
                        "chart_count": 1
                    }
                }
            ],
            "query": "default",
            "total_results": 1
        }
    }


class MockResponse:
    def __init__(self, json_data):
        self._json_data = json_data

    def json(self):
        return self._json_data

    def raise_for_status(self):
        pass


@pytest.fixture(autouse=True)
def mock_httpx_post(monkeypatch, mock_etl_api_responses):
    """Mock httpx POST requests to ETL API."""
    
    async def mock_post(self, url, **kwargs):
        # Return our mock response for ETL API calls
        if "etl.owid.io" in url or "search/indicators" in url:
            # Get the query from the request body
            json_data = kwargs.get("json", {})
            query = json_data.get("query", "default")
            
            # Return appropriate mock response based on query
            response = mock_etl_api_responses.get(query, mock_etl_api_responses["default"])
            return MockResponse(response)
        # For other URLs, return a generic mock
        return MockResponse({"error": "Unexpected URL in test"})
    
    # Mock httpx.AsyncClient.post method
    monkeypatch.setattr("httpx.AsyncClient.post", mock_post)
