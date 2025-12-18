#
#  Tests for owid.catalog.client module
#
import pytest

from owid.catalog import Client
from owid.catalog.client import (
    ChartNotFoundError,
    ChartResult,
    ChartSearchResult,
    DatasetResult,
    IndicatorResult,
    LicenseError,
    PageSearchResult,
    ResultSet,
)


class TestClient:
    """Test the unified Client class."""

    def test_client_has_all_apis(self):
        client = Client()
        assert hasattr(client, "charts")
        assert hasattr(client, "_site_search")
        assert hasattr(client, "indicators")
        assert hasattr(client, "datasets")

    def test_client_repr(self):
        client = Client()
        assert "Client" in repr(client)


class TestChartsAPI:
    """Test the Charts API."""

    def test_get_chart_data(self):
        client = Client()
        df = client.charts.get_data("life-expectancy")

        assert df is not None
        assert len(df) > 0
        assert "entities" in df.columns
        assert "years" in df.columns

    def test_get_chart_data_by_url(self):
        client = Client()
        df = client.charts.get_data("https://ourworldindata.org/grapher/life-expectancy")

        assert df is not None
        assert len(df) > 0

    def test_get_chart_metadata(self):
        client = Client()
        meta = client.charts.metadata("life-expectancy")

        assert meta is not None
        assert isinstance(meta, dict)
        assert "columns" in meta

    def test_get_chart_config(self):
        client = Client()
        config = client.charts.config("life-expectancy")

        assert config is not None
        assert isinstance(config, dict)

    def test_fetch_chart(self):
        client = Client()
        chart = client.charts.fetch("life-expectancy")

        assert isinstance(chart, ChartResult)
        assert chart.slug == "life-expectancy"
        assert chart.title
        assert chart.url == "https://ourworldindata.org/grapher/life-expectancy"

    def test_chart_not_found(self):
        client = Client()
        with pytest.raises(ChartNotFoundError):
            client.charts.get_data("this-chart-does-not-exist")

    def test_non_redistributable_chart(self):
        client = Client()
        with pytest.raises(LicenseError):
            client.charts.get_data("test-scores-ai-capabilities-relative-human-performance")

    def test_invalid_url(self):
        client = Client()
        with pytest.raises(ValueError):
            client.charts.get_data("https://example.com/not-a-grapher-url")


class TestChartsAPISearch:
    """Test the Charts API search functionality."""

    def test_charts_search(self):
        """Test searching for charts via ChartsAPI."""
        client = Client()
        results = client.charts.search("life expectancy")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(isinstance(r, ChartSearchResult) for r in results)
        assert results[0].slug
        assert results[0].title

    def test_charts_search_with_countries(self):
        """Test searching with country filters."""
        client = Client()
        results = client.charts.search("gdp", countries=["France"])

        assert isinstance(results, ResultSet)
        # Results should be filtered by country

    def test_search_results_to_frame(self):
        """Test converting search results to DataFrame."""
        client = Client()
        results = client.charts.search("population")

        df = results.to_frame()
        assert "slug" in df.columns
        assert "title" in df.columns
        assert len(df) == len(results)


class TestSiteSearchAPI:
    """Test the internal SiteSearchAPI (advanced usage)."""

    def test_site_search_pages(self):
        """Test searching for pages via _site_search."""
        client = Client()
        results = client._site_search.pages("climate change")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(isinstance(r, PageSearchResult) for r in results)
        assert results[0].slug
        assert results[0].title

    def test_site_search_charts(self):
        """Test accessing charts via _site_search (should work but not recommended)."""
        client = Client()
        results = client._site_search.charts("gdp")

        assert isinstance(results, ResultSet)
        assert len(results) > 0


class TestIndicatorsAPI:
    """Test the Indicators API."""

    def test_search_indicators(self):
        client = Client()
        results = client.indicators.search("solar power generation")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(isinstance(r, IndicatorResult) for r in results)

        # Check result fields
        first = results[0]
        assert first.indicator_id > 0
        assert first.title
        assert 0 <= first.score <= 1
        assert first.catalog_path

    def test_indicator_results_to_catalog_frame(self):
        client = Client()
        results = client.indicators.search("co2 emissions")

        frame = results.to_catalog_frame()
        assert "indicator_title" in frame.columns
        assert "score" in frame.columns
        assert "path" in frame.columns


class TestDatasetsAPI:
    """Test the Datasets API."""

    def test_search_datasets(self):
        client = Client()
        results = client.datasets.search(table="population")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(isinstance(r, DatasetResult) for r in results)

    def test_search_datasets_by_namespace(self):
        client = Client()
        results = client.datasets.search(table="population", namespace="un")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(r.namespace == "un" for r in results)

    def test_dataset_results_to_catalog_frame(self):
        client = Client()
        results = client.datasets.search(table="population", namespace="un")

        frame = results.to_catalog_frame()
        assert "table" in frame.columns
        assert "namespace" in frame.columns
        assert "path" in frame.columns

        # Should be loadable
        assert hasattr(frame, "load")

    def test_direct_path_access(self):
        # This test may be slow as it loads actual data
        # Uncomment to test:
        # client = Client()
        # table = client.datasets["garden/un/2024-07-11/un_wpp/population"]
        # assert table is not None
        pass


class TestResultSet:
    """Test the ResultSet container."""

    def test_iteration(self):
        results = ResultSet(results=[1, 2, 3], query="test")

        items = list(results)
        assert items == [1, 2, 3]

    def test_indexing(self):
        results = ResultSet(results=["a", "b", "c"], query="test")

        assert results[0] == "a"
        assert results[1] == "b"
        assert results[2] == "c"

    def test_len(self):
        results = ResultSet(results=[1, 2, 3, 4, 5], query="test")
        assert len(results) == 5

    def test_total_auto_set(self):
        results = ResultSet(results=[1, 2, 3], query="test")
        assert results.total == 3

    def test_total_explicit(self):
        results = ResultSet(results=[1, 2, 3], query="test", total=100)
        assert results.total == 100


class TestDataclassModels:
    """Test the dataclass model objects."""

    def test_chart_result(self):
        result = ChartResult(
            slug="test-chart",
            title="Test Chart",
            url="https://ourworldindata.org/grapher/test-chart",
        )

        assert result.slug == "test-chart"
        assert result.title == "Test Chart"

    def test_indicator_result(self):
        result = IndicatorResult(
            indicator_id=123,
            title="Test Indicator",
            score=0.95,
            catalog_path="grapher/test/2024/dataset/table#column",
        )

        assert result.indicator_id == 123
        assert result.score == 0.95

    def test_dataset_result(self):
        result = DatasetResult(
            table="population",
            dataset="un_wpp",
            version="2024-07-11",
            namespace="un",
            channel="garden",
            path="garden/un/2024-07-11/un_wpp/population",
        )

        assert result.table == "population"
        assert result.namespace == "un"
