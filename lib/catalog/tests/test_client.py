#
#  Tests for owid.catalog.api module
#
import pytest

from owid.catalog import Client
from owid.catalog.api import (
    ChartNotFoundError,
    ChartResult,
    IndicatorResult,
    LicenseError,
    PageSearchResult,
    ResultSet,
    TableResult,
)


class TestClient:
    """Test the unified Client class."""

    def test_client_has_all_apis(self):
        client = Client()
        assert hasattr(client, "charts")
        assert hasattr(client, "_site_search")
        assert hasattr(client, "indicators")
        assert hasattr(client, "tables")

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
        meta = client.charts.get_metadata("life-expectancy")

        assert meta is not None
        assert isinstance(meta, dict)
        assert "columns" in meta

    def test_get_chart_config(self):
        client = Client()
        config = client.charts.get_config("life-expectancy")

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
        assert all(isinstance(r, ChartResult) for r in results)
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

    def test_site_search_charts_limit_warning(self):
        """Test that warning is raised when limit > 100 for charts."""
        import warnings

        client = Client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            results = client._site_search.charts("gdp", limit=150)

            # Check warning was raised
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "Max allowed of result items is 100" in str(w[0].message)

            # Check that limit was clamped to 100
            assert isinstance(results, ResultSet)

    def test_site_search_pages_limit_warning(self):
        """Test that warning is raised when limit > 100 for pages."""
        import warnings

        client = Client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            results = client._site_search.pages("climate", limit=200)

            # Check warning was raised
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "Max allowed of result items is 100" in str(w[0].message)

            # Check that limit was clamped to 100
            assert isinstance(results, ResultSet)


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
        assert first.indicator_id is not None and first.indicator_id > 0
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

    def test_fetch_indicator(self):
        """Test fetching a specific indicator by URI."""
        client = Client()
        # First search to find an indicator URI
        results = client.indicators.search("solar power")
        if len(results) > 0:
            # Fetch by URI (catalog_path)
            indicator = client.indicators.fetch(results[0].catalog_path)
            assert indicator.column_name
            assert indicator.title
            assert indicator.catalog_path == results[0].catalog_path
            # Test lazy loading - table should not be loaded yet
            assert indicator._table is None
            # Test data access - this triggers lazy loading
            variable = indicator.data
            assert variable is not None
            # After accessing data, table should be cached
            assert indicator._table is not None

    def test_fetch_indicator_invalid_format(self):
        """Test that invalid path format raises error."""
        client = Client()
        with pytest.raises(ValueError, match="Invalid indicator path format"):
            # Missing # separator
            client.indicators.fetch("grapher/un/2024/pop/population")

    def test_fetch_indicator_missing_column(self):
        """Test that fetching non-existent column raises error."""
        client = Client()
        # Search to get a valid table path
        results = client.indicators.search("solar power")
        if len(results) > 0:
            # Get table path from catalog_path
            table_path = results[0].catalog_path.partition("#")[0]
            # Try to fetch with a non-existent column
            with pytest.raises(ValueError, match="Column 'nonexistent_column_12345' not found"):
                client.indicators.fetch(f"{table_path}#nonexistent_column_12345")

    def test_get_data(self):
        """Test get_data convenience method."""
        client = Client()
        # Search to get an indicator path
        results = client.indicators.search("solar power")
        if len(results) > 0:
            path = results[0].catalog_path
            # Use get_data - should return Variable directly
            variable = client.indicators.get_data(path)
            assert variable is not None
            # Should be equivalent to fetch().data
            variable2 = client.indicators.fetch(path).data
            assert variable.name == variable2.name


class TestTablesAPI:
    """Test the Tables API."""

    def test_search_tables(self):
        client = Client()
        results = client.tables.search(table="population")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(isinstance(r, TableResult) for r in results)

    def test_search_tables_by_namespace(self):
        client = Client()
        results = client.tables.search(table="population", namespace="un")

        assert isinstance(results, ResultSet)
        assert len(results) > 0
        assert all(r.namespace == "un" for r in results)

    def test_table_results_to_catalog_frame(self):
        client = Client()
        results = client.tables.search(table="population", namespace="un")

        frame = results.to_catalog_frame()
        assert "table" in frame.columns
        assert "namespace" in frame.columns
        assert "path" in frame.columns

        # Should be loadable
        assert hasattr(frame, "load")

    def test_fetch_table(self):
        """Test fetching table metadata by path."""
        client = Client()
        # First search to find a path
        results = client.tables.search(table="population", namespace="un")
        if len(results) > 0:
            path = results[0].path
            # Now fetch by path (should return same metadata)
            table_result = client.tables.fetch(path)
            assert isinstance(table_result, TableResult)
            assert table_result.path == path
            assert table_result.table
            assert table_result.dataset
            assert table_result.namespace == "un"

    def test_fetch_invalid_path(self):
        """Test that fetching with invalid path format raises error."""
        client = Client()
        with pytest.raises(ValueError, match="Invalid path format"):
            client.tables.fetch("invalid/path")

    def test_fetch_nonexistent_table(self):
        """Test that fetching non-existent table raises error."""
        client = Client()
        with pytest.raises(ValueError, match="not found"):
            client.tables.fetch("garden/fake/2024-01-01/fake/fake")

    def test_get_data(self):
        """Test get_data convenience method."""
        client = Client()
        # Search to get a table path
        results = client.tables.search(table="population", namespace="un")
        if len(results) > 0:
            path = results[0].path
            # Use get_data - should return Table directly
            table = client.tables.get_data(path)
            assert table is not None
            assert len(table) > 0
            # Should be equivalent to fetch().data
            table2 = client.tables.fetch(path).data
            assert table.m.short_name == table2.m.short_name

    def test_backwards_compatibility_datasets(self):
        """Test that client.datasets still works (backwards compatibility)."""
        client = Client()

        # Should work via datasets attribute
        results = client.datasets.search(table="population")
        assert len(results) > 0

        # Verify it's the same as tables
        assert client.datasets is client.tables


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

    def test_table_result(self):
        result = TableResult(
            table="population",
            dataset="un_wpp",
            version="2024-07-11",
            namespace="un",
            channel="garden",
            path="garden/un/2024-07-11/un_wpp/population",
        )

        assert result.table == "population"
        assert result.namespace == "un"
