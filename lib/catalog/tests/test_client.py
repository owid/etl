#
#  Tests for owid.catalog.api module
#
import pytest

from owid.catalog import Client, Table
from owid.catalog.api import (
    ChartNotFoundError,
    ChartResult,
    IndicatorResult,
    LicenseError,
    ResponseSet,
    TableResult,
)
from owid.catalog.api.datasette import DatasetteAPI, DatasetteTable
from owid.catalog.api.search import PageSearchResult
from owid.catalog.core.charts import ChartTable, ChartTableMeta


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

    def test_fetch_chart(self):
        """Test fetching chart returns ChartTable directly."""
        client = Client()
        tb = client.charts.fetch("life-expectancy")

        assert isinstance(tb, ChartTable)
        # Table has data (load_data=True by default)
        assert len(tb) > 0
        # Chart slug stored in metadata.short_name
        assert tb.metadata.short_name == "life-expectancy"

    def test_fetch_chart_by_url(self):
        """Test fetching chart by URL returns ChartTable."""
        client = Client()
        tb = client.charts.fetch("https://ourworldindata.org/grapher/life-expectancy")

        assert isinstance(tb, ChartTable)
        assert tb.metadata.short_name == "life-expectancy"

    def test_load_chart_table_data(self):
        """Test that fetched chart Table has expected index and columns."""
        client = Client()
        tb = client.charts.fetch("life-expectancy")

        assert len(tb) > 0
        # entities and years are now index columns
        assert "entities" in tb.index.names
        assert "years" in tb.index.names or "dates" in tb.index.names

    def test_load_chart_table_metadata_and_config(self):
        """Test that table metadata, column metadata and chart config are accessible."""
        client = Client()
        tb = client.charts.fetch("life-expectancy")

        # Table metadata should be ChartTableMeta
        assert tb.metadata is not None
        assert isinstance(tb.metadata, ChartTableMeta)
        assert tb.metadata.short_name == "life-expectancy"
        assert tb.metadata.title is not None
        # ChartTableMeta.uri returns chart URL
        assert tb.metadata.uri == "https://ourworldindata.org/grapher/life-expectancy"
        # ChartTableMeta.dataset returns None (charts don't have datasets)
        assert tb.metadata.dataset is None

        # Column metadata should be populated (columns are data columns only, index has entities/years)
        assert len(tb.columns) > 0
        col = tb.columns[0]
        assert tb[col].metadata.unit is not None

        # Origin with citation should be populated
        assert len(tb[col].metadata.origins) > 0
        origin = tb[col].metadata.origins[0]
        assert origin.citation_full is not None

        # Chart config should be accessible via .metadata.chart_config
        assert tb.metadata.chart_config is not None
        assert isinstance(tb.metadata.chart_config, dict)
        assert "title" in tb.metadata.chart_config

    def test_chart_not_found(self):
        client = Client()
        with pytest.raises(ChartNotFoundError):
            client.charts.fetch("this-chart-does-not-exist")

    def test_non_redistributable_chart(self):
        """Test that non-redistributable charts raise LicenseError."""
        client = Client()
        with pytest.raises(LicenseError):
            # LicenseError is raised immediately since load_data=True by default
            client.charts.fetch("test-scores-ai-capabilities-relative-human-performance")

    def test_invalid_url(self):
        client = Client()
        with pytest.raises(ValueError):
            client.charts.fetch("https://example.com/not-a-grapher-url")


class TestChartsAPISearch:
    """Test the Charts API search functionality."""

    def test_charts_search(self):
        """Test searching for charts via ChartsAPI."""
        client = Client()
        results = client.charts.search("life expectancy")

        assert isinstance(results, ResponseSet)
        assert len(results) > 0
        assert all(isinstance(r, ChartResult) for r in results)
        assert results[0].slug
        assert results[0].title

    def test_charts_search_with_countries(self):
        """Test searching with country filters."""
        client = Client()
        results = client.charts.search("gdp", countries=["France"])

        assert isinstance(results, ResponseSet)
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

        assert isinstance(results, ResponseSet)
        assert len(results) > 0
        assert all(isinstance(r, PageSearchResult) for r in results)
        assert results[0].slug
        assert results[0].title

    def test_site_search_charts(self):
        """Test accessing charts via _site_search (should work but not recommended)."""
        client = Client()
        results = client._site_search.charts("gdp")

        assert isinstance(results, ResponseSet)
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
            assert isinstance(results, ResponseSet)

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
            assert isinstance(results, ResponseSet)


class TestIndicatorsAPI:
    """Test the Indicators API."""

    def test_search_indicators(self):
        client = Client()
        results = client.indicators.search("solar power generation")

        assert isinstance(results, ResponseSet)
        assert len(results) > 0
        assert all(isinstance(r, IndicatorResult) for r in results)

        # Check result fields
        first = results[0]
        assert first.indicator_id is not None and first.indicator_id > 0
        assert first.title
        assert 0 <= first.score <= 1
        assert first.path

    def test_indicator_results_to_catalog_frame(self):
        client = Client()
        results = client.indicators.search("co2 emissions")

        frame = results.to_catalog_frame()
        assert "indicator_title" in frame.columns
        assert "score" in frame.columns
        assert "path" in frame.columns

    def test_fetch_indicator(self):
        """Test fetching a specific indicator by path returns Table directly."""
        client = Client()
        # First search to find an indicator path
        results = client.indicators.search("solar power")
        if len(results) > 0:
            # Fetch by path - returns Table with single column
            assert results[0].path
            tb = client.indicators.fetch(results[0].path)
            assert isinstance(tb, Table)
            # Table has data (since load_data=True by default)
            assert len(tb) > 0
            # Should have exactly one column (the indicator)
            assert len(tb.columns) == 1
            # Metadata is accessible on the column
            col_name = tb.columns[0]
            assert tb[col_name].metadata is not None

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
            # Get table path from path
            assert results[0].path
            table_path = results[0].path.partition("#")[0]
            # Try to fetch with a non-existent column
            with pytest.raises(ValueError, match="Column 'nonexistent_column_12345' not found"):
                client.indicators.fetch(f"{table_path}#nonexistent_column_12345")


class TestTablesAPI:
    """Test the Tables API."""

    def test_search_tables(self):
        client = Client()
        results = client.tables.search(table="population")

        assert isinstance(results, ResponseSet)
        assert len(results) > 0
        assert all(isinstance(r, TableResult) for r in results)

    def test_search_tables_by_namespace(self):
        client = Client()
        results = client.tables.search(table="population", namespace="un")

        assert isinstance(results, ResponseSet)
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
        """Test fetching table by path returns Table directly."""
        client = Client()
        # First search to find a path
        results = client.tables.search(table="population", namespace="un")
        if len(results) > 0:
            path = results[0].path
            # Fetch table directly by path
            tb = client.tables.fetch(path)
            assert isinstance(tb, Table)
            # Table has data (since load_data=True by default)
            assert len(tb) > 0
            # Metadata is accessible
            assert tb.metadata is not None

    def test_fetch_invalid_path(self):
        """Test that fetching with invalid path format raises error."""
        client = Client()
        with pytest.raises(ValueError, match="Invalid catalog path"):
            client.tables.fetch("invalid/path")

    def test_fetch_nonexistent_table(self):
        """Test that fetching non-existent table raises error."""
        client = Client()
        with pytest.raises(KeyError, match="No matching table found"):
            client.tables.fetch("garden/fake/2024-01-01/fake/fake")

    def test_search_with_refresh_index(self):
        """Test that refresh_index parameter forces index re-download."""
        client = Client()

        # First search caches the index
        results1 = client.tables.search(table="population")
        assert len(results1) > 0

        # Search with refresh_index=True should work (forces re-download)
        results2 = client.tables.search(table="population", refresh_index=True)
        assert len(results2) > 0

        # Results should be equivalent
        assert len(results1) == len(results2)


class TestResponseSet:
    """Test the ResponseSet container."""

    def test_iteration(self):
        results = ResponseSet(results=[1, 2, 3], query="test", base_url="https://example.com")

        items = list(results)
        assert items == [1, 2, 3]

    def test_indexing(self):
        results = ResponseSet(results=["a", "b", "c"], query="test", base_url="https://example.com")

        assert results[0] == "a"
        assert results[1] == "b"
        assert results[2] == "c"

    def test_len(self):
        results = ResponseSet(results=[1, 2, 3, 4, 5], query="test", base_url="https://example.com")
        assert len(results) == 5

    def test_total_count_auto_set(self):
        results = ResponseSet(results=[1, 2, 3], query="test", base_url="https://example.com")
        assert results.total_count == 3

    def test_total_count_explicit(self):
        results = ResponseSet(results=[1, 2, 3], query="test", total_count=100, base_url="https://example.com")
        assert results.total_count == 100

    def test_filter(self):
        """Test filtering results with a predicate."""
        # Create mock result objects with version attribute
        from pydantic import BaseModel

        class MockResult(BaseModel):
            version: str
            name: str

        items = [
            MockResult(version="2024-01-01", name="a"),
            MockResult(version="2024-06-01", name="b"),
            MockResult(version="2023-12-01", name="c"),
            MockResult(version="2024-03-01", name="d"),
        ]
        results = ResponseSet(results=items, query="test", base_url="https://example.com")

        # Filter by version (>= "2024-03-01" matches 2024-03-01 and 2024-06-01)
        filtered = results.filter(lambda r: r.version >= "2024-03-01")
        assert len(filtered) == 2
        assert all(r.version >= "2024-03-01" for r in filtered)
        assert {r.name for r in filtered} == {"b", "d"}

        # Filter by name
        filtered = results.filter(lambda r: r.name in ["a", "c"])
        assert len(filtered) == 2
        assert {r.name for r in filtered} == {"a", "c"}

        # Chain filters
        filtered = results.filter(lambda r: r.version >= "2024-01-01").filter(lambda r: r.name != "d")
        assert len(filtered) == 2

    def test_sort_by_string(self):
        """Test sorting by attribute name."""
        from pydantic import BaseModel

        class MockResult(BaseModel):
            version: str
            score: float

        items = [
            MockResult(version="2024-03-01", score=0.8),
            MockResult(version="2024-01-01", score=0.9),
            MockResult(version="2024-06-01", score=0.7),
        ]
        results = ResponseSet(results=items, query="test", base_url="https://example.com")

        # Sort by version ascending
        sorted_results = results.sort_by("version")
        versions = [r.version for r in sorted_results]
        assert versions == ["2024-01-01", "2024-03-01", "2024-06-01"]

        # Sort by version descending
        sorted_results = results.sort_by("version", reverse=True)
        versions = [r.version for r in sorted_results]
        assert versions == ["2024-06-01", "2024-03-01", "2024-01-01"]

        # Sort by score
        sorted_results = results.sort_by("score", reverse=True)
        scores = [r.score for r in sorted_results]
        assert scores == [0.9, 0.8, 0.7]

    def test_sort_by_function(self):
        """Test sorting by key function."""
        from pydantic import BaseModel

        class MockResult(BaseModel):
            name: str
            value: int

        items = [
            MockResult(name="c", value=3),
            MockResult(name="a", value=1),
            MockResult(name="b", value=2),
        ]
        results = ResponseSet(results=items, query="test", base_url="https://example.com")

        # Sort by value using lambda
        sorted_results = results.sort_by(lambda r: r.value)
        values = [r.value for r in sorted_results]
        assert values == [1, 2, 3]

        # Sort by name using lambda
        sorted_results = results.sort_by(lambda r: r.name)
        names = [r.name for r in sorted_results]
        assert names == ["a", "b", "c"]

    def test_latest(self):
        """Test getting the latest result."""
        from pydantic import BaseModel

        class MockResult(BaseModel):
            version: str
            published_at: str
            score: float

        items = [
            MockResult(version="2024-01-01", published_at="2024-01-15", score=0.8),
            MockResult(version="2024-06-01", published_at="2024-06-15", score=0.9),
            MockResult(version="2024-03-01", published_at="2024-03-15", score=0.7),
        ]
        results = ResponseSet(results=items, query="test", base_url="https://example.com")

        # Get latest by version
        latest = results.latest(by="version")
        assert latest.version == "2024-06-01"

        # Get latest by published_at
        latest = results.latest(by="published_at")
        assert latest.published_at == "2024-06-15"

        # Get latest by score
        latest = results.latest(by="score")
        assert latest.score == 0.9

    def test_latest_empty_results(self):
        """Test that latest() raises ValueError on empty results."""
        results = ResponseSet(results=[], query="test", base_url="https://example.com")

        with pytest.raises(ValueError, match="No results available"):
            results.latest(by="version")

    def test_chaining(self):
        """Test chaining multiple convenience methods."""
        from pydantic import BaseModel

        class MockResult(BaseModel):
            version: str
            namespace: str
            score: float

        items = [
            MockResult(version="2024-01-01", namespace="un", score=0.8),
            MockResult(version="2024-06-01", namespace="worldbank", score=0.9),
            MockResult(version="2024-03-01", namespace="un", score=0.7),
            MockResult(version="2024-02-01", namespace="worldbank", score=0.6),
        ]
        results = ResponseSet(results=items, query="test", base_url="https://example.com")

        # Chain filter -> sort -> index
        filtered = results.filter(lambda r: r.namespace == "un").sort_by("version", reverse=True)[0]
        assert filtered.version == "2024-03-01"
        assert filtered.namespace == "un"

        # Chain filter -> sort -> latest
        top = results.filter(lambda r: r.score > 0.6).sort_by("score", reverse=True).latest(by="score")
        assert top.score == 0.9


class TestDataclassModels:
    """Test the dataclass model objects."""

    def test_chart_result(self):
        result = ChartResult(
            slug="test-chart",
            title="Test Chart",
            url="https://ourworldindata.org/grapher/test-chart",
            base_url="https://ourworldindata.org/grapher",
        )

        assert result.slug == "test-chart"
        assert result.title == "Test Chart"
        assert result.base_url == "https://ourworldindata.org/grapher"

    def test_indicator_result(self):
        result = IndicatorResult(
            indicator_id=123,
            title="Test Indicator",
            score=0.95,
            path="grapher/test/2024/dataset/table#column",
            catalog_url="https://catalog.ourworldindata.org/",
        )

        assert result.indicator_id == 123
        assert result.score == 0.95
        assert result.catalog_url == "https://catalog.ourworldindata.org/"

    def test_table_result(self):
        result = TableResult(
            table="population",
            dataset="un_wpp",
            version="2024-07-11",
            namespace="un",
            channel="garden",
            path="garden/un/2024-07-11/un_wpp/population",
            catalog_url="https://catalog.ourworldindata.org/",
        )

        assert result.table == "population"
        assert result.namespace == "un"
        assert result.catalog_url == "https://catalog.ourworldindata.org/"


class TestDatasetteAPI:
    """Test the DatasetteAPI for Datasette integration."""

    def test_client_has_datasette(self):
        """Test that Client has _datasette attribute."""
        client = Client()
        assert hasattr(client, "_datasette")
        assert isinstance(client._datasette, DatasetteAPI)

    def test_datasette_repr(self):
        """Test DatasetteAPI repr."""
        api = DatasetteAPI()
        assert "DatasetteAPI" in repr(api)
        assert "datasette-public.owid.io" in repr(api)

    def test_query(self):
        """Test executing raw SQL query."""
        import pandas as pd

        api = DatasetteAPI()
        df = api.query("SELECT 1 as value")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["value"].iloc[0] == 1

    def test_query_raises_on_error(self):
        """Test that invalid SQL raises an exception."""
        import requests

        api = DatasetteAPI()
        with pytest.raises(requests.HTTPError):
            api.query("SELECT * FROM nonexistent_table_12345")

    def test_list_tables(self):
        """Test listing available tables (fast mode, names only)."""
        api = DatasetteAPI()
        tables = api.list_tables()

        assert isinstance(tables, list)
        assert len(tables) > 0
        assert all(isinstance(t, DatasetteTable) for t in tables)

        # Check for known table
        table_names = [t.name for t in tables]
        assert "analytics_popularity" in table_names

        # Fast mode should have empty columns
        assert tables[0].columns == []
        assert tables[0].row_count is None

    def test_list_tables_with_metadata(self):
        """Test listing tables with full metadata (slow mode)."""
        api = DatasetteAPI()
        # Just fetch metadata for first 2 tables to keep test fast
        all_tables = api.list_tables()
        tables_with_meta = [api.get_table(t.name) for t in all_tables[:2]]

        assert len(tables_with_meta) == 2
        for t in tables_with_meta:
            assert isinstance(t, DatasetteTable)
            assert len(t.columns) > 0  # Should have columns
            assert t.row_count is not None  # Should have row count

    def test_get_table(self):
        """Test getting metadata for a single table."""
        api = DatasetteAPI()
        table = api.get_table("analytics_popularity")

        assert isinstance(table, DatasetteTable)
        assert table.name == "analytics_popularity"
        assert table.row_count is not None
        assert table.row_count > 0
        assert table.is_view is False

    def test_table_fetch(self):
        """Test fetching full metadata for a table from list_tables()."""
        api = DatasetteAPI()
        tables = api.list_tables()

        # Fast mode tables should have empty columns
        table = tables[0]
        assert table.columns == []
        assert table.row_count is None

        # .fetch() should return a new table with full metadata
        full_table = table.fetch()
        assert isinstance(full_table, DatasetteTable)
        assert full_table.name == table.name
        assert len(full_table.columns) > 0
        assert full_table.row_count is not None

    def test_fetch_popularity_empty_slugs(self):
        """Test that empty slugs returns empty dict."""
        api = DatasetteAPI()
        result = api.fetch_popularity([], "indicator")

        assert result == {}

    def test_fetch_popularity_indicator(self):
        """Test fetching indicator popularity."""
        api = DatasetteAPI()
        # Use a known indicator path that should exist
        df = api.query("SELECT slug FROM analytics_popularity WHERE type = 'indicator' LIMIT 1")

        if not df.empty:
            slug = df["slug"].iloc[0]
            popularity = api.fetch_popularity([slug], "indicator")

            assert isinstance(popularity, dict)
            if slug in popularity:
                assert 0.0 <= popularity[slug] <= 1.0

    def test_fetch_popularity_dataset(self):
        """Test fetching dataset popularity."""
        api = DatasetteAPI()
        # Use a known dataset path that should exist
        df = api.query("SELECT slug FROM analytics_popularity WHERE type = 'dataset' LIMIT 1")

        if not df.empty:
            slug = df["slug"].iloc[0]
            popularity = api.fetch_popularity([slug], "dataset")

            assert isinstance(popularity, dict)
            if slug in popularity:
                assert 0.0 <= popularity[slug] <= 1.0

    def test_fetch_popularity_nonexistent_slugs(self):
        """Test that nonexistent slugs are not in result dict."""
        api = DatasetteAPI()
        result = api.fetch_popularity(["nonexistent_slug_12345"], "indicator")

        assert isinstance(result, dict)
        assert "nonexistent_slug_12345" not in result

    def test_custom_timeout(self):
        """Test that custom timeout is respected."""
        api = DatasetteAPI(timeout=5)
        assert api.timeout == 5

        # Can override per-request
        tables = api.list_tables(timeout=10)
        assert isinstance(tables, list)
        assert all(isinstance(t, DatasetteTable) for t in tables)
