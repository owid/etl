#
#  Tests for owid.catalog.core.charts module
#
import pandas as pd

from owid.catalog.core.charts import ChartTable, ChartTableMeta


class TestChartTableMeta:
    """Test the ChartTableMeta class."""

    def test_default_values(self):
        """Test ChartTableMeta has correct defaults."""
        meta = ChartTableMeta()
        assert meta.short_name is None
        assert meta.title is None
        assert meta.description is None
        assert meta.dataset is None

    def test_uri_with_short_name(self):
        """Test uri property returns grapher URL when short_name is set."""
        meta = ChartTableMeta(short_name="life-expectancy")
        assert meta.uri == "https://ourworldindata.org/grapher/life-expectancy"

    def test_uri_without_short_name(self):
        """Test uri property returns None when short_name is not set."""
        meta = ChartTableMeta()
        assert meta.uri is None

    def test_dataset_always_none(self):
        """Test dataset field is always None for chart tables."""
        meta = ChartTableMeta(short_name="test", title="Test Chart")
        assert meta.dataset is None


class TestChartTable:
    """Test the ChartTable class."""

    def test_create_empty(self):
        """Test creating an empty ChartTable."""
        tb = ChartTable()
        assert isinstance(tb, ChartTable)
        assert tb.metadata.chart_config == {}

    def test_create_with_config(self):
        """Test creating ChartTable with chart_config."""
        config = {"title": "Test Chart", "subtitle": "A test"}
        meta = ChartTableMeta(chart_config=config)
        tb = ChartTable(metadata=meta)
        assert tb.metadata.chart_config == config
        assert tb.metadata.chart_config["title"] == "Test Chart"

    def test_create_from_dataframe(self):
        """Test creating ChartTable from DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        config = {"title": "Test"}
        meta = ChartTableMeta(chart_config=config)
        tb = ChartTable(df, metadata=meta)

        assert len(tb) == 3
        assert list(tb.columns) == ["a", "b"]
        assert tb.metadata.chart_config == config

    def test_chart_config_setter(self):
        """Test setting chart_config after creation."""
        tb = ChartTable()
        tb.metadata.chart_config = {"title": "Updated"}
        assert tb.metadata.chart_config["title"] == "Updated"

    def test_slicing_preserves_type(self):
        """Test that slicing returns ChartTable."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        meta = ChartTableMeta(chart_config={"title": "Test"})
        tb = ChartTable(df, metadata=meta)

        # Note: pandas operations may not always preserve subclass type
        # This test documents current behavior
        sliced = tb[["a"]]
        assert isinstance(sliced, ChartTable)

    def test_constructor_property(self):
        """Test _constructor returns ChartTable type."""
        tb = ChartTable()
        assert tb._constructor == ChartTable
