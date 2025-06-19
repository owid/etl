"""Tests for etl.collection.core.expand module.

This module tests the expand_config function and related functionality
used to automatically generate collection configurations from multi-dimensional data.
"""

import pytest
from owid.catalog import Table, Variable
from owid.catalog.meta import DatasetMeta, VariableMeta

from etl.collection.core.expand import expand_config
from etl.collection.exceptions import MissingDimensionalIndicatorError
from etl.collection.utils import INDICATORS_SLUG


def create_test_table():
    """Create a test table with multi-dimensional indicators for testing.

    Contains two indicators, with one dimension.

    Indicators: deaths, cases
    Dimensions:
        - sex: female, male

    """
    # Create sample data with dimensions
    data = {
        "country": ["USA", "USA", "USA", "USA", "CAN", "CAN", "CAN", "CAN"],
        "year": [2020, 2020, 2021, 2021, 2020, 2020, 2021, 2021],
        "deaths__sex_male": [100, 100, 110, 110, 50, 50, 55, 55],
        "deaths__sex_female": [80, 80, 90, 90, 40, 40, 45, 45],
        "cases__sex_male": [1000, 1000, 1100, 1100, 500, 500, 550, 550],
        "cases__sex_female": [800, 800, 900, 900, 400, 400, 450, 450],
    }

    # Create table
    tb = Table(data, short_name="test_table")

    # Add metadata for dimensional indicators
    columns = [col for col in tb.columns if col not in ["country", "year"]]
    for col in columns:
        tb[col] = Variable(
            tb[col],
            name=col,
            metadata=VariableMeta(
                original_short_name="deaths" if "deaths" in col else "cases",
                dimensions={"sex": "female" if "female" in col else "male"},
            ),
        )

    return tb


def create_multi_dimension_table():
    """Create a table with multiple dimensions for more complex testing.

    Contains two indicators, with two dimensions 'sex' and 'age'.

    Indicators: deaths, cases
    Dimensions:
        - sex: female, male
        - age: young, old

    """
    data = {
        "country": ["USA", "USA", "USA", "USA"],
        "year": [2020, 2020, 2020, 2020],
        "deaths__sex_male__age_young": [50, 50, 50, 50],
        "deaths__sex_male__age_old": [60, 60, 60, 60],
        "deaths__sex_female__age_young": [40, 40, 40, 40],
        "deaths__sex_female__age_old": [50, 50, 50, 50],
    }

    tb = Table(data, short_name="multi_dim_table")

    for col in data.keys():
        if col.startswith("deaths"):
            sex = "female" if "female" in col else "male"
            age = "young" if "young" in col else "old"
            tb[col] = Variable(
                tb[col],
                name=col,
                metadata=VariableMeta(original_short_name="deaths", dimensions={"sex": sex, "age": age}),
            )

    return tb


def create_table_with_metadata():
    """Create a table with proper dataset metadata for testing expand_path_mode."""
    tb = create_test_table()

    # Create a dataset with metadata
    # ds = Dataset.create_empty("/tmp/test_dataset")

    ds_metadata = DatasetMeta(
        channel="garden",
        namespace="test",
        version="2024",
        short_name="test_dataset",
    )
    tb.metadata.dataset = ds_metadata

    return tb


class TestExpandConfig:
    """Test suite for expand_config function."""

    def test_basic_single_indicator(self):
        """Test expand_config with a single indicator.

        Table contains indicators deaths and cases. Both with dimension sex.
        We expand indicator `deaths`, resulting into 1 x 2 = 2 views.
        """
        tb = create_test_table()

        # Test with single indicator
        config = expand_config(tb, indicator_names="deaths")

        # Check dimensions
        assert "dimensions" in config
        assert len(config["dimensions"]) == 1
        assert config["dimensions"][0]["slug"] == "sex"
        assert len(config["dimensions"][0]["choices"]) == 2

        # Check views
        assert "views" in config
        assert len(config["views"]) == 2  # male and female

        # Verify view structure
        for view in config["views"]:
            assert "dimensions" in view
            assert "indicators" in view
            assert "sex" in view["dimensions"]
            assert view["dimensions"]["sex"] in ["male", "female"]
            assert "y" in view["indicators"]

    def test_multiple_indicators(self):
        """Test expand_config with multiple indicators.

        Table contains indicators deaths and cases. Both with dimension sex.
        We expand both indicators, resulting into 2 x 2 = 4 views
        """
        tb = create_test_table()

        config = expand_config(tb, indicator_names=["deaths", "cases"])

        # Should have indicator dimension plus sex dimension
        assert len(config["dimensions"]) == 2
        dimension_slugs = [d["slug"] for d in config["dimensions"]]
        assert INDICATORS_SLUG in dimension_slugs
        assert "sex" in dimension_slugs

        # Should have 4 views (2 indicators × 2 sex values)
        assert len(config["views"]) == 4

    def test_indicator_as_dimension_single(self):
        """Test expand_config with indicator_as_dimension=True for single indicator.

        Test it with single indicator expansion, but keeping indicator as dimension."""
        tb = create_test_table()

        config = expand_config(tb, indicator_names="deaths", indicator_as_dimension=True)

        # Should have both indicator and sex dimensions
        assert len(config["dimensions"]) == 2
        dimension_slugs = [d["slug"] for d in config["dimensions"]]
        assert INDICATORS_SLUG in dimension_slugs
        assert "sex" in dimension_slugs

    def test_dimensions_list_parameter(self):
        """Test expand_config with dimensions as list to control order.

        Table contains indicators deaths and cases. Both with dimension sex.
        Expand indicator `deaths` with dimensions in specific order.
        """
        tb = create_multi_dimension_table()

        config = expand_config(tb, indicator_names="deaths", dimensions=["age", "sex"])

        # Check dimension order matches input
        dimension_slugs = [d["slug"] for d in config["dimensions"]]
        assert dimension_slugs == ["age", "sex"]

    def test_dimensions_dict_parameter(self):
        """Test expand_config with dimensions as dict to filter values.

        Table contains indicators deaths and cases. Both with dimension sex.
        Expand indicator `deaths`, only sex='male' and all age values.
        """
        tb = create_multi_dimension_table()

        config = expand_config(tb, indicator_names="deaths", dimensions={"sex": ["male"], "age": "*"})

        # Check sex dimension only has male
        sex_dim = next(d for d in config["dimensions"] if d["slug"] == "sex")
        assert len(sex_dim["choices"]) == 1
        assert sex_dim["choices"][0]["slug"] == "male"

        # Check age dimension has all values
        age_dim = next(d for d in config["dimensions"] if d["slug"] == "age")
        assert len(age_dim["choices"]) == 2

    def test_common_view_config(self):
        """Test expand_config with common_view_config parameter.

        Test that I can pass common configuration to all exported views.
        """
        tb = create_test_table()
        common_config = {"chartTypes": ["LineChart"], "hasMapTab": True}

        config = expand_config(tb, indicator_names="deaths", common_view_config=common_config)

        # All views should have the common config
        for view in config["views"]:
            assert "config" in view
            assert view["config"]["chartTypes"] == ["LineChart"]
            assert view["config"]["hasMapTab"] is True

    def test_custom_indicators_slug(self):
        """Test expand_config with custom indicators_slug."""
        tb = create_test_table()

        config = expand_config(tb, indicator_names=["deaths", "cases"], indicators_slug="metric")

        # Indicator dimension should use custom slug
        indicator_dim = next(d for d in config["dimensions"] if d["slug"] == "metric")
        assert indicator_dim is not None
        assert len(indicator_dim["choices"]) == 2

    def test_expand_path_modes(self):
        """Test different expand_path_mode values."""
        tb = create_table_with_metadata()

        # Test table mode (default)
        config_table = expand_config(tb, indicator_names="deaths", expand_path_mode="table")
        indicator_path_table = config_table["views"][0]["indicators"]["y"]
        assert indicator_path_table == "test_table#deaths__sex_male"

        # Test dataset mode
        config_dataset = expand_config(tb, indicator_names="deaths", expand_path_mode="dataset")
        indicator_path_dataset = config_dataset["views"][0]["indicators"]["y"]
        assert indicator_path_dataset == "test_dataset/test_table#deaths__sex_male"

        # Test full mode
        config_full = expand_config(tb, indicator_names="deaths", expand_path_mode="full")
        indicator_path_full = config_full["views"][0]["indicators"]["y"]
        assert indicator_path_full == "garden/test/2024/test_dataset/test_table#deaths__sex_male"

    def test_auto_indicator_detection(self):
        """Test expand_config auto-detects single indicator when none specified."""
        tb = create_test_table()

        # Remove one set of indicators to have only one indicator type
        tb_single = tb.loc[:, ["country", "year", "deaths__sex_male", "deaths__sex_female"]].copy()

        config = expand_config(tb_single)  # No indicator_names specified

        # Should work with auto-detected indicator
        assert len(config["views"]) == 2
        for view in config["views"]:
            assert "y" in view["indicators"]

    def test_missing_indicator_error(self):
        """Test error when requesting non-existent indicator."""
        tb = create_test_table()

        with pytest.raises(ValueError, match="Indicators .* not found"):
            expand_config(tb, indicator_names="nonexistent")

    def test_multiple_indicators_no_name_error(self):
        """Test error when multiple indicators exist but none specified."""
        tb = create_test_table()

        with pytest.raises(ValueError, match="multiple indicators.*no.*indicator_name.*provided"):
            expand_config(tb)  # Should fail since both deaths and cases exist

    def test_missing_dimension_error(self):
        """Test error when specifying non-existent dimension."""
        tb = create_test_table()

        with pytest.raises(ValueError, match="Missing items"):
            expand_config(tb, indicator_names="deaths", dimensions=["nonexistent"])

    def test_dimension_value_validation(self):
        """Test validation of dimension values."""
        tb = create_test_table()

        with pytest.raises(ValueError, match="Unexpected items"):
            expand_config(tb, indicator_names="deaths", dimensions={"sex": ["male", "nonexistent"]})

    def test_empty_table_handling(self):
        """Test handling of table with no dimensional indicators."""
        data = {"country": ["USA", "CAN"], "year": [2020, 2020], "simple_indicator": [100, 50]}
        tb = Table(data, short_name="simple_table")

        with pytest.raises(MissingDimensionalIndicatorError):
            expand_config(tb)


class TestCollectionConfigExpander:
    """Test suite for CollectionConfigExpander class methods."""

    def test_dimension_names_property(self):
        """Test dimension_names property returns correct dimension columns."""
        from etl.collection.core.expand import CollectionConfigExpander

        tb = create_test_table()
        expander = CollectionConfigExpander(tb, "indicator", "deaths")

        # Should return dimensions excluding short_name
        assert expander.dimension_names == ["sex"]

    def test_table_properties(self):
        """Test table name and dataset properties."""
        from etl.collection.core.expand import CollectionConfigExpander

        tb = create_table_with_metadata()
        expander = CollectionConfigExpander(tb, "indicator", "deaths")

        assert expander.table_name == "test_table"
        assert expander.dataset_name == "test_dataset"
        assert expander.dataset_uri == "garden/test/2024/test_dataset"
