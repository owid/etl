"""Tests for etl.collection.core.create module.

This module tests the main collection creation function that combines
various collection utilities to create complete collections and explorers
from configurations and optional table data.
"""

from typing import Callable, cast
from unittest.mock import Mock, patch

import pytest
from owid.catalog import Table, Variable
from owid.catalog.meta import VariableMeta

from etl.collection.core.create import (
    _get_expand_path_mode,
    _rename_choices,
    create_collection,
    create_collection_single_table,
)
from etl.collection.explorer import Explorer
from etl.collection.model.core import Collection
from etl.collection.model.dimension import Dimension, DimensionChoice


def create_test_config():
    """Create a basic test configuration for collections."""
    return {
        "title": {"title": "Test Collection", "title_variant": "Test Collection Variant"},
        "default_selection": ["country"],
        "dimensions": [
            {
                "slug": "country",
                "name": "Country",
                "choices": [{"slug": "usa", "name": "United States"}, {"slug": "can", "name": "Canada"}],
            }
        ],
        "views": [{"dimensions": {"country": "usa"}, "indicators": {"y": [{"catalogPath": "test#indicator1"}]}}],
    }


def create_test_explorer_config():
    """Create a basic test configuration for explorers."""
    config = create_test_config()
    config["config"] = {"hasMapTab": True, "chartTypes": ["LineChart", "DiscreteBar"]}
    return config


def create_test_table():
    """Create a test table with multi-dimensional indicators."""
    data = {
        "country": ["USA", "USA", "CAN", "CAN"],
        "year": [2020, 2021, 2020, 2021],
        "deaths__sex_male": [100, 110, 50, 55],
        "deaths__sex_female": [80, 90, 40, 45],
    }

    tb = Table(data, short_name="test_table")

    # Add metadata for dimensional indicators
    for col in ["deaths__sex_male", "deaths__sex_female"]:
        tb[col] = Variable(
            tb[col],
            name=col,
            metadata=VariableMeta(
                original_short_name="deaths", dimensions={"sex": "male" if "male" in col else "female"}
            ),
        )

    return tb


def create_test_table_2():
    """Create a second test table with different dimensional indicators."""
    data = {
        "country": ["USA", "USA", "CAN", "CAN"],
        "year": [2020, 2021, 2020, 2021],
        "cases__age_young": [200, 220, 100, 110],
        "cases__age_old": [150, 170, 75, 85],
    }

    tb = Table(data, short_name="test_table_2")

    # Add metadata for dimensional indicators
    for col in ["cases__age_young", "cases__age_old"]:
        tb[col] = Variable(
            tb[col],
            name=col,
            metadata=VariableMeta(
                original_short_name="cases", dimensions={"age": "young" if "young" in col else "old"}
            ),
        )

    return tb


def create_multiple_test_tables():
    """Create a list of test tables for multi-table testing."""
    return [create_test_table(), create_test_table_2()]


class TestCreateCollection:
    """Test suite for create_collection function."""

    def test_create_collection_basic_no_table(self):
        """Test basic collection creation without table input."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
            mock_collection = Mock(spec=Collection)
            mock_create.return_value = mock_collection

            result = create_collection_single_table(
                config_yaml=config, dependencies=dependencies, catalog_path=catalog_path
            )

            # Verify the function was called correctly
            mock_create.assert_called_once_with(config=config, dependencies=dependencies, catalog_path=catalog_path)
            assert result == mock_collection

    def test_create_collection_with_table_input(self):
        """Test collection creation with table input for auto-expansion."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"
        tb = create_test_table()

        with patch("etl.collection.core.create.has_duplicate_table_names") as mock_has_duplicates:
            with patch("etl.collection.core.create.expand_config") as mock_expand:
                with patch("etl.collection.core.create.combine_config_dimensions") as mock_combine:
                    with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
                        mock_has_duplicates.return_value = False

                        # Mock expand_config to return auto-generated config
                        mock_expand.return_value = {
                            "dimensions": [
                                {
                                    "slug": "sex",
                                    "name": "Sex",
                                    "choices": [{"slug": "male", "name": "Male"}, {"slug": "female", "name": "Female"}],
                                }
                            ],
                            "views": [
                                {"dimensions": {"sex": "male"}, "indicators": {"y": "test_table#deaths__sex_male"}}
                            ],
                        }

                        # Mock combine_config_dimensions to return combined dimensions
                        mock_combine.return_value = [
                            {
                                "slug": "country",
                                "name": "Country",
                                "choices": [{"slug": "usa", "name": "United States"}],
                            }
                        ]

                        mock_collection = Mock(spec=Collection)
                        mock_create.return_value = mock_collection

                        result = create_collection_single_table(
                            config_yaml=config,
                            dependencies=dependencies,
                            catalog_path=catalog_path,
                            tb=tb,
                            indicator_names="deaths",
                        )

                        # Verify expand_config was called with table
                        mock_expand.assert_called_once_with(
                            tb=tb,
                            indicator_names="deaths",
                            dimensions=None,
                            common_view_config=None,
                            indicators_slug=None,
                            indicator_as_dimension=False,
                            expand_path_mode="table",
                        )

                        # Verify combine_config_dimensions was called
                        mock_combine.assert_called_once()

                        # Verify create_collection_from_config was called
                        mock_create.assert_called_once()

                        assert result == mock_collection

    def test_create_collection_explorer_mode(self):
        """Test collection creation in explorer mode."""
        config = create_test_explorer_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
            mock_explorer = Mock(spec=Explorer)
            mock_create.return_value = mock_explorer

            result = create_collection_single_table(
                config_yaml=config, dependencies=dependencies, catalog_path=catalog_path, explorer=True
            )

            # Verify explorer-specific call
            mock_create.assert_called_once_with(
                config=config,
                dependencies=dependencies,
                catalog_path=catalog_path,
                validate_schema=False,
                explorer=True,
            )
            assert result == mock_explorer

    def test_create_collection_with_choice_renames_dict(self):
        """Test collection creation with choice renaming using dictionary."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"
        choice_renames = {"country": {"usa": "United States of America", "can": "Canada"}}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
            # Create a mock collection with dimensions and choices
            mock_collection = Mock(spec=Collection)

            # Create mock dimensions with choices
            usa_choice = Mock(spec=DimensionChoice)
            usa_choice.slug = "usa"
            usa_choice.name = "United States"

            can_choice = Mock(spec=DimensionChoice)
            can_choice.slug = "can"
            can_choice.name = "Canada"

            country_dim = Mock(spec=Dimension)
            country_dim.slug = "country"
            country_dim.choices = [usa_choice, can_choice]

            mock_collection.dimensions = [country_dim]
            mock_create.return_value = mock_collection

            result = create_collection_single_table(
                config_yaml=config, dependencies=dependencies, catalog_path=catalog_path, choice_renames=choice_renames
            )

            # Verify choice names were updated
            assert usa_choice.name == "United States of America"
            assert can_choice.name == "Canada"
            assert result == mock_collection

    def test_create_collection_with_choice_renames_function(self):
        """Test collection creation with choice renaming using function."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"

        def rename_country(slug):
            if slug == "usa":
                return "United States of America"
            elif slug == "can":
                return "Canada"
            return None

        choice_renames = {"country": rename_country}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
            # Create a mock collection with dimensions and choices
            mock_collection = Mock(spec=Collection)

            usa_choice = Mock(spec=DimensionChoice)
            usa_choice.slug = "usa"
            usa_choice.name = "United States"

            country_dim = Mock(spec=Dimension)
            country_dim.slug = "country"
            country_dim.choices = [usa_choice]

            mock_collection.dimensions = [country_dim]
            mock_create.return_value = mock_collection

            result = create_collection_single_table(
                config_yaml=config, dependencies=dependencies, catalog_path=catalog_path, choice_renames=choice_renames
            )

            # Verify choice name was updated via function
            assert usa_choice.name == "United States of America"
            assert result == mock_collection

    def test_create_collection_with_all_expand_params(self):
        """Test collection creation with all expand_config parameters."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"
        tb = create_test_table()

        with patch("etl.collection.core.create.has_duplicate_table_names") as mock_has_duplicates:
            with patch("etl.collection.core.create.expand_config") as mock_expand:
                with patch("etl.collection.core.create.combine_config_dimensions") as mock_combine:
                    with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
                        mock_has_duplicates.return_value = False
                        mock_expand.return_value = {"dimensions": [], "views": []}
                        mock_combine.return_value = []
                        mock_collection = Mock(spec=Collection)
                        mock_create.return_value = mock_collection

                        result = create_collection_single_table(
                            config_yaml=config,
                            dependencies=dependencies,
                            catalog_path=catalog_path,
                            tb=tb,
                            indicator_names=["deaths"],
                            dimensions={"sex": ["male"]},
                            common_view_config={"chartTypes": ["LineChart"]},
                            indicators_slug="metric",
                            indicator_as_dimension=True,
                            catalog_path_full=True,
                        )

                        # Verify expand_config was called with all parameters
                        mock_expand.assert_called_once_with(
                            tb=tb,
                            indicator_names=["deaths"],
                            dimensions={"sex": ["male"]},
                            common_view_config={"chartTypes": ["LineChart"]},
                            indicators_slug="metric",
                            indicator_as_dimension=True,
                            expand_path_mode="full",
                        )

                        assert result == mock_collection


class TestGetExpandPathMode:
    """Test suite for _get_expand_path_mode helper function."""

    def test_get_expand_path_mode_default(self):
        """Test default expand path mode is 'table'."""
        dependencies = {"dataset1/table1", "dataset2/table2"}

        with patch("etl.collection.core.create.has_duplicate_table_names") as mock_has_duplicates:
            mock_has_duplicates.return_value = False

            result = _get_expand_path_mode(dependencies, catalog_path_full=False)

            assert result == "table"
            mock_has_duplicates.assert_called_once_with(dependencies)

    def test_get_expand_path_mode_full(self):
        """Test expand path mode is 'full' when catalog_path_full=True."""
        dependencies = {"dataset1/table1"}

        result = _get_expand_path_mode(dependencies, catalog_path_full=True)

        assert result == "full"

    def test_get_expand_path_mode_dataset_on_duplicates(self):
        """Test expand path mode is 'dataset' when there are duplicate table names."""
        dependencies = {"dataset1/table1", "dataset2/table1"}  # Same table name

        with patch("etl.collection.core.create.has_duplicate_table_names") as mock_has_duplicates:
            mock_has_duplicates.return_value = True

            result = _get_expand_path_mode(dependencies, catalog_path_full=False)

            assert result == "dataset"
            mock_has_duplicates.assert_called_once_with(dependencies)


class TestRenameChoices:
    """Test suite for _rename_choices helper function."""

    def test_rename_choices_with_dict(self):
        """Test choice renaming using dictionary mapping."""
        # Create mock collection with dimensions and choices
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "usa"
        choice1.name = "United States"

        choice2 = Mock(spec=DimensionChoice)
        choice2.slug = "can"
        choice2.name = "Canada"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1, choice2]

        mock_collection.dimensions = [dimension]

        choice_renames = {"country": {"usa": "United States of America", "can": "Canada (Renamed)"}}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        _rename_choices(mock_collection, choice_renames)

        # Verify names were updated
        assert choice1.name == "United States of America"
        assert choice2.name == "Canada (Renamed)"

    def test_rename_choices_with_function(self):
        """Test choice renaming using function."""
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "usa"
        choice1.name = "United States"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1]

        mock_collection.dimensions = [dimension]

        def rename_func(slug):
            if slug == "usa":
                return "United States of America"
            return None

        choice_renames = {"country": rename_func}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        _rename_choices(mock_collection, choice_renames)

        # Verify name was updated via function
        assert choice1.name == "United States of America"

    def test_rename_choices_function_returns_none(self):
        """Test choice renaming when function returns None (no rename)."""
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "gbr"
        choice1.name = "United Kingdom"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1]

        mock_collection.dimensions = [dimension]

        def rename_func(slug):
            if slug == "usa":
                return "United States of America"
            return None  # No rename for other slugs

        choice_renames = {"country": rename_func}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        _rename_choices(mock_collection, choice_renames)

        # Verify name was not changed
        assert choice1.name == "United Kingdom"

    def test_rename_choices_no_renames(self):
        """Test choice renaming when choice_renames is None."""
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "usa"
        choice1.name = "United States"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1]

        mock_collection.dimensions = [dimension]

        _rename_choices(mock_collection, None)

        # Verify name was not changed
        assert choice1.name == "United States"

    def test_rename_choices_dimension_not_in_renames(self):
        """Test choice renaming when dimension slug is not in renames."""
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "usa"
        choice1.name = "United States"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1]

        mock_collection.dimensions = [dimension]

        choice_renames = {
            "region": {  # Different dimension slug
                "north_america": "North America"
            }
        }
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        _rename_choices(mock_collection, choice_renames)

        # Verify name was not changed
        assert choice1.name == "United States"

    def test_rename_choices_invalid_renames_format(self):
        """Test choice renaming with invalid renames format raises error."""
        mock_collection = Mock(spec=Collection)

        choice1 = Mock(spec=DimensionChoice)
        choice1.slug = "usa"
        choice1.name = "United States"

        dimension = Mock(spec=Dimension)
        dimension.slug = "country"
        dimension.choices = [choice1]

        mock_collection.dimensions = [dimension]

        choice_renames = {
            "country": "invalid_format"  # Should be dict or function
        }

        with pytest.raises(ValueError, match="Invalid choice_renames format"):
            _rename_choices(mock_collection, choice_renames)  # type: ignore[arg-type]


class TestIntegration:
    """Integration tests for create_collection function."""

    def test_create_collection_integration_with_table(self):
        """Test complete integration with table expansion and choice renaming."""
        config = create_test_config()
        dependencies = {"test#indicator1"}
        catalog_path = "test/latest/data#table"
        tb = create_test_table()

        choice_renames = {"sex": {"male": "Male", "female": "Female"}}
        choice_renames = cast(dict[str, dict[str, str] | Callable], choice_renames)

        with patch("etl.collection.core.create.has_duplicate_table_names") as mock_has_duplicates:
            with patch("etl.collection.core.create.create_collection_from_config") as mock_create:
                mock_has_duplicates.return_value = False

                # Create a mock collection to test the full flow
                mock_collection = Mock(spec=Collection)

                # Mock dimension with choices for renaming test
                choice_male = Mock(spec=DimensionChoice)
                choice_male.slug = "male"
                choice_male.name = "male"

                choice_female = Mock(spec=DimensionChoice)
                choice_female.slug = "female"
                choice_female.name = "female"

                sex_dimension = Mock(spec=Dimension)
                sex_dimension.slug = "sex"
                sex_dimension.choices = [choice_male, choice_female]

                mock_collection.dimensions = [sex_dimension]
                mock_create.return_value = mock_collection

                # Mock the expand_config and combine functions to work together
                with patch("etl.collection.core.create.expand_config") as mock_expand:
                    with patch("etl.collection.core.create.combine_config_dimensions") as mock_combine:
                        mock_expand.return_value = {
                            "dimensions": [{"slug": "sex", "name": "Sex", "choices": []}],
                            "views": [],
                        }
                        mock_combine.return_value = [{"slug": "sex", "name": "Sex", "choices": []}]

                        result = create_collection_single_table(
                            config_yaml=config,
                            dependencies=dependencies,
                            catalog_path=catalog_path,
                            tb=tb,
                            indicator_names="deaths",
                            choice_renames=choice_renames,
                        )

                        # Verify the full pipeline was executed
                        mock_expand.assert_called_once()
                        mock_combine.assert_called_once()
                        mock_create.assert_called_once()

                        # Verify choice renaming was applied
                        assert choice_male.name == "Male"
                        assert choice_female.name == "Female"

                        assert result == mock_collection


class TestCreateCollectionMultipleTables:
    """Test suite for create_collection function with multiple tables."""

    def test_create_collection_multiple_tables_basic(self):
        """Test collection creation with multiple tables."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()

        with patch("etl.collection.core.create.create_collection_single_table") as mock_single:
            with patch("etl.collection.core.create.combine_collections") as mock_combine:
                # Mock single table creation to return different collections
                mock_collection_1 = Mock(spec=Collection)
                mock_collection_2 = Mock(spec=Collection)
                mock_single.side_effect = [mock_collection_1, mock_collection_2]

                # Mock combine_collections to return final collection
                mock_final_collection = Mock(spec=Collection)
                mock_combine.return_value = mock_final_collection

                result = create_collection(
                    config_yaml=config,
                    dependencies=dependencies,
                    catalog_path=catalog_path,
                    tb=tables,
                    indicator_names=[["deaths"], ["cases"]],
                )

                # Verify create_collection_single_table was called twice
                assert mock_single.call_count == 2

                # Verify first call arguments
                first_call = mock_single.call_args_list[0]
                assert first_call[1]["tb"] is tables[0]
                assert first_call[1]["indicator_names"] == ["deaths"]

                # Verify second call arguments
                second_call = mock_single.call_args_list[1]
                assert second_call[1]["tb"] is tables[1]
                assert second_call[1]["indicator_names"] == ["cases"]

                # Verify combine_collections was called
                mock_combine.assert_called_once_with(
                    collections=[mock_collection_1, mock_collection_2],
                    catalog_path=catalog_path,
                    config=config,
                    is_explorer=False,
                )

                assert result == mock_final_collection

    def test_create_collection_multiple_tables_with_list_parameters(self):
        """Test collection creation with multiple tables and list parameters."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()

        # List parameters for each table
        indicator_names = [["deaths"], ["cases"]]
        dimensions = [{"sex": ["male", "female"]}, {"age": ["young", "old"]}]
        common_view_configs = [{"chartType": "LineChart"}, {"chartType": "BarChart"}]
        choice_renames = [
            {"sex": {"male": "Male", "female": "Female"}},
            {"age": {"young": "Young", "old": "Old"}},
        ]

        with patch("etl.collection.core.create.create_collection_single_table") as mock_single:
            with patch("etl.collection.core.create.combine_collections") as mock_combine:
                mock_collection_1 = Mock(spec=Collection)
                mock_collection_2 = Mock(spec=Collection)
                mock_single.side_effect = [mock_collection_1, mock_collection_2]

                mock_final_collection = Mock(spec=Collection)
                mock_combine.return_value = mock_final_collection

                result = create_collection(
                    config_yaml=config,
                    dependencies=dependencies,
                    catalog_path=catalog_path,
                    tb=tables,
                    indicator_names=indicator_names,
                    dimensions=dimensions,
                    common_view_config=common_view_configs,
                    choice_renames=choice_renames,
                )

                # Verify create_collection_single_table was called twice with correct parameters
                assert mock_single.call_count == 2

                first_call = mock_single.call_args_list[0]
                assert first_call[1]["tb"] is tables[0]
                assert first_call[1]["indicator_names"] == ["deaths"]
                assert first_call[1]["dimensions"] == {"sex": ["male", "female"]}
                assert first_call[1]["common_view_config"] == {"chartType": "LineChart"}
                assert first_call[1]["choice_renames"] == {"sex": {"male": "Male", "female": "Female"}}

                second_call = mock_single.call_args_list[1]
                assert second_call[1]["tb"] is tables[1]
                assert second_call[1]["indicator_names"] == ["cases"]
                assert second_call[1]["dimensions"] == {"age": ["young", "old"]}
                assert second_call[1]["common_view_config"] == {"chartType": "BarChart"}
                assert second_call[1]["choice_renames"] == {"age": {"young": "Young", "old": "Old"}}

                assert result == mock_final_collection

    def test_create_collection_multiple_tables_single_parameters(self):
        """Test collection creation with multiple tables and single parameters applied to all."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()

        # Single parameters that should be applied to all tables
        single_indicator_name = "metric"
        single_dimensions = {"country": ["USA", "CAN"]}
        single_common_view_config = {"chartType": "ScatterPlot"}

        with patch("etl.collection.core.create.create_collection_single_table") as mock_single:
            with patch("etl.collection.core.create.combine_collections") as mock_combine:
                mock_collection_1 = Mock(spec=Collection)
                mock_collection_2 = Mock(spec=Collection)
                mock_single.side_effect = [mock_collection_1, mock_collection_2]

                mock_final_collection = Mock(spec=Collection)
                mock_combine.return_value = mock_final_collection

                result = create_collection(
                    config_yaml=config,
                    dependencies=dependencies,
                    catalog_path=catalog_path,
                    tb=tables,
                    indicator_names=single_indicator_name,
                    dimensions=single_dimensions,
                    common_view_config=single_common_view_config,
                )

                # Verify both calls received the same single parameters
                assert mock_single.call_count == 2

                for call in mock_single.call_args_list:
                    assert call[1]["indicator_names"] == single_indicator_name
                    assert call[1]["dimensions"] == single_dimensions
                    assert call[1]["common_view_config"] == single_common_view_config

                assert result == mock_final_collection

    def test_create_collection_multiple_tables_explorer_mode(self):
        """Test collection creation with multiple tables in explorer mode."""
        config = create_test_explorer_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()

        with patch("etl.collection.core.create.create_collection_single_table") as mock_single:
            with patch("etl.collection.core.create.combine_collections") as mock_combine:
                mock_explorer_1 = Mock(spec=Explorer)
                mock_explorer_2 = Mock(spec=Explorer)
                mock_single.side_effect = [mock_explorer_1, mock_explorer_2]

                mock_final_explorer = Mock(spec=Explorer)
                mock_combine.return_value = mock_final_explorer

                result = create_collection(
                    config_yaml=config,
                    dependencies=dependencies,
                    catalog_path=catalog_path,
                    tb=tables,
                    explorer=True,
                )

                # Verify explorer flag was passed to all calls
                assert mock_single.call_count == 2
                for call in mock_single.call_args_list:
                    assert call[1]["explorer"] is True

                # Verify combine_collections was called with explorer flag
                mock_combine.assert_called_once_with(
                    collections=[mock_explorer_1, mock_explorer_2],
                    catalog_path=catalog_path,
                    config=config,
                    is_explorer=True,
                )

                assert result == mock_final_explorer

    def test_create_collection_multiple_tables_mismatched_list_length(self):
        """Test that mismatched list lengths raise ValueError."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()  # 2 tables

        # Provide list of indicator names with wrong length (3 for 2 tables)
        indicator_names = [["deaths"], ["cases"], ["extra"]]

        with pytest.raises(ValueError, match="Parameter 'indicator_names' is a list of length 3"):
            create_collection(
                config_yaml=config,
                dependencies=dependencies,
                catalog_path=catalog_path,
                tb=tables,
                indicator_names=indicator_names,
            )

    def test_create_collection_multiple_tables_mixed_none_parameters(self):
        """Test collection creation with multiple tables where some list parameters contain None."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"
        tables = create_multiple_test_tables()

        # Mixed parameters with None values
        indicator_names = [None, "cases"]  # First table uses all indicators, second uses specific
        dimensions = [None, {"age": ["young"]}]  # First table uses all dimensions, second uses specific

        with patch("etl.collection.core.create.create_collection_single_table") as mock_single:
            with patch("etl.collection.core.create.combine_collections") as mock_combine:
                mock_collection_1 = Mock(spec=Collection)
                mock_collection_2 = Mock(spec=Collection)
                mock_single.side_effect = [mock_collection_1, mock_collection_2]

                mock_final_collection = Mock(spec=Collection)
                mock_combine.return_value = mock_final_collection

                result = create_collection(
                    config_yaml=config,
                    dependencies=dependencies,
                    catalog_path=catalog_path,
                    tb=tables,
                    indicator_names=indicator_names,
                    dimensions=dimensions,
                )

                # Verify parameters were passed correctly
                first_call = mock_single.call_args_list[0]
                assert first_call[1]["indicator_names"] is None
                assert first_call[1]["dimensions"] is None

                second_call = mock_single.call_args_list[1]
                assert second_call[1]["indicator_names"] == "cases"
                assert second_call[1]["dimensions"] == {"age": ["young"]}

                assert result == mock_final_collection
