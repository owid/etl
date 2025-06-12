"""Tests for etl.collection.core.utils module.

This module tests utility functions for processing collections and views,
including the main collection creation function and view processing logic.
"""

from unittest.mock import Mock, patch

from etl.collection.core.utils import create_collection_from_config, process_views
from etl.collection.explorer import Explorer
from etl.collection.model.core import Collection
from etl.collection.model.view import CommonView, View


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
        "views": [
            {"dimensions": {"country": "usa"}, "indicators": {"y": [{"catalogPath": "test#indicator1"}]}},
            {"dimensions": {"country": "can"}, "indicators": {"y": [{"catalogPath": "test#indicator2"}]}},
        ],
        "definitions": {"common_views": []},
        "default_dimensions": {"country": "usa"},
    }


def create_test_config_with_common_views():
    """Create test config with common views in definitions."""
    config = create_test_config()
    config["definitions"]["common_views"] = [
        {"dimensions": {"country": "usa"}, "config": {"chartTypes": ["LineChart"], "hasMapTab": True}}
    ]
    return config


def create_test_explorer_config():
    """Create a basic test configuration for explorers."""
    config = create_test_config()
    config["config"] = {"hasMapTab": True, "chartTypes": ["LineChart"]}
    return config


class TestProcessViews:
    """Test suite for process_views function."""

    def test_process_views_basic(self):
        """Test basic view processing functionality."""
        # Create mock collection with views
        collection = Mock(spec=Collection)
        view1 = Mock(spec=View)
        view2 = Mock(spec=View)
        collection.views = [view1, view2]
        collection.definitions = None

        dependencies = {"test#indicator1", "test#indicator2"}

        with patch("etl.collection.core.utils.get_tables_by_name_mapping") as mock_mapping:
            mock_mapping.return_value = {"table": ["test/latest/data#table"]}

            process_views(collection, dependencies)

            # Verify expand_paths was called on each view
            view1.expand_paths.assert_called_once_with({"table": ["test/latest/data#table"]})
            view2.expand_paths.assert_called_once_with({"table": ["test/latest/data#table"]})

            # Verify common views combination was not called (no definitions)
            view1.combine_with_common.assert_not_called()
            view2.combine_with_common.assert_not_called()

    def test_process_views_with_common_views(self):
        """Test view processing with common views defined."""
        # Create mock collection with common views
        collection = Mock(spec=Collection)
        view1 = Mock(spec=View)
        collection.views = [view1]

        # Mock definitions with common views
        common_views = [Mock(spec=CommonView)]
        definitions = Mock()
        definitions.common_views = common_views
        collection.definitions = definitions

        dependencies = {"test#indicator1"}

        with patch("etl.collection.core.utils.get_tables_by_name_mapping") as mock_mapping:
            mock_mapping.return_value = {"table": ["test/latest/data#table"]}

            process_views(collection, dependencies)

            # Verify expand_paths was called
            view1.expand_paths.assert_called_once()

            # Verify common views combination was called
            view1.combine_with_common.assert_called_once_with(common_views)

    def test_process_views_with_metadata_needed(self):
        """Test view processing when metadata is needed (multiple indicators)."""
        collection = Mock(spec=Collection)
        view1 = Mock(spec=View)
        view1.metadata_is_needed = True  # Simulate multiple indicators
        collection.views = [view1]
        collection.definitions = None

        dependencies = {"test#indicator1"}

        with patch("etl.collection.core.utils.get_tables_by_name_mapping") as mock_mapping:
            mock_mapping.return_value = {"table": ["test/latest/data#table"]}

            # Test with combine_metadata_when_mult=True
            process_views(collection, dependencies, combine_metadata_when_mult=True)

            # Currently this just passes, but test ensures no errors
            view1.expand_paths.assert_called_once()

    def test_process_views_with_explorer(self):
        """Test process_views works with Explorer objects too."""
        explorer = Mock(spec=Explorer)
        view1 = Mock(spec=View)
        explorer.views = [view1]
        explorer.definitions = None

        dependencies = {"test#indicator1"}

        with patch("etl.collection.core.utils.get_tables_by_name_mapping") as mock_mapping:
            mock_mapping.return_value = {"table": ["test/latest/data#table"]}

            process_views(explorer, dependencies)

            view1.expand_paths.assert_called_once()


class TestCreateCollectionFromConfig:
    """Test suite for create_collection_from_config function."""

    def test_create_collection_basic(self):
        """Test basic collection creation from config."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.utils.process_views") as mock_process:
            collection = create_collection_from_config(config, dependencies, catalog_path, explorer=False)

            # Verify it returns a Collection
            assert isinstance(collection, Collection)
            assert collection.catalog_path == catalog_path
            assert collection.title == {"title": "Test Collection", "title_variant": "Test Collection Variant"}

            # Verify process_views was called
            mock_process.assert_called_once_with(collection, dependencies=dependencies)

    def test_create_explorer_from_config(self):
        """Test explorer creation from config."""
        config = create_test_explorer_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.utils.process_views") as mock_process:
            with patch.object(Explorer, "validate_schema"):
                explorer = create_collection_from_config(config, dependencies, catalog_path, explorer=True)

                # Verify it returns an Explorer
                assert isinstance(explorer, Explorer)
                assert explorer.catalog_path == catalog_path
                assert explorer.title == {"title": "Test Collection", "title_variant": "Test Collection Variant"}

                # Verify process_views was called
                mock_process.assert_called_once_with(explorer, dependencies=dependencies)

    def test_create_collection_with_validation_disabled(self):
        """Test collection creation with schema validation disabled."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.utils.process_views"):
            collection = create_collection_from_config(config, dependencies, catalog_path, validate_schema=False)

            # Should still create collection successfully
            assert isinstance(collection, Collection)
            assert collection.catalog_path == catalog_path

    def test_create_collection_validation_steps(self):
        """Test that all validation steps are called when enabled."""
        config = create_test_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.utils.process_views"):
            with patch.object(Collection, "validate_schema") as mock_validate_schema:
                with patch.object(Collection, "validate_views_with_dimensions") as mock_validate_views:
                    with patch.object(Collection, "check_duplicate_views") as mock_check_duplicates:
                        create_collection_from_config(config, dependencies, catalog_path, validate_schema=True)

                        # Verify all validation methods were called
                        mock_validate_schema.assert_called_once()
                        mock_validate_views.assert_called_once()
                        mock_check_duplicates.assert_called_once()

    def test_create_collection_config_merging(self):
        """Test that catalog_path is properly merged into config."""
        config = {"title": {"title": "Test"}}
        dependencies = set()
        catalog_path = "test/path#table"

        with patch("etl.collection.core.utils.process_views"):
            with patch.object(Collection, "from_dict") as mock_from_dict:
                mock_from_dict.return_value = Mock(spec=Collection)

                create_collection_from_config(config, dependencies, catalog_path, validate_schema=False)

                # Verify from_dict was called with merged config
                expected_config = {"title": {"title": "Test"}, "catalog_path": catalog_path}
                mock_from_dict.assert_called_once_with(expected_config)

    def test_create_explorer_config_merging(self):
        """Test that catalog_path is properly merged into config for Explorer."""
        config = {"title": {"title": "Test"}}
        dependencies = set()
        catalog_path = "test/path#table"

        with patch("etl.collection.core.utils.process_views"):
            with patch.object(Explorer, "from_dict") as mock_from_dict:
                mock_from_dict.return_value = Mock(spec=Explorer)

                create_collection_from_config(config, dependencies, catalog_path, explorer=True, validate_schema=False)

                # Verify from_dict was called with merged config
                expected_config = {"title": {"title": "Test"}, "catalog_path": catalog_path}
                mock_from_dict.assert_called_once_with(expected_config)

    def test_overload_return_types(self):
        """Test that overloaded functions have correct return type hints."""
        # This is more of a static type checking test, but we can verify runtime behavior
        collection_config = create_test_config()
        explorer_config = create_test_explorer_config()
        dependencies = set()
        catalog_path = "test/path#table"

        with patch("etl.collection.core.utils.process_views"):
            # Test Collection return type
            collection = create_collection_from_config(
                collection_config, dependencies, catalog_path, explorer=False, validate_schema=False
            )
            assert isinstance(collection, Collection)

            # Test Explorer return type
            explorer = create_collection_from_config(
                explorer_config, dependencies, catalog_path, explorer=True, validate_schema=False
            )
            assert isinstance(explorer, Explorer)


class TestIntegration:
    """Integration tests that test the full pipeline."""

    def test_full_pipeline_collection(self):
        """Test complete collection creation and processing pipeline."""
        config = create_test_config_with_common_views()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        # Mock the external dependencies but allow real object creation
        with patch("etl.collection.core.utils.process_views") as mock_process:
            with patch.object(Collection, "validate_schema"):
                collection = create_collection_from_config(config, dependencies, catalog_path, validate_schema=True)

                # Verify collection was created with proper structure
                assert isinstance(collection, Collection)
                assert len(collection.views) == 2
                assert len(collection.dimensions) == 1
                assert collection.catalog_path == catalog_path

                # Verify process_views was called
                mock_process.assert_called_once_with(collection, dependencies=dependencies)

    def test_full_pipeline_explorer(self):
        """Test complete explorer creation and processing pipeline."""
        config = create_test_explorer_config()
        dependencies = {"test#indicator1", "test#indicator2"}
        catalog_path = "test/latest/data#table"

        with patch("etl.collection.core.utils.process_views") as mock_process:
            with patch.object(Explorer, "validate_schema"):
                explorer = create_collection_from_config(
                    config, dependencies, catalog_path, explorer=True, validate_schema=True
                )

                # Verify explorer was created with proper structure
                assert isinstance(explorer, Explorer)
                assert len(explorer.views) == 2
                assert len(explorer.dimensions) == 1
                assert explorer.catalog_path == catalog_path

                # Verify process_views was called
                mock_process.assert_called_once_with(explorer, dependencies=dependencies)
