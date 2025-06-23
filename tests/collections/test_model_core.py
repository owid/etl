"""Tests for etl.collection.model.core module.

This module tests the Collection and CollectionSet classes which are core components
for managing ETL collections. Collections define data views with dimensions, indicators,
and configuration for data visualization and exploration.
"""

import warnings
from pathlib import Path
from unittest.mock import patch

from etl.collection.core.collection_set import CollectionSet
from etl.collection.model.core import Collection
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.model.view import View, ViewIndicators


def _simple_collection_dict(name: str) -> dict:
    """Create a minimal collection dictionary for testing purposes.

    Args:
        name: The name to use for the collection (used in catalog_path)

    Returns:
        A dictionary representing a basic collection with:
        - One dimension with one choice
        - One view with that dimension and one indicator
        - Basic metadata (title, catalog_path, etc.)
    """
    return {
        "dimensions": [{"slug": "dim", "name": "Dim", "choices": [{"slug": "a", "name": "A"}]}],
        "views": [
            {
                "dimensions": {"dim": "a"},
                "indicators": {"y": [{"catalogPath": "table#ind"}]},
            }
        ],
        "catalog_path": f"dataset#{name}",
        "title": {"title": "Title"},
        "default_selection": [],
    }


def test_collection_set(tmp_path: Path):
    """Test CollectionSet functionality for managing multiple collections.

    This test verifies that:
    1. Collections can be created from dictionaries and saved to files
    2. CollectionSet can discover and list collection files in a directory
    3. CollectionSet can load individual collections by name
    4. Loaded collections maintain their properties (short_name, etc.)
    5. The file naming convention (*.config.json) works correctly

    Args:
        tmp_path: Pytest fixture providing a temporary directory for test files
    """
    path = tmp_path
    coll1 = Collection.from_dict(_simple_collection_dict("coll1"))
    coll2 = Collection.from_dict(_simple_collection_dict("coll2"))
    coll1.save_file(path / "coll1.config.json")
    coll2.save_file(path / "coll2.config.json")

    cs = CollectionSet(path)
    assert cs.names == ["coll1", "coll2"]

    loaded = cs.read("coll1")
    assert isinstance(loaded, Collection)
    assert loaded.short_name == "coll1"


def test_grouped_view_validation_warnings():
    """Test that grouped views without proper metadata generate warnings during save.
    
    This test verifies the sanity_check_grouped_view functionality by:
    1. Creating a collection with multiple views
    2. Grouping views to create grouped views without metadata
    3. Mocking the database validation to avoid DB dependency
    4. Calling save() and verifying appropriate warnings are raised
    """
    # Create collection with dimensions and views
    collection = Collection(
        dimensions=[
            Dimension(
                slug="sex",
                name="Sex", 
                choices=[
                    DimensionChoice(slug="male", name="Male"),
                    DimensionChoice(slug="female", name="Female"),
                ]
            ),
            Dimension(
                slug="age",
                name="Age",
                choices=[
                    DimensionChoice(slug="adults", name="Adults"),
                    DimensionChoice(slug="children", name="Children"),
                ]
            )
        ],
        views=[
            View(
                dimensions={"sex": "male", "age": "adults"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator1"}])
            ),
            View(
                dimensions={"sex": "female", "age": "adults"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator2"}])
            ),
            View(
                dimensions={"sex": "male", "age": "children"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator3"}])
            ),
            View(
                dimensions={"sex": "female", "age": "children"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator4"}])
            ),
        ],
        catalog_path="test#collection",
        title={"title": "Test Collection"},
        default_selection=["test"],
        _definitions={"common_views": None}
    )
    
    # Group views by sex dimension (creates grouped views without metadata)
    collection.group_views([{
        "dimension": "sex",
        "choice_new_slug": "all_sexes",
        "choices": ["male", "female"]
    }])
    
    # Verify that grouped views were created and marked as grouped
    grouped_views = [view for view in collection.views if view.is_grouped]
    assert len(grouped_views) == 2  # Should have 2 grouped views (one for each age group)
    
    # Mock database validation to avoid DB dependency
    with patch('etl.collection.utils.validate_indicators_in_db'):
        # Capture warnings during save
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # Ensure all warnings are captured
            
            # This should trigger warnings for missing metadata
            collection.validate_grouped_views()
            
            # Verify warnings were raised
            assert len(w) >= 2  # At least 2 warnings (one per grouped view)
            
            # Check that warnings mention missing metadata
            warning_messages = [str(warning.message) for warning in w]
            
            # Should have warnings about missing metadata attribute
            missing_metadata_warnings = [msg for msg in warning_messages if "missing 'metadata' attribute" in msg]
            assert len(missing_metadata_warnings) >= 2
            
            # Verify warning details
            for warning_msg in missing_metadata_warnings:
                assert "description_key" in warning_msg
                assert "description_short" in warning_msg
                assert "all_sexes" in warning_msg  # Should mention the grouped choice


def test_grouped_view_validation_with_incomplete_metadata():
    """Test warnings for grouped views with partial metadata."""
    # Create a collection similar to above
    collection = Collection(
        dimensions=[
            Dimension(
                slug="category",
                name="Category", 
                choices=[
                    DimensionChoice(slug="a", name="A"),
                    DimensionChoice(slug="b", name="B"),
                ]
            )
        ],
        views=[
            View(
                dimensions={"category": "a"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator1"}])
            ),
            View(
                dimensions={"category": "b"},
                indicators=ViewIndicators(y=[{"catalogPath": "table#indicator2"}])
            ),
        ],
        catalog_path="test#collection2",
        title={"title": "Test Collection 2"},
        default_selection=["test"],
        _definitions={"common_views": None}
    )
    
    # Group views with partial metadata (missing description_short)
    collection.group_views([{
        "dimension": "category",
        "choice_new_slug": "combined",
        "choices": ["a", "b"],
        "view_metadata": {
            "description_key": ["Some key info"]  # Missing description_short
        }
    }])
    
    # Test validation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        collection.validate_grouped_views()
        
        # Should have warning about missing description_short
        warning_messages = [str(warning.message) for warning in w]
        missing_desc_short = [msg for msg in warning_messages if "missing 'description_short'" in msg]
        assert len(missing_desc_short) >= 1
