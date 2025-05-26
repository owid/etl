"""Tests for etl.collection.model.core module.

This module tests the Collection and CollectionSet classes which are core components
for managing ETL collections. Collections define data views with dimensions, indicators,
and configuration for data visualization and exploration.
"""

from pathlib import Path

from etl.collection.core.collection_set import CollectionSet
from etl.collection.model.core import Collection


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
