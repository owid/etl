"""
Tests for ETL collection utility functions.

This module tests utility functions from etl.collection.utils that handle
data manipulation, view processing, and configuration management.
"""
from pathlib import Path
from unittest.mock import Mock

import pytest


# Mock dependencies to avoid heavy imports
@pytest.fixture(autouse=True)
def mock_dependencies(monkeypatch):
    """
    Mock heavy dependencies to make tests lightweight and fast.
    
    Uses monkeypatch to cleanly mock modules without polluting sys.modules.
    This approach is much cleaner than manual sys.modules manipulation.
    """
    # Mock deprecated decorator
    def mock_deprecated(reason):
        def decorator(func):
            return func
        return decorator
    
    monkeypatch.setattr("deprecated.deprecated", mock_deprecated)
    
    # Mock catalog and database dependencies
    monkeypatch.setattr("owid.catalog.Dataset", object)
    monkeypatch.setattr("sqlalchemy.orm.Session", object)
    
    # Mock ETL modules
    monkeypatch.setattr("etl.config.OWID_ENV", None)
    monkeypatch.setattr("etl.config.OWIDEnv", object)
    monkeypatch.setattr("etl.db.read_sql", Mock(return_value=None))
    monkeypatch.setattr("etl.files.yaml_dump", Mock(return_value=""))
    monkeypatch.setattr("etl.paths.DATA_DIR", Path("."))
    
    # Mock grapher model
    monkeypatch.setattr("etl.grapher.model", Mock())


@pytest.fixture
def collection_utils():
    """
    Import and return the collection utils module with mocked dependencies.
    
    This fixture handles the import after mocking is in place, ensuring
    the module loads cleanly without heavy dependencies.
    """
    # Import after mocking is set up
    import importlib.util
    
    ROOT = Path(__file__).resolve().parents[1]
    
    # Load exceptions module first
    spec_exc = importlib.util.spec_from_file_location(
        "etl.collection.exceptions", 
        ROOT / "etl/collection/exceptions.py"
    )
    exc_module = importlib.util.module_from_spec(spec_exc)
    spec_exc.loader.exec_module(exc_module)
    
    # Load utils module
    spec_utils = importlib.util.spec_from_file_location(
        "collection_utils", 
        ROOT / "etl/collection/utils.py"
    )
    utils_module = importlib.util.module_from_spec(spec_utils)
    spec_utils.loader.exec_module(utils_module)
    
    # Attach exception classes to utils for easy access
    utils_module.ParamKeyError = exc_module.ParamKeyError
    
    return utils_module


# ----------------------------------------------------------------------------
# expand_combinations and get_complete_dimensions_filter
# ----------------------------------------------------------------------------


def test_expand_combinations(collection_utils):
    """Test expanding dimension combinations into all possible permutations."""
    dims = {"a": ["x", "y"], "b": ["1"]}
    combos = collection_utils.expand_combinations(dims)
    assert len(combos) == 2
    assert {tuple(sorted(c.items())) for c in combos} == {
        tuple(sorted({"a": "x", "b": "1"}.items())),
        tuple(sorted({"a": "y", "b": "1"}.items())),
    }


def test_get_complete_dimensions_filter(collection_utils):
    """Test completing partial dimension filters with all available values."""
    dims_avail = {"metric": {"cases", "deaths"}, "age": {"0-9", "10-19"}}
    dims_filter = {"metric": "cases"}
    result = collection_utils.get_complete_dimensions_filter(dims_avail, dims_filter)
    assert {tuple(sorted(r.items())) for r in result} == {
        tuple(sorted({"metric": "cases", "age": "0-9"}.items())),
        tuple(sorted({"metric": "cases", "age": "10-19"}.items())),
    }
    with pytest.raises(AssertionError):
        collection_utils.get_complete_dimensions_filter(dims_avail, {"metric": "unknown"})


# ----------------------------------------------------------------------------
# move_field_to_top and extract_definitions
# ----------------------------------------------------------------------------


def test_move_field_to_top(collection_utils):
    """Test moving a dictionary field to the top while preserving order."""
    data = {"b": 2, "a": 1, "c": 3}
    moved = collection_utils.move_field_to_top(data, "a")
    assert list(moved.keys())[:1] == ["a"]
    # Ensure other fields preserved
    assert list(moved.keys()) == ["a", "b", "c"]
    # Field not present: object should be returned unchanged
    same = collection_utils.move_field_to_top(data, "missing")
    assert same is data


def test_extract_definitions_simple(collection_utils):
    """Test extracting common definitions to reduce config duplication."""
    config = {"views": [{"indicators": {"y": [{"display": {"additionalInfo": "Line1\\nLine2"}}]}}]}
    out = collection_utils.extract_definitions(config)
    # definitions moved to top
    assert list(out.keys())[0] == "definitions"
    defs = out["definitions"]["additionalInfo"]
    assert isinstance(defs, dict) and len(defs) == 1
    anchor = next(iter(defs))
    assert defs[anchor] == "Line1\nLine2"
    # indicator references the anchor
    assert out["views"][0]["indicators"]["y"][0]["display"]["additionalInfo"] == f"*{anchor}"


# ----------------------------------------------------------------------------
# fill_placeholders
# ----------------------------------------------------------------------------


def test_fill_placeholders(collection_utils):
    """Test filling template placeholders in nested data structures."""
    data = {
        "a": "{x} is {y}",
        "b": ["{y}", 1],
        "c": {"d": "{x}"},
        "e": ("{x}", "{y}"),
    }
    params = {"x": "foo", "y": "bar"}
    out = collection_utils.fill_placeholders(data, params)
    assert out == {
        "a": "foo is bar",
        "b": ["bar", 1],
        "c": {"d": "foo"},
        "e": ("foo", "bar"),
    }

    with pytest.raises(collection_utils.ParamKeyError):
        collection_utils.fill_placeholders("{x} {z}", {"x": "foo"})


# ----------------------------------------------------------------------------
# group_views_legacy and helpers
# ----------------------------------------------------------------------------


def test_group_views_legacy(collection_utils):
    """Test grouping views by dimensions for legacy compatibility."""
    views = [
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind1"}},
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind2"}},
        {"dimensions": {"country": "b"}, "indicators": {"y": "ind3"}},
    ]
    grouped = collection_utils.group_views_legacy(views, by=["country"])
    assert grouped == [
        {
            "dimensions": {"country": "a"},
            "indicators": {"y": ["ind1", "ind2"]},
        },
        {
            "dimensions": {"country": "b"},
            "indicators": {"y": ["ind3"]},
        },
    ]

    err_view = {"dimensions": {"country": "c"}, "indicators": {"y": ["a", "b"]}}
    with pytest.raises(NotImplementedError):
        collection_utils.group_views_legacy([err_view], by=["country"])


# ----------------------------------------------------------------------------
# records_to_dictionary and unique_records
# ----------------------------------------------------------------------------


def test_records_to_dictionary_and_unique_records(collection_utils):
    """Test converting records to dictionary and removing duplicates."""
    recs = [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
        {"id": 1, "v": "a"},
    ]
    dic = collection_utils.records_to_dictionary(recs, "id")
    assert dic == {1: {"v": "a"}, 2: {"v": "b"}}

    uniq = collection_utils.unique_records(recs)
    assert uniq == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
    ]
