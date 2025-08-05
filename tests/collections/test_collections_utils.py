"""
Tests for ETL collection utility functions.

This module tests utility functions from etl.collection.utils that handle
data manipulation, view processing, and configuration management.
"""

import pytest


# Test the core utility functions that don't require heavy dependencies
def test_expand_combinations():
    """
    Test expand_combinations - creates all possible combinations from dimension choices.

    Example: {"country": ["USA", "UK"], "metric": ["cases"]} becomes:
    [{"country": "USA", "metric": "cases"}, {"country": "UK", "metric": "cases"}]
    """
    from etl.collection.utils import expand_combinations

    dims = {"a": ["x", "y"], "b": ["1"]}
    combos = expand_combinations(dims)
    assert len(combos) == 2
    assert {tuple(sorted(c.items())) for c in combos} == {
        tuple(sorted({"a": "x", "b": "1"}.items())),
        tuple(sorted({"a": "y", "b": "1"}.items())),
    }


def test_get_complete_dimensions_filter():
    """
    Test get_complete_dimensions_filter - expands partial filters to all combinations.

    Example: If you filter by metric="cases" but don't specify age, it expands
    to all age groups: [{"metric": "cases", "age": "0-9"}, {"metric": "cases", "age": "10-19"}]
    """
    from etl.collection.utils import get_complete_dimensions_filter

    dims_avail = {"metric": {"cases", "deaths"}, "age": {"0-9", "10-19"}}
    dims_filter = {"metric": "cases"}
    result = get_complete_dimensions_filter(dims_avail, dims_filter)
    assert {tuple(sorted(r.items())) for r in result} == {
        tuple(sorted({"metric": "cases", "age": "0-9"}.items())),
        tuple(sorted({"metric": "cases", "age": "10-19"}.items())),
    }
    with pytest.raises(AssertionError):
        get_complete_dimensions_filter(dims_avail, {"metric": "unknown"})


def test_move_field_to_top():
    """
    Test move_field_to_top - moves a dictionary field to the beginning.

    Example: {"b": 2, "a": 1, "c": 3} with field "a" becomes {"a": 1, "b": 2, "c": 3}
    """
    from etl.collection.utils import move_field_to_top

    data = {"b": 2, "a": 1, "c": 3}
    moved = move_field_to_top(data, "a")
    assert list(moved.keys())[:1] == ["a"]
    # Ensure other fields preserved
    assert list(moved.keys()) == ["a", "b", "c"]
    # Field not present: object should be returned unchanged
    same = move_field_to_top(data, "missing")
    assert same is data


def test_extract_definitions_simple():
    """
    Test extract_definitions - moves repeated content to a shared definitions section.

    Example: Multiple indicators with same additionalInfo get extracted to a
    definitions block and replaced with anchor references like "*def_12345"
    """
    from etl.collection.utils import extract_definitions

    config = {"views": [{"indicators": {"y": [{"display": {"additionalInfo": "Line1\\nLine2"}}]}}]}
    out = extract_definitions(config)
    # definitions moved to top
    assert list(out.keys())[0] == "definitions"
    defs = out["definitions"]["additionalInfo"]
    assert isinstance(defs, dict) and len(defs) == 1
    anchor = next(iter(defs))
    assert defs[anchor] == "Line1\nLine2"
    # indicator references the anchor
    assert out["views"][0]["indicators"]["y"][0]["display"]["additionalInfo"] == f"*{anchor}"


def test_fill_placeholders():
    """
    Test fill_placeholders - replaces {placeholder} templates with actual values.

    Example: "{country} has {cases} cases" with params {"country": "USA", "cases": 100}
    becomes "USA has 100 cases". Works recursively on nested dicts/lists.
    """
    from etl.collection.exceptions import ParamKeyError
    from etl.collection.utils import fill_placeholders

    data = {
        "a": "{x} is {y}",
        "b": ["{y}", 1],
        "c": {"d": "{x}"},
        "e": ("{x}", "{y}"),
    }
    params = {"x": "foo", "y": "bar"}
    out = fill_placeholders(data, params)
    assert out == {
        "a": "foo is bar",
        "b": ["bar", 1],
        "c": {"d": "foo"},
        "e": ("foo", "bar"),
    }

    with pytest.raises(ParamKeyError):
        fill_placeholders("{x} {z}", {"x": "foo"})


def test_group_views_legacy():
    """
    Test group_views_legacy - groups views by dimensions and combines indicators.

    Example: Two views with same country dimension get grouped into one view
    with a list of indicators. This function is deprecated.
    """
    from etl.collection.utils import group_views_legacy

    views = [
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind1"}},
        {"dimensions": {"country": "a"}, "indicators": {"y": "ind2"}},
        {"dimensions": {"country": "b"}, "indicators": {"y": "ind3"}},
    ]
    grouped = group_views_legacy(views, by=["country"])
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
        group_views_legacy([err_view], by=["country"])


def test_records_to_dictionary_and_unique_records():
    """
    Test records_to_dictionary and unique_records - data transformation utilities.

    records_to_dictionary: Converts [{"id": 1, "name": "A"}] to {1: {"name": "A"}}
    unique_records: Removes duplicate records while preserving order
    """
    from etl.collection.utils import records_to_dictionary, unique_records

    recs = [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
        {"id": 1, "v": "a"},
    ]
    dic = records_to_dictionary(recs, "id")
    assert dic == {1: {"v": "a"}, 2: {"v": "b"}}

    uniq = unique_records(recs)
    assert uniq == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
    ]
