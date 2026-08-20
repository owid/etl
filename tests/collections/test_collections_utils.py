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
    with pytest.warns(DeprecationWarning, match="group_views_legacy"):
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
    with pytest.warns(DeprecationWarning, match="group_views_legacy"), pytest.raises(NotImplementedError):
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


def _create_table_with_dimensions():
    """Create a table with two indicators over dimensions sex and equivalence_scale.

    Mirrors the shape that motivates filter_columns_by_dimension_choices: a table where only one
    value of a dimension (equivalence_scale="square root") is wanted in a collection.
    """
    from owid.catalog import Table, Variable
    from owid.catalog.core.meta import VariableMeta

    data = {
        "country": ["USA", "CAN"],
        "year": [2020, 2020],
        "income__sex_male__scale_sqrt": [1, 2],
        "income__sex_female__scale_sqrt": [3, 4],
        "income__sex_male__scale_none": [5, 6],
        "income__sex_female__scale_none": [7, 8],
    }
    tb = Table(data, short_name="test_table")
    for col in data:
        if col in ("country", "year"):
            continue
        tb[col] = Variable(
            tb[col],
            name=col,
            metadata=VariableMeta(
                original_short_name="income",
                dimensions={
                    "sex": "female" if "female" in col else "male",
                    "equivalence_scale": "square root" if "sqrt" in col else "none",
                },
            ),
        )
    tb.metadata.dimensions = [
        {"slug": "sex", "name": "Sex"},
        {"slug": "equivalence_scale", "name": "Equivalence scale"},
    ]
    return tb


def test_filter_columns_by_dimension_choices():
    """
    Test filter_columns_by_dimension_choices - filters indicators to one dimension choice and
    removes the dimension from column-level and table-level metadata.
    """
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    result = filter_columns_by_dimension_choices(tb, {"equivalence_scale": "square root"})

    # Only the matching indicators are kept; non-dimensional columns (country, year) too.
    assert sorted(result.columns) == [
        "country",
        "income__sex_female__scale_sqrt",
        "income__sex_male__scale_sqrt",
        "year",
    ]

    # The dimension is dropped from the metadata of the kept indicators; other dimensions remain.
    assert result["income__sex_male__scale_sqrt"].m.dimensions == {"sex": "male"}
    assert result["income__sex_female__scale_sqrt"].m.dimensions == {"sex": "female"}

    # The dimension is dropped from the table-level metadata.
    assert result.metadata.dimensions == [{"slug": "sex", "name": "Sex"}]

    # The input table is not modified.
    assert "income__sex_male__scale_none" in tb.columns
    assert tb["income__sex_male__scale_sqrt"].m.dimensions == {"sex": "male", "equivalence_scale": "square root"}
    assert len(tb.metadata.dimensions) == 2


def test_filter_columns_by_dimension_choices_expands_without_the_dimension():
    """
    Test that a table processed with filter_columns_by_dimension_choices expands into a collection
    config without the dropped dimension (the motivating use case, see issue #5670).
    """
    from etl.collection import expand_config
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    result = filter_columns_by_dimension_choices(tb, {"equivalence_scale": "square root"})
    config = expand_config(result, indicator_names="income")

    # Only the sex dimension is left, so no single-option dropdown is rendered.
    assert [dim["slug"] for dim in config["dimensions"]] == ["sex"]
    assert len(config["views"]) == 2
    for view in config["views"]:
        assert "equivalence_scale" not in view["dimensions"]


def test_filter_columns_by_dimension_choices_errors():
    """
    Test filter_columns_by_dimension_choices error cases - unknown dimension and unknown choice.
    """
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    with pytest.raises(ValueError, match="Dimension 'welfare' not found"):
        filter_columns_by_dimension_choices(tb, {"welfare": "income"})

    with pytest.raises(ValueError, match="Available choices"):
        filter_columns_by_dimension_choices(tb, {"equivalence_scale": "oecd"})

    # An indicator that has dimensions but not the one being filtered cannot be filtered, and keeping it
    # would add views for choices the caller excluded. It must fail instead.
    from owid.catalog import Variable
    from owid.catalog.core.meta import VariableMeta

    tb["income__sex_male"] = Variable(
        tb["income__sex_male__scale_sqrt"],
        name="income__sex_male",
        metadata=VariableMeta(original_short_name="income", dimensions={"sex": "male"}),
    )
    with pytest.raises(ValueError, match="have dimensions, but not 'equivalence_scale'"):
        filter_columns_by_dimension_choices(tb, {"equivalence_scale": "square root"})


def test_filter_columns_by_dimension_choices_keeps_several_choices():
    """
    Test filter_columns_by_dimension_choices with a list of choices - a dimension that keeps more
    than one choice is not dropped, since its dropdown still has something to choose from.
    """
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    result = filter_columns_by_dimension_choices(tb, {"sex": ["male", "female"]})

    # Nothing is filtered out, since both choices of sex are kept.
    assert sorted(result.columns) == sorted(tb.columns)

    # sex is kept (two choices), equivalence_scale too (also two choices).
    assert result["income__sex_male__scale_sqrt"].m.dimensions == {"sex": "male", "equivalence_scale": "square root"}
    assert [dim["slug"] for dim in result.metadata.dimensions or []] == ["sex", "equivalence_scale"]


def test_filter_columns_by_dimension_choices_several_dimensions():
    """
    Test filter_columns_by_dimension_choices with more than one dimension - a column is kept only if
    it matches every given dimension, and both dimensions end up with a single choice, so both go.
    """
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    result = filter_columns_by_dimension_choices(tb, {"sex": "female", "equivalence_scale": "square root"})

    assert sorted(result.columns) == ["country", "income__sex_female__scale_sqrt", "year"]
    assert result["income__sex_female__scale_sqrt"].m.dimensions == {}
    assert result.metadata.dimensions == []


def test_filter_columns_by_dimension_choices_without_dropping_dimensions():
    """
    Test filter_columns_by_dimension_choices with drop_single_choice_dimensions=False - the columns
    are filtered, but the dimension is left in the metadata.
    """
    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = _create_table_with_dimensions()

    result = filter_columns_by_dimension_choices(
        tb, {"equivalence_scale": "square root"}, drop_single_choice_dimensions=False
    )

    assert sorted(result.columns) == [
        "country",
        "income__sex_female__scale_sqrt",
        "income__sex_male__scale_sqrt",
        "year",
    ]
    assert result["income__sex_male__scale_sqrt"].m.dimensions == {"sex": "male", "equivalence_scale": "square root"}
    assert [dim["slug"] for dim in result.metadata.dimensions or []] == ["sex", "equivalence_scale"]


def test_filter_columns_by_dimension_choices_with_non_string_choices():
    """
    Test filter_columns_by_dimension_choices with integer choices - dimension choices are not always
    strings (e.g. ppp_version=2021 in the World Bank PIP collections).
    """
    from owid.catalog import Table, Variable
    from owid.catalog.core.meta import VariableMeta

    from etl.collection.utils import filter_columns_by_dimension_choices

    tb = Table(
        {"country": ["USA"], "year": [2020], "poverty__ppp_2017": [1], "poverty__ppp_2021": [2]},
        short_name="test_table",
    )
    for ppp_version in [2017, 2021]:
        column = f"poverty__ppp_{ppp_version}"
        tb[column] = Variable(
            tb[column],
            name=column,
            metadata=VariableMeta(original_short_name="poverty", dimensions={"ppp_version": ppp_version}),
        )
    tb.metadata.dimensions = [{"slug": "ppp_version", "name": "PPP version"}]

    result = filter_columns_by_dimension_choices(tb, {"ppp_version": 2021})

    assert sorted(result.columns) == ["country", "poverty__ppp_2021", "year"]
    assert result["poverty__ppp_2021"].m.dimensions == {}
    assert result.metadata.dimensions == []


def test_resolve_grapher_schema_accepted_forms():
    """
    Test resolve_grapher_schema - both authoring forms resolve to a full schema URL.

    Short form "011" expands to the published URL; a full URL passes through unchanged;
    None falls back to the version this repo vendors (DEFAULT_GRAPHER_SCHEMA).
    """
    from etl.collection.utils import resolve_grapher_schema
    from etl.config import DEFAULT_GRAPHER_SCHEMA

    assert resolve_grapher_schema("011") == "https://files.ourworldindata.org/schemas/grapher-schema.011.json"
    # An older pin is preserved — that is the point of pinning: Grapher migrates it forward.
    assert resolve_grapher_schema("008") == "https://files.ourworldindata.org/schemas/grapher-schema.008.json"

    full = "https://files.ourworldindata.org/schemas/grapher-schema.010.json"
    assert resolve_grapher_schema(full) == full

    assert resolve_grapher_schema(None) == DEFAULT_GRAPHER_SCHEMA


def test_resolve_grapher_schema_rejects_unquoted_yaml_version():
    """
    Test resolve_grapher_schema - a bare YAML version is rejected with a quoting hint.

    YAML 1.1 parses `grapher_schema: 011` as octal, so it arrives as 9 (or "9" once the dataclass
    coerces it to str). Silently zero-padding that back to "009" would pin the wrong version, so we
    raise and point at the missing quotes.
    """
    from etl.collection.utils import resolve_grapher_schema

    for value in (9, "9", 11, "11"):
        with pytest.raises(ValueError, match="Quote it"):
            resolve_grapher_schema(value)


def test_resolve_grapher_schema_rejects_malformed_values():
    """
    Test resolve_grapher_schema - anything that isn't a 3-digit version or a schema URL is rejected.

    Example: "latest" and a bare filename both fail, since Grapher keys its config migrations on a
    concrete version.
    """
    from etl.collection.utils import resolve_grapher_schema

    for value in ("latest", "0.11", "grapher-schema.011.json", "https://example.com/schema.json", ""):
        with pytest.raises(ValueError, match="Invalid `grapher_schema` value"):
            resolve_grapher_schema(value)
