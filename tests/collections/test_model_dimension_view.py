"""
Tests for ETL collection model components: Dimension and View classes.

This module tests the functionality of dimension and view models used in the ETL
collection system. These models are part of the data visualization and filtering
system that allows users to interact with datasets through different dimensions
and view configurations.

Key concepts tested:
- Dimension: Represents a categorical dimension (e.g., Age, Country) with choices
- View: Represents a data visualization configuration with indicators and dimensions
- Indicator: Represents a data path to a specific metric in the catalog
"""

import pytest

from etl.collection.exceptions import DuplicateValuesError, ExtraIndicatorsInUseError, MissingChoiceError
from etl.collection.model.dimension import (
    Dimension,
    DimensionChoice,
    DimensionPresentation,
    DimensionPresentationUIType,
)
from etl.collection.model.view import Indicator, View, ViewIndicators


def make_dimension():
    """
    Factory function to create a test Dimension instance.

    Creates an "Age" dimension with three choices (old, young, adult) and
    a dropdown presentation type. This provides a consistent test fixture
    for dimension-related tests.

    Returns:
        Dimension: A configured dimension instance for testing
    """
    return Dimension(
        slug="age",
        name="Age",
        description="Age groups of individuals",
        choices=[
            DimensionChoice(slug="old", name="Old"),
            DimensionChoice(slug="young", name="Young"),
            DimensionChoice(slug="adult", name="Adult"),
        ],
        presentation=DimensionPresentation(type=DimensionPresentationUIType.DROPDOWN),
    )


def test_sort_choices():
    """
    Test the sort_choices method of Dimension class.

    The sort_choices method allows reordering dimension choices either by:
    1. Providing a list of slugs in the desired order
    2. Providing a function that takes slugs and returns them in desired order

    This is useful for controlling the display order of dimension choices in UIs.
    """
    dim = make_dimension()

    # Test sorting with explicit order list
    # Original order: ["old", "young", "adult"]
    # Desired order: ["young", "adult", "old"]
    dim.sort_choices(["young", "adult", "old"])
    assert [c.slug for c in dim.choices] == ["young", "adult", "old"]

    # Test sorting with a function (alphabetical sorting)
    # The function receives all slugs and should return them in desired order
    dim.sort_choices(lambda slugs: sorted(slugs))
    assert [c.slug for c in dim.choices] == ["adult", "old", "young"]


def test_sort_choices_missing_slug():
    """
    Test that sort_choices raises ValueError when not all slugs are provided.

    When sorting with an explicit list, all existing choice slugs must be included.
    If any are missing, the method should raise a ValueError to prevent data loss.

    This ensures that dimension choices are never accidentally removed during sorting.
    """
    dim = make_dimension()
    # Dimension has choices: ["old", "young", "adult"]
    # But we only provide ["young", "old"], missing "adult"
    with pytest.raises(MissingChoiceError):
        dim.sort_choices(["young", "old"])  # missing 'adult'


def test_unique_validations():
    """
    Test validation methods that ensure uniqueness of choice names and slugs.

    Dimensions must have unique slugs and names across all their choices to:
    1. Prevent ambiguity in data filtering and selection
    2. Ensure proper UI rendering without conflicts
    3. Maintain data integrity in the collection system

    Tests both successful validation and expected failures with duplicates.
    """
    # Test successful validation with unique names and slugs
    dim = make_dimension()
    dim.validate_choice_names_unique()  # Should pass - all names are unique
    dim.validate_choice_slugs_unique()  # Should pass - all slugs are unique

    # Test validation failure with duplicate name
    # Create dimension with duplicate name "Old" (different slugs)
    dim_dup_name = make_dimension()
    dim_dup_name.choices.append(DimensionChoice(slug="child", name="Old"))  # "Old" already exists
    with pytest.raises(DuplicateValuesError):
        dim_dup_name.validate_choice_names_unique()

    # Test validation failure with duplicate slug
    # Create dimension with duplicate slug "old" (different names)
    dim_dup_slug = make_dimension()
    dim_dup_slug.choices.append(DimensionChoice(slug="old", name="Very Old"))  # "old" already exists
    with pytest.raises(DuplicateValuesError):
        dim_dup_slug.validate_choice_slugs_unique()


def test_indicator_expand_path():
    """
    Test the expand_path method of Indicator class.

    Indicators start with short-form paths like "table#value" and need to be expanded
    to full catalog paths like "grapher/ns/latest/ds/table#value" for data retrieval.

    The expand_path method uses a mapping dictionary to convert table names to their
    full catalog paths, preserving the variable/column reference after the '#'.

    This is essential for resolving data references in the collection system.
    """
    # Create indicator with short-form path
    indicator = Indicator("table#value")

    # Mapping from table name to full catalog path
    mapping = {"table": ["grapher/ns/latest/ds/table"]}

    # Expand the path using the mapping
    indicator.expand_path(mapping)

    # Verify the path was properly expanded
    assert indicator.catalogPath == "grapher/ns/latest/ds/table#value"


def test_view_indicators_from_dict_and_to_records():
    """
    Test ViewIndicators serialization and deserialization methods.

    ViewIndicators manages a collection of indicators mapped to chart axes.
    This tests the round-trip conversion:
    1. from_dict: Creates ViewIndicators from a dictionary mapping axes to paths
    2. to_records: Converts back to a list of record dictionaries

    This is used for storing and retrieving view configurations in the system.
    The 'display' field stores chart-specific formatting options.
    """
    # Input dictionary mapping chart axes to indicator paths
    data = {"y": "table#ind1", "x": "table#ind2"}

    # Create ViewIndicators object from dictionary
    vi = ViewIndicators.from_dict(data)

    # Convert back to record format
    records = vi.to_records()

    # Verify the structure matches expected format
    assert records == [
        {"path": "table#ind1", "axis": "y", "display": {}},  # y-axis indicator
        {"path": "table#ind2", "axis": "x", "display": {}},  # x-axis indicator
    ]


def test_view_expand_paths_and_indicators_used():
    """
    Test View path expansion and indicator discovery functionality.

    This test covers two key View methods:
    1. expand_paths: Converts short-form indicator paths to full catalog paths
    2. indicators_used: Discovers all indicators referenced in the view configuration

    Views can reference indicators in multiple places:
    - Explicit indicators (in the indicators field)
    - Config references (like sortColumnSlug for sorting)

    The test demonstrates handling of "extra" indicators found in config that aren't
    explicitly declared in the indicators field.
    """
    # Create a view with indicators and config references
    view = View(
        dimensions={"d": "a"},  # Dimension mapping
        indicators=ViewIndicators.from_dict({"y": "table#ind1"}),  # Explicit y-axis indicator
        config={"sortColumnSlug": "other#ind2"},  # Additional indicator used for sorting
    )

    # Mapping to expand short table names to full catalog paths
    mapping = {
        "table": ["grapher/ns/latest/ds/table"],
        "other": ["grapher/ns/latest/ds/other"],
    }

    # Expand all paths in the view
    view.expand_paths(mapping)

    # Test strict mode: should raise ValueError because sortColumnSlug references
    # an indicator ("other#ind2") that's not in the explicit indicators list
    with pytest.raises(ExtraIndicatorsInUseError):
        view.indicators_used()

    # Test tolerant mode: should return all indicators including config references
    paths = view.indicators_used(tolerate_extra_indicators=True)
    assert set(paths) == {
        "grapher/ns/latest/ds/table#ind1",  # From explicit indicators
        "grapher/ns/latest/ds/other#ind2",  # From config.sortColumnSlug
    }


def test_view_matches_single_dimension_exact():
    """
    Test View.matches with single dimension exact matching.

    Example: View with dimensions {"country": "usa", "age": "adult"} should match
    country="usa" but not country="uk".
    """
    view = View(
        dimensions={"country": "usa", "age": "adult"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Should match exact dimension value
    assert view.matches(country="usa")
    assert view.matches(age="adult")

    # Should not match different values
    assert not view.matches(country="uk")
    assert not view.matches(age="child")


def test_view_matches_multiple_dimensions():
    """
    Test View.matches with multiple dimension matching.

    Example: View should match only when ALL specified dimensions match.
    """
    view = View(
        dimensions={"country": "usa", "age": "adult", "sex": "female"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Should match when all specified dimensions match
    assert view.matches(country="usa", age="adult")
    assert view.matches(country="usa", sex="female")
    assert view.matches(age="adult", sex="female")
    assert view.matches(country="usa", age="adult", sex="female")

    # Should not match when any dimension doesn't match
    assert not view.matches(country="usa", age="child")  # age mismatch
    assert not view.matches(country="uk", age="adult")  # country mismatch
    assert not view.matches(country="usa", age="adult", sex="male")  # sex mismatch


def test_view_matches_list_values():
    """
    Test View.matches with list of acceptable values.

    Example: matches(age=["adult", "child"]) should match if view.age is either "adult" OR "child".
    """
    view_adult = View(
        dimensions={"country": "usa", "age": "adult"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    view_child = View(
        dimensions={"country": "usa", "age": "child"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    view_elderly = View(
        dimensions={"country": "usa", "age": "elderly"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Should match when view's dimension value is in the list
    assert view_adult.matches(age=["adult", "child"])
    assert view_child.matches(age=["adult", "child"])

    # Should not match when view's dimension value is not in the list
    assert not view_elderly.matches(age=["adult", "child"])

    # Test with multiple dimensions, some with lists
    assert view_adult.matches(country="usa", age=["adult", "child"])
    assert not view_adult.matches(country="uk", age=["adult", "child"])  # country mismatch


def test_view_matches_numeric_values():
    """
    Test View.matches with numeric values (int, float).

    Example: Views with numeric dimension values should match properly.
    """
    view = View(
        dimensions={"year": "2023", "score": "95.5", "rank": "1"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Should match numeric values (note: dimensions are stored as strings)
    assert view.matches(year="2023")
    assert view.matches(score="95.5")
    assert view.matches(rank="1")

    # Should not match different numeric values
    assert not view.matches(year="2022")
    assert not view.matches(score="95.6")
    assert not view.matches(rank="2")


def test_view_matches_empty_list():
    """
    Test View.matches with empty list (edge case).

    Example: matches(age=[]) should return True as empty list means no restrictions.
    This makes sense - an empty list means "don't filter on this dimension".
    """
    view = View(
        dimensions={"country": "usa", "age": "adult"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Empty list should match anything (no restrictions)
    assert view.matches(age=[])
    assert view.matches(country=[])
    assert view.matches(age=[], country="usa")  # Mix of empty list and exact match


def test_view_matches_mixed_types():
    """
    Test View.matches with mixed argument types (strings and lists).

    Example: matches(country="usa", age=["adult", "child"]) mixes string and list matching.
    """
    view = View(
        dimensions={"country": "usa", "age": "adult", "sex": "female"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Mix of exact match and list match
    assert view.matches(country="usa", age=["adult", "child"])
    assert view.matches(country=["usa", "canada"], age="adult")
    assert view.matches(country=["usa", "canada"], age=["adult", "child"])

    # Should fail if any condition fails
    assert not view.matches(country="uk", age=["adult", "child"])  # country mismatch
    assert not view.matches(country=["canada", "mexico"], age="adult")  # country not in list
    assert not view.matches(country="usa", age=["child", "elderly"])  # age not in list


def test_view_matches_nonexistent_dimension():
    """
    Test View.matches raises ValueError for non-existent dimensions.

    Example: Trying to match on dimension not present in view should raise ValueError.
    """
    view = View(
        dimensions={"country": "usa", "age": "adult"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # Should raise ValueError for dimension not in view
    with pytest.raises(ValueError, match="Dimension 'region' not found in view dimensions"):
        view.matches(region="north_america")

    with pytest.raises(ValueError, match="Dimension 'income' not found in view dimensions"):
        view.matches(country="usa", income="high")


def test_view_matches_string_vs_bytes():
    """
    Test View.matches properly handles string vs bytes distinction.

    Example: String values should be treated as single values, not iterables.
    """
    view = View(
        dimensions={"country": "usa", "category": "health"},
        indicators=ViewIndicators.from_dict({"y": "table#indicator"}),
    )

    # String should be treated as single value, not iterable
    assert view.matches(country="usa")  # exact string match
    assert view.matches(category="health")  # exact string match

    # Should not match substring iterations
    assert not view.matches(country="us")  # partial match should fail
    assert not view.matches(category="heal")  # partial match should fail


def test_view_matches_complex_scenario():
    """
    Test View.matches with complex real-world scenario.

    Example: Multi-dimensional view with various matching patterns as might be used
    in an actual data collection filtering scenario.
    """
    view = View(
        dimensions={"country": "usa", "age_group": "adult", "sex": "female", "year": "2023", "metric": "population"},
        indicators=ViewIndicators.from_dict({"y": "table#population_count"}),
    )

    # Various realistic matching scenarios
    assert view.matches(country="usa")  # Filter by country only
    assert view.matches(country="usa", year="2023")  # Filter by country and year
    assert view.matches(sex="female", metric="population")  # Filter by sex and metric
    assert view.matches(age_group=["adult", "elderly"], sex="female")  # Mix of list and exact

    # Complex multi-criteria filtering
    assert view.matches(country=["usa", "canada"], age_group="adult", year=["2022", "2023"])

    # Should fail when criteria don't match
    assert not view.matches(country="usa", sex="male")  # sex mismatch
    assert not view.matches(country=["canada", "mexico"])  # country not in list
    assert not view.matches(year=["2020", "2021", "2022"])  # year not in list
