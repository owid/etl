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
