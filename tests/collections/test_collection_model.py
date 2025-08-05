"""
Tests for Collection model from etl.collection.model.core.

This module tests the core functionality of the Collection class including
initialization, validation, view management, dimension handling, and data export.
It also tests the CollectionSet class for managing multiple collections.
"""

import warnings
from pathlib import Path
from unittest.mock import patch

import pytest

from etl.collection.core.collection_set import CollectionSet
from etl.collection.exceptions import DuplicateCollectionViews, DuplicateValuesError
from etl.collection.model.core import Collection, Definitions
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.model.view import Indicator, View, ViewIndicators


def test_collection_from_dict_basic():
    """
    Test Collection.from_dict - creates Collection from dictionary configuration.

    Example: Standard config dict becomes a Collection with proper field mapping
    (definitions -> _definitions, default_dimensions -> _default_dimensions)
    """
    config = {
        "catalog_path": "test/latest/data#table",
        "title": {"en": "Test Collection"},
        "default_selection": ["country"],
        "dimensions": [{"slug": "country", "name": "Country", "choices": [{"slug": "usa", "name": "United States"}]}],
        "views": [{"dimensions": {"country": "usa"}, "indicators": {"y": [{"catalogPath": "test#indicator"}]}}],
        "definitions": {"common_views": []},
        "default_dimensions": {"country": "usa"},
    }

    collection = Collection.from_dict(config)

    assert collection.catalog_path == "test/latest/data#table"
    assert collection.title == {"en": "Test Collection"}
    assert collection.default_selection == ["country"]
    assert len(collection.dimensions) == 1
    assert len(collection.views) == 1
    assert collection._definitions is not None
    assert collection._default_dimensions == {"country": "usa"}


def test_collection_catalog_path_validation():
    """
    Test Collection catalog_path validation - must contain '#' separator.

    Example: "dataset/table" fails, "dataset#table" succeeds
    """
    config = {
        "catalog_path": "invalid_path_without_hash",
        "title": {"en": "Test"},
        "default_selection": [],
        "dimensions": [],
        "views": [],
        "_definitions": Definitions(),
    }

    with pytest.raises(AssertionError, match="Catalog path should be in the format"):
        Collection.from_dict(config)


def test_collection_properties():
    """
    Test Collection property accessors - short_name, dimension_slugs, etc.

    Example: catalog_path "data/latest/test#my_table" gives short_name "my_table"
    """
    collection = Collection(
        catalog_path="data/latest/test#my_table",
        title={"en": "Test"},
        default_selection=["dim1"],
        dimensions=[
            Dimension(slug="dim1", name="Dimension 1", choices=[DimensionChoice(slug="choice1", name="Choice 1")])
        ],
        views=[],
        _definitions=Definitions(),
    )

    assert collection.short_name == "my_table"
    assert collection.dimension_slugs == ["dim1"]
    assert collection.dimension_choices == {"dim1": ["choice1"]}


def test_collection_get_dimension():
    """
    Test Collection.get_dimension - retrieves dimension by slug.

    Example: get_dimension("country") returns the country Dimension object
    """
    dim = Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[dim],
        views=[],
        _definitions=Definitions(),
    )

    result = collection.get_dimension("country")
    assert result.slug == "country"
    assert result.name == "Country"

    with pytest.raises(ValueError, match="Dimension missing not found"):
        collection.get_dimension("missing")


def test_collection_get_choice_names():
    """
    Test Collection.get_choice_names - gets choice slug->name mapping for dimension.

    Example: country dimension returns {"usa": "United States", "uk": "United Kingdom"}
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[
                    DimensionChoice(slug="usa", name="United States"),
                    DimensionChoice(slug="uk", name="United Kingdom"),
                ],
            )
        ],
        views=[],
        _definitions=Definitions(),
    )

    choice_names = collection.get_choice_names("country")
    assert choice_names == {"usa": "United States", "uk": "United Kingdom"}


def test_collection_dimension_choices_in_use():
    """
    Test Collection.dimension_choices_in_use - finds choices actually used in views.

    Example: If views only use "usa" and "uk", returns {"country": {"usa", "uk"}}
    even if dimension has more choices defined
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[
                    DimensionChoice(slug="usa", name="USA"),
                    DimensionChoice(slug="uk", name="UK"),
                    DimensionChoice(slug="france", name="France"),  # Not used in views
                ],
            )
        ],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    choices_in_use = collection.dimension_choices_in_use()
    assert choices_in_use == {"country": {"usa", "uk"}}


def test_collection_prune_dimension_choices():
    """
    Test Collection.prune_dimension_choices - removes unused choices from dimensions.

    Example: Dimension with 3 choices but only 2 used in views gets pruned to 2 choices
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[
                    DimensionChoice(slug="usa", name="USA"),
                    DimensionChoice(slug="uk", name="UK"),
                    DimensionChoice(slug="france", name="France"),  # Unused
                ],
            )
        ],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    # Before pruning
    assert len(collection.dimensions[0].choices) == 3

    collection.prune_dimension_choices()

    # After pruning - unused choice removed
    assert len(collection.dimensions[0].choices) == 2
    choice_slugs = [c.slug for c in collection.dimensions[0].choices]
    assert "usa" in choice_slugs
    assert "uk" in choice_slugs
    assert "france" not in choice_slugs


def test_collection_prune_dimensions():
    """
    Test Collection.prune_dimensions - removes dimensions with only one choice in use.

    Example: Dimension with only one choice used gets removed from collection entirely
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(
                slug="metric",
                name="Metric",
                choices=[DimensionChoice(slug="cases", name="Cases"), DimensionChoice(slug="deaths", name="Deaths")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    # Before pruning
    assert len(collection.dimensions) == 2

    collection.prune_dimensions()

    # After pruning - country dimension removed (only usa used)
    assert len(collection.dimensions) == 1
    assert collection.dimensions[0].slug == "metric"

    # Views should also have country dimension removed
    for view in collection.views:
        assert "country" not in view.dimensions
        assert "metric" in view.dimensions


def test_collection_drop_views():
    """
    Test Collection.drop_views - removes views matching specified dimension filters.

    Example: drop_views({"country": "usa"}) removes all views with country=usa
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[DimensionChoice(slug="usa", name="USA"), DimensionChoice(slug="uk", name="UK")],
            ),
            Dimension(slug="metric", name="Metric", choices=[DimensionChoice(slug="cases", name="Cases")]),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    # Before dropping
    assert len(collection.views) == 2

    collection.drop_views({"country": "usa"})

    # After dropping - only UK view remains
    assert len(collection.views) == 1
    assert collection.views[0].dimensions["country"] == "uk"


def test_collection_check_duplicate_views():
    """
    Test Collection.check_duplicate_views - detects views with identical dimensions.

    Example: Two views with same dimensions should raise DuplicateCollectionViews
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),  # Duplicate
        ],
        _definitions=Definitions(),
    )

    with pytest.raises(DuplicateCollectionViews):
        collection.check_duplicate_views()


def test_collection_default_dimensions_setter():
    """
    Test Collection.default_dimensions setter - validates and sets default view.

    Example: Setting default to existing view succeeds, non-existing view fails
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    # Setting valid default dimensions should work
    collection.default_dimensions = {"country": "usa"}
    assert collection.default_dimensions == {"country": "usa"}

    # Setting invalid default dimensions should fail
    with pytest.raises(ValueError, match="no view matches these dimensions"):
        collection.default_dimensions = {"country": "nonexistent"}


def test_collection_indicators_in_use():
    """
    Test Collection.indicators_in_use - extracts all indicator paths from views.

    Example: Views with different indicators return all unique indicator paths
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[DimensionChoice(slug="usa", name="USA"), DimensionChoice(slug="uk", name="UK")],
            )
        ],
        views=[
            View(
                dimensions={"country": "usa"}, indicators=ViewIndicators(y=[Indicator(catalogPath="test#indicator1")])
            ),
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[Indicator(catalogPath="test#indicator2")])),
        ],
        _definitions=Definitions(),
    )

    indicators = collection.indicators_in_use()
    assert set(indicators) == {"test#indicator1", "test#indicator2"}


def test_collection_to_dict():
    """
    Test Collection.to_dict - converts Collection back to dictionary format.

    Example: Collection object becomes dict suitable for YAML export
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    result = collection.to_dict()

    assert result["catalog_path"] == "test#table"
    assert result["title"] == {"en": "Test"}
    assert len(result["dimensions"]) == 1
    assert len(result["views"]) == 1
    # Definitions should be dropped by default
    assert "_definitions" not in result
    assert "definitions" not in result


def test_snake_case_slugs():
    """
    Test Collection.snake_case_slugs - converts all slugs to snake_case format.

    Example: "United States" becomes "united_states", "GDP per capita" becomes "gdp_per_capita"
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(slug="country-name", name="Country", choices=[DimensionChoice(slug="united-states", name="USA")])
        ],
        views=[View(dimensions={"country-name": "united-states"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    collection.snake_case_slugs()

    # Dimension slug should be snake_case
    assert collection.dimensions[0].slug == "country_name"
    # Choice slug should be snake_case
    assert collection.dimensions[0].choices[0].slug == "united_states"
    # View dimensions should be updated
    assert collection.views[0].dimensions == {"country_name": "united_states"}


def test_sort_views_with_default_first():
    """
    Test Collection.sort_views_with_default_first - moves default view to front.

    Example: View matching default_dimensions gets moved to position 0
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[DimensionChoice(slug="usa", name="USA"), DimensionChoice(slug="uk", name="UK")],
            )
        ],
        views=[
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
        _default_dimensions={"country": "usa"},
    )

    # Before sorting - USA view is second
    assert collection.views[0].dimensions["country"] == "uk"
    assert collection.views[1].dimensions["country"] == "usa"

    collection.sort_views_with_default_first()

    # After sorting - USA view should be first
    assert collection.views[0].dimensions["country"] == "usa"
    assert collection.views[1].dimensions["country"] == "uk"


def test_validate_dimension_uniqueness_success():
    """
    Test Collection.validate_dimension_uniqueness - passes with unique dimension slugs.

    Example: Collection with dimensions having different slugs should pass validation
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(slug="metric", name="Metric", choices=[DimensionChoice(slug="cases", name="Cases")]),
            Dimension(slug="year", name="Year", choices=[DimensionChoice(slug="2023", name="2023")]),
        ],
        views=[View(dimensions={"country": "usa", "metric": "cases", "year": "2023"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    # Should not raise any exception
    collection.validate_dimension_uniqueness()


def test_validate_dimension_uniqueness_duplicate_slugs():
    """
    Test Collection.validate_dimension_uniqueness - fails with duplicate dimension slugs.

    Example: Collection with dimensions having same slug should raise DuplicateValuesError
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(
                slug="country", name="Another Country", choices=[DimensionChoice(slug="uk", name="UK")]
            ),  # Duplicate slug
        ],
        views=[View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    with pytest.raises(DuplicateValuesError, match="Dimension slug 'country' is not unique"):
        collection.validate_dimension_uniqueness()


def test_validate_dimension_uniqueness_empty_dimensions():
    """
    Test Collection.validate_dimension_uniqueness - passes with no dimensions.

    Example: Collection with empty dimensions list should pass validation
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=[],
        dimensions=[],
        views=[],
        _definitions=Definitions(),
    )

    # Should not raise any exception
    collection.validate_dimension_uniqueness()


def test_validate_dimension_uniqueness_single_dimension():
    """
    Test Collection.validate_dimension_uniqueness - passes with single dimension.

    Example: Collection with only one dimension should always pass validation
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
        ],
        views=[View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    # Should not raise any exception
    collection.validate_dimension_uniqueness()


def test_validate_dimension_uniqueness_multiple_duplicates():
    """
    Test Collection.validate_dimension_uniqueness - catches first duplicate when multiple exist.

    Example: Collection with multiple duplicate dimension slugs should raise error for first found
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(slug="country", name="Country 1", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(slug="metric", name="Metric 1", choices=[DimensionChoice(slug="cases", name="Cases")]),
            Dimension(
                slug="country", name="Country 2", choices=[DimensionChoice(slug="uk", name="UK")]
            ),  # First duplicate
            Dimension(
                slug="metric", name="Metric 2", choices=[DimensionChoice(slug="deaths", name="Deaths")]
            ),  # Second duplicate
        ],
        views=[View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[]))],
        _definitions=Definitions(),
    )

    # Should raise error for the first duplicate found (country)
    with pytest.raises(DuplicateValuesError, match="Dimension slug 'country' is not unique"):
        collection.validate_dimension_uniqueness()


# =============================================================================
# CollectionSet and grouped view validation tests
# =============================================================================


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
                ],
            ),
            Dimension(
                slug="age",
                name="Age",
                choices=[
                    DimensionChoice(slug="adults", name="Adults"),
                    DimensionChoice(slug="children", name="Children"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"sex": "male", "age": "adults"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator1"}]}),
            ),
            View(
                dimensions={"sex": "female", "age": "adults"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator2"}]}),
            ),
            View(
                dimensions={"sex": "male", "age": "children"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator3"}]}),
            ),
            View(
                dimensions={"sex": "female", "age": "children"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator4"}]}),
            ),
        ],
        catalog_path="test#collection",
        title={"title": "Test Collection"},
        default_selection=["test"],
        _definitions=Definitions(common_views=None),
    )

    # Group views by sex dimension (creates grouped views without metadata)
    collection.group_views([{"dimension": "sex", "choice_new_slug": "all_sexes", "choices": ["male", "female"]}])

    # Verify that grouped views were created and marked as grouped
    grouped_views = [view for view in collection.views if view.is_grouped]
    assert len(grouped_views) == 2  # Should have 2 grouped views (one for each age group)

    # Mock database validation to avoid DB dependency
    with patch("etl.collection.utils.validate_indicators_in_db"):
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
                ],
            )
        ],
        views=[
            View(
                dimensions={"category": "a"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator1"}]}),
            ),
            View(
                dimensions={"category": "b"},
                indicators=ViewIndicators.from_dict({"y": [{"catalogPath": "table#indicator2"}]}),
            ),
        ],
        catalog_path="test#collection2",
        title={"title": "Test Collection 2"},
        default_selection=["test"],
        _definitions=Definitions(common_views=None),
    )

    # Group views with partial metadata (missing description_short)
    collection.group_views(
        [
            {
                "dimension": "category",
                "choice_new_slug": "combined",
                "choices": ["a", "b"],
                "view_metadata": {
                    "description_key": ["Some key info"]  # Missing description_short
                },
            }
        ]
    )

    # Test validation
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        collection.validate_grouped_views()

        # Should have warning about missing description_short
        warning_messages = [str(warning.message) for warning in w]
        missing_desc_short = [msg for msg in warning_messages if "missing 'description_short'" in msg]
        assert len(missing_desc_short) >= 1


def test_validate_indicators_are_from_dependencies_success():
    """
    Test Collection.validate_indicators_are_from_dependencies - passes when indicators match dependencies.

    Example: If collection has dependency "data://grapher/ns/2023/dataset" and uses
    indicator "grapher/ns/2023/dataset/table#column", validation should pass.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[
            View(
                dimensions={"country": "usa"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="grapher/ns/2023/dataset/table#column")]),
            )
        ],
        dependencies={"data://grapher/ns/2023/dataset"},
        _definitions=Definitions(),
    )

    # Get indicators in use
    indicators = collection.indicators_in_use()

    # Should pass validation
    result = collection.validate_indicators_are_from_dependencies(indicators)
    assert result is True


def test_validate_indicators_are_from_dependencies_failure():
    """
    Test Collection.validate_indicators_are_from_dependencies - fails when indicators don't match dependencies.

    Example: If collection has dependency "data://grapher/ns/2023/dataset" but uses
    indicator from "grapher/other/2023/otherset", validation should fail.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[
            View(
                dimensions={"country": "usa"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="grapher/other/2023/otherset/table#column")]),
            )
        ],
        dependencies={"data://grapher/ns/2023/dataset"},
        _definitions=Definitions(),
    )

    # Get indicators in use
    indicators = collection.indicators_in_use()

    # Should fail validation
    with pytest.raises(
        ValueError, match="Indicator grapher/other/2023/otherset/table#column is not covered by any dependency"
    ):
        collection.validate_indicators_are_from_dependencies(indicators)


def test_validate_indicators_are_from_dependencies_multiple_dependencies():
    """
    Test Collection.validate_indicators_are_from_dependencies - works with multiple dependencies.

    Example: Collection with multiple dependencies should validate indicators from any of them.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(slug="metric", name="Metric", choices=[DimensionChoice(slug="cases", name="Cases")]),
        ],
        views=[
            View(
                dimensions={"country": "usa", "metric": "cases"},
                indicators=ViewIndicators(
                    y=[
                        Indicator(catalogPath="grapher/ns1/2023/dataset1/table#column1"),
                        Indicator(catalogPath="grapher/ns2/2023/dataset2/table#column2"),
                    ]
                ),
            )
        ],
        dependencies={"data://grapher/ns1/2023/dataset1", "data://grapher/ns2/2023/dataset2"},
        _definitions=Definitions(),
    )

    # Get indicators in use
    indicators = collection.indicators_in_use()

    # Should pass validation - both indicators covered by dependencies
    result = collection.validate_indicators_are_from_dependencies(indicators)
    assert result is True


def test_validate_indicators_are_from_dependencies_partial_match():
    """
    Test Collection.validate_indicators_are_from_dependencies - fails when only some indicators match.

    Example: If one indicator matches dependencies but another doesn't, validation should fail.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")]),
            Dimension(slug="metric", name="Metric", choices=[DimensionChoice(slug="cases", name="Cases")]),
        ],
        views=[
            View(
                dimensions={"country": "usa", "metric": "cases"},
                indicators=ViewIndicators(
                    y=[
                        Indicator(catalogPath="grapher/ns1/2023/dataset1/table#column1"),  # Covered
                        Indicator(catalogPath="grapher/other/2023/uncovered/table#column2"),  # Not covered
                    ]
                ),
            )
        ],
        dependencies={"data://grapher/ns1/2023/dataset1"},
        _definitions=Definitions(),
    )

    # Get indicators in use
    indicators = collection.indicators_in_use()

    # Should fail validation for the uncovered indicator
    with pytest.raises(
        ValueError, match="Indicator grapher/other/2023/uncovered/table#column2 is not covered by any dependency"
    ):
        collection.validate_indicators_are_from_dependencies(indicators)


def test_validate_indicators_are_from_dependencies_empty_dependencies():
    """
    Test Collection.validate_indicators_are_from_dependencies - fails when no dependencies set.

    Example: Collection with indicators but no dependencies should fail validation.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[
            View(
                dimensions={"country": "usa"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="grapher/ns/2023/dataset/table#column")]),
            )
        ],
        dependencies=set(),
        _definitions=Definitions(),
    )

    # Get indicators in use
    indicators = collection.indicators_in_use()

    # Should fail validation
    with pytest.raises(
        ValueError, match="Indicator grapher/ns/2023/dataset/table#column is not covered by any dependency"
    ):
        collection.validate_indicators_are_from_dependencies(indicators)


def test_validate_indicators_are_from_dependencies_empty_indicators():
    """
    Test Collection.validate_indicators_are_from_dependencies - passes when no indicators used.

    Example: Collection with no indicators should pass validation regardless of dependencies.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[Dimension(slug="country", name="Country", choices=[DimensionChoice(slug="usa", name="USA")])],
        views=[View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[]))],
        dependencies={"data://grapher/ns/2023/dataset"},
        _definitions=Definitions(),
    )

    # Get indicators in use (should be empty)
    indicators = collection.indicators_in_use()
    assert len(indicators) == 0

    # Should pass validation
    result = collection.validate_indicators_are_from_dependencies(indicators)
    assert result is True
