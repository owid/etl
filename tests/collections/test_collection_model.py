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


def test_collection_drop_views_single_key_value():
    """
    Test Collection.drop_views - removes views matching a single dimension filter.

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


def test_collection_drop_views_multiple_key_value_pairs():
    """
    Test Collection.drop_views with multiple key-value pairs (AND logic).

    Example: drop_views({"country": "usa", "metric": "cases"}) removes views
    that have BOTH country=usa AND metric=cases.
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
            Dimension(
                slug="metric",
                name="Metric",
                choices=[DimensionChoice(slug="cases", name="Cases"), DimensionChoice(slug="deaths", name="Deaths")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    assert len(collection.views) == 4

    # Drop views where country=usa AND metric=cases (only 1 view matches)
    collection.drop_views({"country": "usa", "metric": "cases"})

    assert len(collection.views) == 3
    remaining_dims = [(v.dimensions["country"], v.dimensions["metric"]) for v in collection.views]
    assert ("usa", "cases") not in remaining_dims
    assert ("usa", "deaths") in remaining_dims
    assert ("uk", "cases") in remaining_dims
    assert ("uk", "deaths") in remaining_dims


def test_collection_drop_views_list_values_or_logic():
    """
    Test Collection.drop_views with list values (OR logic within a dimension).

    Example: drop_views({"country": ["usa", "uk"]}) removes views that have
    country=usa OR country=uk.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[
                    DimensionChoice(slug="usa", name="USA"),
                    DimensionChoice(slug="uk", name="UK"),
                    DimensionChoice(slug="france", name="France"),
                ],
            ),
            Dimension(slug="metric", name="Metric", choices=[DimensionChoice(slug="cases", name="Cases")]),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "france", "metric": "cases"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    assert len(collection.views) == 3

    # Drop views where country is "usa" OR "uk"
    collection.drop_views({"country": ["usa", "uk"]})

    assert len(collection.views) == 1
    assert collection.views[0].dimensions["country"] == "france"


def test_collection_drop_views_list_of_dicts_or_logic():
    """
    Test Collection.drop_views with list of dicts (OR logic between dicts).

    Example: drop_views([{"country": "usa", "metric": "cases"}, {"country": "uk", "metric": "deaths"}])
    removes views that match the first dict OR the second dict.
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
            Dimension(
                slug="metric",
                name="Metric",
                choices=[DimensionChoice(slug="cases", name="Cases"), DimensionChoice(slug="deaths", name="Deaths")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    assert len(collection.views) == 4

    # Drop views matching (country=usa AND metric=cases) OR (country=uk AND metric=deaths)
    collection.drop_views([{"country": "usa", "metric": "cases"}, {"country": "uk", "metric": "deaths"}])

    assert len(collection.views) == 2
    remaining_dims = [(v.dimensions["country"], v.dimensions["metric"]) for v in collection.views]
    assert ("usa", "cases") not in remaining_dims
    assert ("uk", "deaths") not in remaining_dims
    assert ("usa", "deaths") in remaining_dims
    assert ("uk", "cases") in remaining_dims


def test_collection_drop_views_combined_list_and_or_logic():
    """
    Test Collection.drop_views with combined list of dicts and list values.

    Example: drop_views([{"country": ["usa", "uk"], "metric": "cases"}])
    removes views that have (country=usa OR country=uk) AND metric=cases.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country", "metric"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[
                    DimensionChoice(slug="usa", name="USA"),
                    DimensionChoice(slug="uk", name="UK"),
                    DimensionChoice(slug="france", name="France"),
                ],
            ),
            Dimension(
                slug="metric",
                name="Metric",
                choices=[DimensionChoice(slug="cases", name="Cases"), DimensionChoice(slug="deaths", name="Deaths")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "france", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "france", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    assert len(collection.views) == 6

    # Drop views where (country=usa OR country=uk) AND metric=cases
    collection.drop_views([{"country": ["usa", "uk"], "metric": "cases"}])

    assert len(collection.views) == 4
    remaining_dims = [(v.dimensions["country"], v.dimensions["metric"]) for v in collection.views]
    assert ("usa", "cases") not in remaining_dims
    assert ("uk", "cases") not in remaining_dims
    assert ("usa", "deaths") in remaining_dims
    assert ("uk", "deaths") in remaining_dims
    assert ("france", "cases") in remaining_dims
    assert ("france", "deaths") in remaining_dims


def test_collection_drop_views_invalid_dimension_slug():
    """
    Test Collection.drop_views raises ValueError for invalid dimension slugs.

    Example: drop_views({"invalid_dim": "value"}) should raise ValueError
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
            ),
        ],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    with pytest.raises(ValueError, match="Invalid dimension slug.*invalid_dim.*in drop_views filter"):
        collection.drop_views({"invalid_dim": "value"})


def test_collection_drop_views_invalid_slug_in_list_of_dicts():
    """
    Test Collection.drop_views raises ValueError when invalid slug is in one of multiple dicts.

    Example: drop_views([{"country": "usa"}, {"invalid": "value"}]) should raise ValueError
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
            ),
        ],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    with pytest.raises(ValueError, match="Invalid dimension slug.*invalid.*in drop_views filter"):
        collection.drop_views([{"country": "usa"}, {"invalid": "value"}])


def test_collection_drop_views_multiple_invalid_slugs():
    """
    Test Collection.drop_views raises ValueError when multiple invalid slugs are provided.

    Example: drop_views({"invalid1": "v1", "invalid2": "v2"}) should raise ValueError
    mentioning multiple slugs.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["country"],
        dimensions=[
            Dimension(
                slug="country",
                name="Country",
                choices=[DimensionChoice(slug="usa", name="USA")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    with pytest.raises(ValueError, match="Invalid dimension slugs.*in drop_views filter"):
        collection.drop_views({"invalid1": "v1", "invalid2": "v2"})


def test_collection_drop_views_partial_dimension_filter():
    """
    Test Collection.drop_views with a filter that doesn't specify all dimensions.

    Example: With 2 dimensions (country, metric), drop_views({"country": "usa"})
    should drop all views with country=usa regardless of metric value.
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
            Dimension(
                slug="metric",
                name="Metric",
                choices=[DimensionChoice(slug="cases", name="Cases"), DimensionChoice(slug="deaths", name="Deaths")],
            ),
        ],
        views=[
            View(dimensions={"country": "usa", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "usa", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "cases"}, indicators=ViewIndicators(y=[])),
            View(dimensions={"country": "uk", "metric": "deaths"}, indicators=ViewIndicators(y=[])),
        ],
        _definitions=Definitions(),
    )

    assert len(collection.views) == 4

    # Drop all views where country=usa (both cases and deaths)
    collection.drop_views({"country": "usa"})

    assert len(collection.views) == 2
    for view in collection.views:
        assert view.dimensions["country"] == "uk"


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


# =============================================================================
# Tests for Collection.group_views with replace=True
# =============================================================================


def test_group_views_replace_removes_original_views():
    """
    Test Collection.group_views with replace=True removes original views.

    When replace=True, the views with the original grouped choices should be removed,
    leaving only the newly created grouped views.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["sex", "age"],
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
                    DimensionChoice(slug="young", name="Young"),
                    DimensionChoice(slug="old", name="Old"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"sex": "male", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"sex": "female", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"sex": "male", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
            View(
                dimensions={"sex": "female", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind4")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before grouping: 4 views (2 sex choices x 2 age choices)
    assert len(collection.views) == 4

    # Group views with replace=True - should remove male/female views and add combined
    collection.group_views(
        [
            {
                "dimension": "sex",
                "choices": ["male", "female"],
                "choice_new_slug": "combined",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=False,  # Keep dimension even with one choice
    )

    # After grouping: only 2 views remain (the combined views for each age)
    assert len(collection.views) == 2

    # All remaining views should have sex="combined"
    for view in collection.views:
        assert view.dimensions["sex"] == "combined"

    # Views should be for different age groups
    ages = {view.dimensions["age"] for view in collection.views}
    assert ages == {"young", "old"}


def test_group_views_replace_prunes_choices():
    """
    Test Collection.group_views with replace=True prunes unused choices from dimension.

    When replace=True, the original choices that were grouped should be removed
    from the dimension's choices list.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["metric"],
        dimensions=[
            Dimension(
                slug="metric",
                name="Metric",
                choices=[
                    DimensionChoice(slug="cases", name="Cases"),
                    DimensionChoice(slug="deaths", name="Deaths"),
                    DimensionChoice(slug="recovered", name="Recovered"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"metric": "cases"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"metric": "deaths"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"metric": "recovered"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before grouping: 3 choices
    assert len(collection.dimensions[0].choices) == 3

    # Group cases and deaths with replace=True
    collection.group_views(
        [
            {
                "dimension": "metric",
                "choices": ["cases", "deaths"],
                "choice_new_slug": "cases_and_deaths",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # After grouping: cases and deaths should be removed, only recovered and combined remain
    choice_slugs = [c.slug for c in collection.dimensions[0].choices]
    assert "cases" not in choice_slugs
    assert "deaths" not in choice_slugs
    assert "recovered" in choice_slugs
    assert "cases_and_deaths" in choice_slugs
    assert len(choice_slugs) == 2


def test_group_views_replace_false_keeps_original_views():
    """
    Test Collection.group_views with replace=False (default) keeps original views.

    When replace=False, both the original views and the new grouped views should exist.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["category"],
        dimensions=[
            Dimension(
                slug="category",
                name="Category",
                choices=[
                    DimensionChoice(slug="a", name="A"),
                    DimensionChoice(slug="b", name="B"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"category": "a"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"category": "b"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before grouping: 2 views
    assert len(collection.views) == 2

    # Group views with replace=False (default)
    collection.group_views(
        [
            {
                "dimension": "category",
                "choices": ["a", "b"],
                "choice_new_slug": "all",
                "replace": False,  # Explicitly false
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # After grouping: 3 views (original a, original b, and combined)
    assert len(collection.views) == 3

    # Check all choices are present
    categories = {view.dimensions["category"] for view in collection.views}
    assert categories == {"a", "b", "all"}


def test_group_views_replace_with_multiple_groups():
    """
    Test Collection.group_views with replace=True for multiple groups.

    When multiple groups are processed together, ALL groups create their views based on
    the ORIGINAL views. Then, the replace logic runs for all groups with replace=True.
    This means views from a second group may be removed if they have dimension values
    that a first group with replace=True is removing.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["sex", "age"],
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
                    DimensionChoice(slug="young", name="Young"),
                    DimensionChoice(slug="old", name="Old"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"sex": "male", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"sex": "female", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"sex": "male", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
            View(
                dimensions={"sex": "female", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind4")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before grouping: 4 views
    assert len(collection.views) == 4

    # Group both dimensions: sex with replace=True, age with replace=False
    # Note: The age grouping creates views based on original views (with sex=male/female),
    # but these get removed by the sex grouping's replace=True logic.
    collection.group_views(
        [
            {
                "dimension": "sex",
                "choices": ["male", "female"],
                "choice_new_slug": "both_sexes",
                "replace": True,  # Remove original sex views
            },
            {
                "dimension": "age",
                "choices": ["young", "old"],
                "choice_new_slug": "all_ages",
                "replace": False,  # Keep original age views
            },
        ],
        drop_dimensions_if_single_choice=False,
    )

    # The sex grouping creates views with sex="both_sexes" for each age (2 views)
    # The age grouping creates views with age="all_ages" for each sex (2 views with sex=male/female)
    # Then replace=True for sex removes all views where sex is "male" or "female"
    # This removes: original 4 views + the 2 age-grouped views (which had sex=male/female)
    # Remaining: only the sex-grouped views (sex="both_sexes")
    assert len(collection.views) == 2

    # Verify sex dimension only has "both_sexes"
    sex_choices = {view.dimensions["sex"] for view in collection.views}
    assert sex_choices == {"both_sexes"}

    # Verify age dimension only has young and old (all_ages views were removed)
    age_choices = {view.dimensions["age"] for view in collection.views}
    assert age_choices == {"young", "old"}


def test_group_views_replace_combines_indicators():
    """
    Test that replace=True properly combines indicators from original views.

    The grouped view should contain all y indicators from the original views.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["source"],
        dimensions=[
            Dimension(
                slug="source",
                name="Source",
                choices=[
                    DimensionChoice(slug="source_a", name="Source A"),
                    DimensionChoice(slug="source_b", name="Source B"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"source": "source_a"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#indicator_a")]),
            ),
            View(
                dimensions={"source": "source_b"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#indicator_b")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Group with replace=True
    collection.group_views(
        [
            {
                "dimension": "source",
                "choices": ["source_a", "source_b"],
                "choice_new_slug": "combined_sources",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # Only 1 view should remain
    assert len(collection.views) == 1

    # The combined view should have both indicators
    combined_view = collection.views[0]
    assert combined_view.dimensions["source"] == "combined_sources"

    assert combined_view.indicators.y is not None
    indicator_paths = [ind.catalogPath for ind in combined_view.indicators.y]
    assert "table#indicator_a" in indicator_paths
    assert "table#indicator_b" in indicator_paths
    assert len(indicator_paths) == 2


def test_group_views_replace_dimension_pruned_when_single_choice():
    """
    Test that with replace=True and drop_dimensions_if_single_choice=True,
    the dimension is removed if only one choice remains.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["type", "region"],
        dimensions=[
            Dimension(
                slug="type",
                name="Type",
                choices=[
                    DimensionChoice(slug="type_a", name="Type A"),
                    DimensionChoice(slug="type_b", name="Type B"),
                ],
            ),
            Dimension(
                slug="region",
                name="Region",
                choices=[
                    DimensionChoice(slug="north", name="North"),
                    DimensionChoice(slug="south", name="South"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"type": "type_a", "region": "north"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"type": "type_b", "region": "north"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"type": "type_a", "region": "south"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
            View(
                dimensions={"type": "type_b", "region": "south"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind4")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before: 2 dimensions
    assert len(collection.dimensions) == 2

    # Group type dimension with replace=True and allow pruning (default)
    collection.group_views(
        [
            {
                "dimension": "type",
                "choices": ["type_a", "type_b"],
                "choice_new_slug": "all_types",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=True,  # Default behavior
    )

    # Type dimension should be removed (only 1 choice: all_types)
    assert len(collection.dimensions) == 1
    assert collection.dimensions[0].slug == "region"

    # Views should not have "type" dimension anymore
    for view in collection.views:
        assert "type" not in view.dimensions
        assert "region" in view.dimensions


def test_group_views_replace_with_partial_choices():
    """
    Test Collection.group_views with replace=True grouping only some choices.

    When only some choices are grouped with replace=True, the ungrouped choices
    should remain as separate views.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["status"],
        dimensions=[
            Dimension(
                slug="status",
                name="Status",
                choices=[
                    DimensionChoice(slug="active", name="Active"),
                    DimensionChoice(slug="pending", name="Pending"),
                    DimensionChoice(slug="archived", name="Archived"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"status": "active"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"status": "pending"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"status": "archived"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before: 3 views
    assert len(collection.views) == 3

    # Group only active and pending, leaving archived
    collection.group_views(
        [
            {
                "dimension": "status",
                "choices": ["active", "pending"],
                "choice_new_slug": "current",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # After: 2 views (current + archived)
    assert len(collection.views) == 2

    # Check status values
    status_values = {view.dimensions["status"] for view in collection.views}
    assert status_values == {"current", "archived"}
    assert "active" not in status_values
    assert "pending" not in status_values


def test_group_views_replace_sequential_calls():
    """
    Test using replace=True in sequential group_views calls.

    When you need to group multiple dimensions and use the grouped results
    of one dimension in another grouping, call group_views separately.
    """
    collection = Collection(
        catalog_path="test#table",
        title={"en": "Test"},
        default_selection=["sex", "age"],
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
                    DimensionChoice(slug="young", name="Young"),
                    DimensionChoice(slug="old", name="Old"),
                ],
            ),
        ],
        views=[
            View(
                dimensions={"sex": "male", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind1")]),
            ),
            View(
                dimensions={"sex": "female", "age": "young"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind2")]),
            ),
            View(
                dimensions={"sex": "male", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind3")]),
            ),
            View(
                dimensions={"sex": "female", "age": "old"},
                indicators=ViewIndicators(y=[Indicator(catalogPath="table#ind4")]),
            ),
        ],
        _definitions=Definitions(),
    )

    # Before grouping: 4 views
    assert len(collection.views) == 4

    # First: group sex dimension with replace=True
    collection.group_views(
        [
            {
                "dimension": "sex",
                "choices": ["male", "female"],
                "choice_new_slug": "both_sexes",
                "replace": True,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # After first grouping: 2 views (both_sexes x young/old)
    assert len(collection.views) == 2
    for view in collection.views:
        assert view.dimensions["sex"] == "both_sexes"

    # Second: group age dimension with replace=False (keeping young/old, adding all_ages)
    collection.group_views(
        [
            {
                "dimension": "age",
                "choices": ["young", "old"],
                "choice_new_slug": "all_ages",
                "replace": False,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )

    # After second grouping: 3 views (both_sexes x young/old/all_ages)
    assert len(collection.views) == 3

    # All views have sex="both_sexes"
    sex_choices = {view.dimensions["sex"] for view in collection.views}
    assert sex_choices == {"both_sexes"}

    # Age has young, old, and all_ages
    age_choices = {view.dimensions["age"] for view in collection.views}
    assert age_choices == {"young", "old", "all_ages"}
