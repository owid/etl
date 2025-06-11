"""
Tests for Collection model from etl.collection.model.core.

This module tests the core functionality of the Collection class including
initialization, validation, view management, dimension handling, and data export.
"""

import pytest

from etl.collection.exceptions import DuplicateCollectionViews
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
