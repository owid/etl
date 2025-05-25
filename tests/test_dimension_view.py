import pytest

dimension_mod = pytest.importorskip("etl.collection.model.dimension")
view_mod = pytest.importorskip("etl.collection.model.view")

DimensionChoice = dimension_mod.DimensionChoice
DimensionPresentation = dimension_mod.DimensionPresentation
DimensionPresentationUIType = dimension_mod.DimensionPresentationUIType
Dimension = dimension_mod.Dimension

Indicator = view_mod.Indicator
ViewIndicators = view_mod.ViewIndicators
View = view_mod.View


def make_dimension():
    return Dimension(
        slug="age",
        name="Age",
        choices=[
            DimensionChoice(slug="old", name="Old"),
            DimensionChoice(slug="young", name="Young"),
            DimensionChoice(slug="adult", name="Adult"),
        ],
        presentation=DimensionPresentation(type=DimensionPresentationUIType.DROPDOWN),
    )


def test_sort_choices():
    dim = make_dimension()
    dim.sort_choices(["young", "adult", "old"])
    assert [c.slug for c in dim.choices] == ["young", "adult", "old"]

    dim.sort_choices(lambda slugs: sorted(slugs))
    assert [c.slug for c in dim.choices] == ["adult", "old", "young"]


def test_sort_choices_missing_slug():
    dim = make_dimension()
    with pytest.raises(ValueError):
        dim.sort_choices(["young", "old"])  # missing 'adult'


def test_unique_validations():
    dim = make_dimension()
    dim.validate_unique_names()
    dim.validate_unique_slugs()

    # duplicate name
    dim_dup_name = make_dimension()
    dim_dup_name.choices.append(DimensionChoice(slug="child", name="Old"))
    with pytest.raises(ValueError):
        dim_dup_name.validate_unique_names()

    # duplicate slug
    dim_dup_slug = make_dimension()
    dim_dup_slug.choices.append(DimensionChoice(slug="old", name="Very Old"))
    with pytest.raises(ValueError):
        dim_dup_slug.validate_unique_slugs()


def test_indicator_expand_path():
    indicator = Indicator("table#value")
    mapping = {"table": ["grapher/ns/latest/ds/table"]}
    indicator.expand_path(mapping)
    assert indicator.catalogPath == "grapher/ns/latest/ds/table#value"


def test_view_indicators_from_dict_and_to_records():
    data = {"y": "table#ind1", "x": "table#ind2"}
    vi = ViewIndicators.from_dict(data)
    records = vi.to_records()
    assert records == [
        {"path": "table#ind1", "axis": "y", "display": {}},
        {"path": "table#ind2", "axis": "x", "display": {}},
    ]


def test_view_expand_paths_and_indicators_used():
    view = View(
        dimensions={"d": "a"},
        indicators=ViewIndicators.from_dict({"y": "table#ind1"}),
        config={"sortColumnSlug": "other#ind2"},
    )
    mapping = {
        "table": ["grapher/ns/latest/ds/table"],
        "other": ["grapher/ns/latest/ds/other"],
    }
    view.expand_paths(mapping)

    with pytest.raises(ValueError):
        view.indicators_used()

    paths = view.indicators_used(tolerate_extra_indicators=True)
    assert set(paths) == {
        "grapher/ns/latest/ds/table#ind1",
        "grapher/ns/latest/ds/other#ind2",
    }
