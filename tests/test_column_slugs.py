"""Tests for `etl.grapher.column_slugs`.

The functions under test are pure: the caller resolves variable ids to catalog paths and passes
them in, so every matching rule is exercised here without a database.
"""

from etl.grapher.column_slugs import (
    MAP_COLUMN_SLUG,
    SORT_COLUMN_SLUG,
    dimension_variable_ids,
    find_dangling_column_slugs,
    indicator_identity,
    read_column_slug,
    repair_column_slugs,
    variable_ids_to_resolve,
)

# The same indicator at two versions, plus an unrelated one.
_OLD = 1227375
_NEW = 1340000
_OTHER = 900000
_PATHS = {
    _OLD: "grapher/animal_welfare/2026-04-16/chick_culling_laws/chick_culling_laws#status",
    _NEW: "grapher/animal_welfare/2026-09-01/chick_culling_laws/chick_culling_laws#status",
    _OTHER: "grapher/demography/2026-01-01/population/population#population",
}


def _config(dimension_ids: list[int], **fields) -> dict:
    config = {"dimensions": [{"property": "y", "variableId": i} for i in dimension_ids]}
    config.update(fields)
    return config


def test_indicator_identity_strips_the_version():
    assert indicator_identity(_PATHS[_OLD]) == indicator_identity(_PATHS[_NEW])
    assert indicator_identity(_PATHS[_OLD]) == "grapher/animal_welfare/chick_culling_laws/chick_culling_laws#status"


def test_indicator_identity_of_unmatchable_paths():
    # Legacy variables carry no catalog path, and an unexpected shape is not one we can take a
    # version out of — both mean "nothing to match on" rather than a mangled identity.
    assert indicator_identity(None) is None
    assert indicator_identity("") is None
    assert indicator_identity("grapher/ns/version/table#col") is None


def test_indicator_identity_distinguishes_different_indicators():
    assert indicator_identity(_PATHS[_OLD]) != indicator_identity(_PATHS[_OTHER])


def test_dimension_variable_ids_keeps_order():
    assert dimension_variable_ids(_config([_NEW, _OTHER])) == [_NEW, _OTHER]
    assert dimension_variable_ids({}) == []


def test_read_column_slug_reads_both_fields():
    config = _config([_NEW], map={"columnSlug": str(_OLD)}, sortColumnSlug=str(_OTHER))
    assert read_column_slug(config, MAP_COLUMN_SLUG) == _OLD
    assert read_column_slug(config, SORT_COLUMN_SLUG) == _OTHER


def test_read_column_slug_ignores_unresolved_catalog_paths():
    # A collection config may declare either field as a catalog path; that is authored input
    # awaiting resolution, not a dangling reference.
    config = _config([_NEW], map={"columnSlug": "chick_culling_laws#status"})
    assert read_column_slug(config, MAP_COLUMN_SLUG) is None
    assert find_dangling_column_slugs(config) == {}


def test_read_column_slug_when_absent():
    assert read_column_slug(_config([_NEW]), MAP_COLUMN_SLUG) is None
    assert read_column_slug(_config([_NEW], map={"hideTimeline": True}), MAP_COLUMN_SLUG) is None
    assert read_column_slug(_config([_NEW]), SORT_COLUMN_SLUG) is None


def test_a_slug_naming_a_plotted_column_is_not_dangling():
    config = _config([_NEW, _OTHER], map={"columnSlug": str(_OTHER)})
    assert find_dangling_column_slugs(config) == {}
    assert repair_column_slugs(config, _PATHS) == (config, [])


def test_variable_ids_to_resolve_covers_dimensions_and_dangling_slugs():
    config = _config([_NEW], map={"columnSlug": str(_OLD)})
    assert variable_ids_to_resolve(config) == {_NEW, _OLD}


def test_map_slug_is_remapped_to_the_same_indicator_at_its_new_version():
    # The issue's case: chart 7118 after the version bump.
    config = _config([_NEW], map={"columnSlug": str(_OLD), "hideTimeline": True})
    repaired, repairs = repair_column_slugs(config, _PATHS)

    assert repaired["map"] == {"columnSlug": str(_NEW), "hideTimeline": True}
    assert [(r.field, r.old_id, r.new_id) for r in repairs] == [(MAP_COLUMN_SLUG, _OLD, _NEW)]
    # The input is left alone.
    assert config["map"]["columnSlug"] == str(_OLD)


def test_map_slug_is_dropped_when_the_chart_no_longer_plots_that_indicator():
    config = _config([_OTHER], map={"columnSlug": str(_OLD), "hideTimeline": True})
    repaired, repairs = repair_column_slugs(config, _PATHS)

    assert repaired["map"] == {"hideTimeline": True}
    assert repairs[0].new_id is None
    assert repairs[0].reason == "chart no longer plots this indicator"


def test_map_slug_is_dropped_when_the_retired_variable_was_deleted():
    # 129 of the dangling slugs in production point at a variable that no longer exists, so it
    # never comes back from the catalog-path lookup.
    config = _config([_NEW], map={"columnSlug": "42218"})
    repaired, repairs = repair_column_slugs(config, _PATHS)

    assert "columnSlug" not in repaired["map"]
    assert repairs[0].reason == "retired variable no longer exists"


def test_map_slug_is_dropped_when_the_retired_variable_has_no_catalog_path():
    config = _config([_NEW], map={"columnSlug": "8815"})
    repaired, repairs = repair_column_slugs(config, {**_PATHS, 8815: None})

    assert "columnSlug" not in repaired["map"]
    assert repairs[0].reason == "no catalog path to match a plotted column on"


def test_map_slug_is_dropped_when_several_plotted_columns_match():
    # A chart plotting two versions of one indicator gives no basis for picking.
    config = _config([_OLD, _NEW], map={"columnSlug": "999999"})
    repaired, repairs = repair_column_slugs(
        config,
        {**_PATHS, 999999: "grapher/animal_welfare/2023-09-01/chick_culling_laws/chick_culling_laws#status"},
    )

    assert "columnSlug" not in repaired["map"]
    assert repairs[0].reason == "ambiguous: 2 plotted columns match"


def test_sort_column_slug_is_remapped():
    config = _config([_NEW], sortBy="column", sortColumnSlug=str(_OLD))
    repaired, repairs = repair_column_slugs(config, _PATHS)

    assert repaired["sortColumnSlug"] == str(_NEW)
    assert repaired["sortBy"] == "column"
    assert [(r.field, r.new_id) for r in repairs] == [(SORT_COLUMN_SLUG, _NEW)]


def test_dropping_sort_column_slug_also_drops_sort_by_column():
    # `sortBy: column` with no column to sort by is a mode the config can't describe, and our
    # own indicator-upgrade path asserts the two travel together.
    config = _config([_OTHER], sortBy="column", sortColumnSlug=str(_OLD))
    repaired, _ = repair_column_slugs(config, _PATHS)

    assert "sortColumnSlug" not in repaired
    assert "sortBy" not in repaired


def test_dropping_sort_column_slug_leaves_other_sort_modes_alone():
    config = _config([_OTHER], sortBy="total", sortColumnSlug=str(_OLD))
    repaired, _ = repair_column_slugs(config, _PATHS)

    assert "sortColumnSlug" not in repaired
    assert repaired["sortBy"] == "total"


def test_both_fields_are_repaired_in_one_pass():
    config = _config([_NEW], map={"columnSlug": str(_OLD)}, sortBy="column", sortColumnSlug=str(_OLD))
    repaired, repairs = repair_column_slugs(config, _PATHS)

    assert repaired["map"]["columnSlug"] == str(_NEW)
    assert repaired["sortColumnSlug"] == str(_NEW)
    assert len(repairs) == 2
