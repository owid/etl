from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest
from owid.catalog import Table

from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff
from etl.grapher import entity_selection as es


def _config(selected, dims=((1, "y"),), **kwargs):
    return {
        "selectedEntityNames": list(selected),
        "dimensions": [{"variableId": v, "property": p} for v, p in dims],
        **kwargs,
    }


def _table(entities):
    index = pd.MultiIndex.from_tuples(
        [(i, "", e, 2000) for i, e in enumerate(entities)], names=["entityId", "entityCode", "entityName", "year"]
    )
    return Table(pd.DataFrame({"value": range(len(entities))}, index=index))


def test_partly_dead_selection_is_reported():
    config = _config(["African Region (WHO)", "World"])
    assert es.dead_selected_entities(config, {1: {"Africa (WHO)", "World"}}) == ["African Region (WHO)"]


def test_entity_with_data_in_any_y_indicator_is_alive():
    config = _config(["France", "Japan"], dims=((1, "y"), (2, "y")))
    assert es.dead_selected_entities(config, {1: {"France"}, 2: {"Japan"}}) == []


def test_selection_is_checked_against_y_dimensions_only():
    # Japan only has data in the color dimension, so the chart can't plot it
    config = _config(["Japan"], dims=((1, "y"), (2, "color")))
    assert es.dead_selected_entities(config, {1: {"France"}, 2: {"Japan"}}) == ["Japan"]


def test_map_only_chart_is_skipped():
    config = _config(["Atlantis"], chartTypes=[], hasMapTab=True)
    assert es.dead_selected_entities(config, {1: {"France"}}) == []


def test_unknown_availability_is_not_a_finding():
    assert es.dead_selected_entities(_config(["France"]), {1: None}) is None


def _upsert_check(selected, entities_in_production, upserted):
    check = es.UpsertPinsCheck(engine=mock.MagicMock(), dataset_id=1)
    check.charts = [es.ChartPins(chart_id=10, slug="hib-vaccine", config=_config(selected))]
    check.catalog_paths = {1: "grapher/who/latest/vaccination#hib3"}
    check.tracked_paths = {"grapher/who/latest/vaccination#hib3"}
    check.entities_in_production = {1: entities_in_production}
    check.add("grapher/who/latest/vaccination#hib3", _table(upserted))
    return check


def test_upsert_that_removes_a_pinned_entity_fails():
    check = _upsert_check(
        ["African Region (WHO)", "World"],
        entities_in_production={"African Region (WHO)", "World"},
        upserted=["Africa (WHO)", "World"],
    )
    with mock.patch.object(es.config, "ENV", "staging"):
        with pytest.raises(es.PinnedEntitiesRemovedError, match="'African Region \\(WHO\\)'"):
            check.run()


def test_pin_already_dead_in_production_is_out_of_scope():
    check = _upsert_check(
        ["Atlantis", "World"],
        entities_in_production={"World"},
        upserted=["World"],
    )
    with mock.patch.object(es.config, "ENV", "staging"):
        check.run()


def test_production_reports_instead_of_failing():
    check = _upsert_check(["France"], entities_in_production={"France"}, upserted=["Germany"])
    with (
        mock.patch.object(es.config, "ENV", "production"),
        mock.patch("etl.slack_helpers.send_slack_message") as send,
    ):
        check.run()
    send.assert_called_once()
    assert "'France'" in send.call_args.kwargs["message"]


def test_chart_diff_flags_dead_pins_of_edited_and_new_charts_only():
    df_charts = pd.DataFrame({"configEdited": [True, False, False]}, index=[1, 2, 3])
    source_charts = {
        1: SimpleNamespace(config=_config(["Atlantis", "France"])),  # edited
        2: SimpleNamespace(config=_config(["Atlantis"])),  # data-only diff
        3: SimpleNamespace(config=_config(["Atlantis"])),  # new (no target)
    }
    target_charts = {1: object(), 2: object()}
    with mock.patch(
        "apps.wizard.app_pages.chart_diff.chart_diff.fetch_entities_with_data", return_value={1: {"France"}}
    ):
        dead = ChartDiff._get_dead_selected_entities(df_charts, source_charts, target_charts, source_env=None)  # ty: ignore[invalid-argument-type]
    assert dead == {1: ["Atlantis"], 3: ["Atlantis"]}
