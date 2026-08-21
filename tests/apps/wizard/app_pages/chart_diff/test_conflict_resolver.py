"""Tests for the chart-diff conflict resolver (issue #6736).

The bug these guard against: the PRODUCTION/STAGING radio had no effect, because the editor was a
keyed Streamlit widget created with `value=` -- which is ignored from the second render onwards -- so
whatever the editor was born with (production's value) was what got written to the staging chart.
"""

import json

import pytest
from streamlit.testing.v1 import AppTest

from apps.wizard.app_pages.chart_diff.conflict_resolver import (
    FieldParseError,
    build_resolved_config,
    compare_chart_configs,
    parse_field_text,
    resolve_field_value,
)

PROD_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "id": 42,
    "version": 7,
    "isPublished": True,
    "title": "Production title",
    "note": "2024",
    "selectedEntityNames": ["World"],
    "map": {"time": 2020},
}
STAGING_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.008.json",
    "id": 42,
    "version": 9,
    "isPublished": True,
    "title": "Staging title",
    "note": "2024",
    "selectedEntityNames": ["Kenya", "Peru"],
    "subtitle": "Only on staging",
}


def test_compare_chart_configs_ignores_bookkeeping_fields():
    """`version` differs on essentially every real conflict, and is not a conflict to resolve."""
    fields = {field["key"] for field in compare_chart_configs(PROD_CONFIG, STAGING_CONFIG)}
    assert fields == {"title", "selectedEntityNames", "map", "subtitle"}


def test_compare_chart_configs_keeps_raw_values_and_is_sorted():
    fields = compare_chart_configs(PROD_CONFIG, STAGING_CONFIG)

    # Sorted, so the UI (and these tests) see a stable order.
    assert [field["key"] for field in fields] == ["map", "selectedEntityNames", "subtitle", "title"]

    by_key = {field["key"]: field for field in fields}
    # Raw values are kept as objects, not pre-stringified: that is what lets an untouched field be
    # written back with its type intact.
    assert by_key["selectedEntityNames"]["raw2"] == ["Kenya", "Peru"]
    assert by_key["map"]["raw1"] == {"time": 2020}
    assert by_key["map"]["raw2"] is None
    assert by_key["map"]["text2"] == ""
    # Strings are shown raw, everything else as JSON.
    assert by_key["title"]["text1"] == "Production title"
    assert by_key["selectedEntityNames"]["text2"] == json.dumps(["Kenya", "Peru"], indent=2)


def test_resolve_field_value_untouched_editor_keeps_the_object():
    value = ["Kenya", "Peru"]
    rendered = json.dumps(value, indent=2)
    assert resolve_field_value("selectedEntityNames", rendered, rendered, value) is value


def test_resolve_field_value_emptied_editor_drops_the_field():
    assert resolve_field_value("subtitle", "", "Only on staging", "Only on staging") is None


def test_resolve_field_value_edited_list():
    assert resolve_field_value("selectedEntityNames", '["Chad"]', '["Kenya"]', ["Kenya"]) == ["Chad"]


def test_parse_field_text_does_not_retype_strings():
    """A note of "2024" is the string "2024" -- the old code turned it into the number 2024."""
    assert parse_field_text("note", "2024", "2023") == "2024"
    assert parse_field_text("note", "null", "text") == "null"
    assert parse_field_text("note", "1,2", "text") == "1,2"


def test_parse_field_text_accepts_python_reprs():
    assert parse_field_text("selectedEntityNames", "['Chad', 'Peru']", ["Kenya"]) == ["Chad", "Peru"]


def test_parse_field_text_raises_on_malformed_structured_value():
    """A broken edit must not be written to the chart as a raw string."""
    with pytest.raises(FieldParseError) as excinfo:
        parse_field_text("selectedEntityNames", '["Chad", ', ["Kenya"])
    assert excinfo.value.field_key == "selectedEntityNames"


def test_build_resolved_config():
    config = build_resolved_config(
        {**STAGING_CONFIG, "dataApiUrl": "https://api.staging"},
        {
            "title": "Production title",  # keep production's
            "selectedEntityNames": ["Kenya", "Peru"],  # keep staging's
            "subtitle": None,  # absent in production -> drop it
        },
    )

    assert config["title"] == "Production title"
    assert config["selectedEntityNames"] == ["Kenya", "Peru"]
    assert "subtitle" not in config
    # Environment-specific fields are never sent back to the admin API.
    assert "dataApiUrl" not in config
    # Untouched fields come through from staging.
    assert config["version"] == 9


def test_build_resolved_config_does_not_mutate_the_source():
    source = {"title": "Staging title", "selectedEntityNames": ["Kenya"]}
    build_resolved_config(source, {"title": "Production title", "selectedEntityNames": None})
    assert source == {"title": "Staging title", "selectedEntityNames": ["Kenya"]}


def _resolver_app():
    """Render the field resolvers for one conflict. Must be self-contained (AppTest runs its source)."""
    import streamlit as st

    from apps.wizard.app_pages.chart_diff.conflict_resolver import ChartDiffConflictResolver

    class _Chart:
        def __init__(self, config):
            self.config = config
            self.lastEditedByUserId = 1

    class _Diff:
        chart_id = 42

        def __init__(self):
            self.target_chart = _Chart(
                {
                    "$schema": "s",
                    "title": "Production title",
                    "selectedEntityNames": ["World"],
                }
            )
            self.source_chart = _Chart(
                {
                    "$schema": "s",
                    "title": "Staging title",
                    "selectedEntityNames": ["Kenya", "Peru"],
                }
            )

    resolver = ChartDiffConflictResolver(_Diff(), session=None)  # ty: ignore
    for field in resolver.config_compare:
        resolver.show_field_resolver(field)
    st.text("undecided: [" + ", ".join(resolver.fields_undecided) + "]")


def test_editor_follows_the_radio():
    """The regression test for #6736: today's code returns production's value whatever is clicked."""
    at = AppTest.from_function(_resolver_app, default_timeout=30).run()

    # Fields are sorted: selectedEntityNames, title.
    assert len(at.radio) == 2
    assert len(at.text_area) == 2

    # Nothing is preselected, so nothing can be written by accident.
    assert [radio.value for radio in at.radio] == [None, None]
    assert [text_area.value for text_area in at.text_area] == ["", ""]
    assert all(text_area.disabled for text_area in at.text_area)
    assert at.text[0].value == "undecided: [selectedEntityNames, title]"

    # Choosing STAGING fills the editor with staging's value...
    at = at.radio[1].set_value(2).run()
    assert at.text_area[1].value == "Staging title"
    assert not at.text_area[1].disabled

    # ...and switching to PRODUCTION follows.
    at = at.radio[1].set_value(1).run()
    assert at.text_area[1].value == "Production title"

    # Structured values are shown as JSON.
    at = at.radio[0].set_value(2).run()
    assert at.text_area[0].value == json.dumps(["Kenya", "Peru"], indent=2)
    assert at.text[0].value == "undecided: []"


def test_editor_keeps_a_hand_edit_until_the_choice_changes():
    at = AppTest.from_function(_resolver_app, default_timeout=30).run()
    at = at.radio[1].set_value(2).run()

    at = at.text_area[1].set_value("Hand-written title").run()
    assert at.text_area[1].value == "Hand-written title"

    # Re-picking the same environment does not wipe the edit...
    at = at.radio[1].set_value(2).run()
    assert at.text_area[1].value == "Hand-written title"

    # ...but choosing the other one does, since that is an explicit request for its value.
    at = at.radio[1].set_value(1).run()
    assert at.text_area[1].value == "Production title"


class FakeAdminAPI:
    """Captures what would be pushed to the staging chart."""

    calls: list[dict] = []

    def __init__(self, owid_env):
        pass

    def update_chart(self, chart_id, chart_config, user_id=None):
        FakeAdminAPI.calls.append({"chart_id": chart_id, "config": chart_config, "user_id": user_id})
        return {"success": True}


def _resolve_app():
    """Full flow for one conflict, up to the call to the admin API. Must be self-contained."""
    import streamlit as st

    import apps.wizard.app_pages.chart_diff.conflict_resolver as cr

    class _Chart:
        def __init__(self, config):
            self.config = config
            self.lastEditedByUserId = 13

    class _Diff:
        chart_id = 42

        def __init__(self):
            self.target_chart = _Chart(
                {
                    "$schema": "s",
                    "title": "Production title",
                    "selectedEntityNames": ["World"],
                    "isInheritanceEnabled": True,
                }
            )
            self.source_chart = _Chart(
                {
                    "$schema": "s",
                    "title": "Staging title",
                    "selectedEntityNames": ["Kenya", "Peru"],
                    "isInheritanceEnabled": False,
                }
            )

        def set_conflict_to_resolved(self, session):
            pass

    resolver = cr.ChartDiffConflictResolver(_Diff(), session=None)  # ty: ignore
    for field in resolver.config_compare:
        resolver.show_field_resolver(field)
    st.button("Resolve conflicts", disabled=bool(resolver.fields_undecided), on_click=resolver.resolve_conflicts)

    message = st.session_state.get("conflict-resolver-msg-42")
    if message is not None:
        st.text(f"{message[0]}: {message[1]}")


@pytest.fixture
def fake_admin_api(monkeypatch):
    import apps.wizard.app_pages.chart_diff.conflict_resolver as cr

    FakeAdminAPI.calls = []
    monkeypatch.setattr(cr, "AdminAPI", FakeAdminAPI)
    # Keep schema validation offline: the real one downloads the grapher schema.
    monkeypatch.setattr(cr, "get_schema_from_url", lambda url: {"type": "object"})
    return FakeAdminAPI


def test_resolving_writes_the_chosen_side(fake_admin_api):
    """End to end for #6736: what reaches the admin API is what was picked, per field."""
    at = AppTest.from_function(_resolve_app, default_timeout=30).run()

    # Fields, sorted: isInheritanceEnabled, selectedEntityNames, title.
    assert [radio.value for radio in at.radio] == [None, None, None]
    # Nothing can be resolved until every field has been decided.
    assert at.button[0].disabled

    at = at.radio[0].set_value(1).run()  # isInheritanceEnabled <- production
    at = at.radio[1].set_value(2).run()  # selectedEntityNames <- staging
    at = at.radio[2].set_value(1).run()  # title <- production
    assert not at.button[0].disabled

    at = at.button[0].click().run()

    assert len(fake_admin_api.calls) == 1
    call = fake_admin_api.calls[0]
    assert call["chart_id"] == 42
    assert call["user_id"] == 13
    # The staging-side entity selection survives -- the whole point of the issue.
    assert call["config"]["selectedEntityNames"] == ["Kenya", "Peru"]
    assert call["config"]["title"] == "Production title"
    # isInheritanceEnabled is re-attached after schema validation strips it, so choosing a side for it
    # actually reaches the admin API.
    assert call["config"]["isInheritanceEnabled"] is True
    assert at.text[0].value.startswith("success: Chart 42 updated on staging")


def test_resolving_a_malformed_edit_writes_nothing(fake_admin_api):
    at = AppTest.from_function(_resolve_app, default_timeout=30).run()
    at = at.radio[0].set_value(2).run()
    at = at.radio[1].set_value(2).run()
    at = at.radio[2].set_value(2).run()

    # Break the JSON of a list field.
    at = at.text_area[1].set_value('["Kenya", ').run()
    at = at.button[0].click().run()

    assert fake_admin_api.calls == []
    assert at.text[0].value.startswith("error: Nothing was written")
    assert "selectedEntityNames" in at.text[0].value
