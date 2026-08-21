import ast
import json
from copy import deepcopy
from typing import Any

import streamlit as st
import structlog
from requests.exceptions import HTTPError
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import CONFIG_KEYS_IGNORE, ChartDiff
from apps.wizard.app_pages.chart_diff.utils import SOURCE
from etl.files import get_schema_from_url
from etl.indicator_upgrade.schema import validate_chart_config_and_set_defaults

log = structlog.get_logger()

PRODUCTION = 1
STAGING = 2
ENVIRONMENT_IDS = {
    PRODUCTION: "PRODUCTION",
    STAGING: "STAGING",
}

# Fields the admin API sets itself, so they are never sent back to it.
KEYS_NOT_SENT = (
    "bakedGrapherURL",
    "adminBaseUrl",
    "dataApiUrl",
)


class FieldParseError(Exception):
    """The text typed into a field's editor could not be read back into a config value."""

    def __init__(self, field_key: str, message: str):
        self.field_key = field_key
        super().__init__(message)


def as_text(value: Any) -> str:
    """Render a config value as text to edit.

    Strings are shown raw (a title stays a title, without quotes around it); everything else as JSON,
    which is what `parse_field_text` reads back.
    """
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2)


def parse_field_text(field_key: str, text: str, rendered_value: Any) -> Any:
    """Read the text of a field's editor back into a config value.

    `rendered_value` is the value the editor was seeded with, and its type decides how `text` is read.
    That matters: a string field must stay a string even when its content happens to look like
    something else (a note of "2024" is the string "2024", not the number 2024).

    Raises `FieldParseError` rather than silently falling back to the raw string, so that a malformed
    edit cannot write a string into a field that expects a list.
    """
    if isinstance(rendered_value, str):
        return text

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    try:
        # Values shown as JSON come back as JSON, but a user may paste a Python repr (single quotes,
        # True/None), which json.loads rejects.
        return json.loads(json.dumps(ast.literal_eval(text)))
    except (ValueError, SyntaxError) as e:
        raise FieldParseError(field_key, str(e)) from e


def resolve_field_value(field_key: str, text: str, rendered_text: str, rendered_value: Any) -> Any:
    """Value a conflicted field should take, given the content of its editor.

    An untouched editor keeps the chosen side's value exactly as it is, without a text round-trip, so
    types survive (a list stays a list). An emptied editor means "this field should not be set".
    """
    if text == rendered_text:
        return rendered_value
    if text.strip() == "":
        return None
    return parse_field_text(field_key, text, rendered_value)


def build_resolved_config(source_config: dict[str, Any], resolutions: dict[str, Any]) -> dict[str, Any]:
    """Apply the resolution of each conflicted field on top of the staging config."""
    config = deepcopy(source_config)

    for field_key, value in resolutions.items():
        if (value is None) or (value == ""):
            config.pop(field_key, None)
        else:
            config[field_key] = value

    for key in KEYS_NOT_SENT:
        config.pop(key, None)

    return config


def compare_chart_configs(c1: dict[str, Any], c2: dict[str, Any]) -> list[dict[str, Any]]:
    """Compare chart configs c1 (production) and c2 (staging).

    Returns one entry per differing field, with both the raw value on each side (`raw1`/`raw2`, used
    to write the config) and its text rendering (`text1`/`text2`, used to fill the editor).
    """
    diff_list = []
    for key in sorted(set(c1.keys()).union(c2.keys())):
        if key in CONFIG_KEYS_IGNORE:
            continue
        value1 = c1.get(key)
        value2 = c2.get(key)
        if value1 != value2:
            diff_list.append(
                {
                    "key": key,
                    "raw1": value1,
                    "raw2": value2,
                    "text1": "" if value1 is None else as_text(value1),
                    "text2": "" if value2 is None else as_text(value2),
                }
            )

    return diff_list


class ChartDiffConflictResolver:
    """Resolve conflicts between charts.

    Provides UI.

    All of the resolver's state lives in `st.session_state`: this object is re-created on every
    Streamlit rerun, and `resolve_conflicts` runs as a widget callback (i.e. before the rerun that
    would re-create it), so anything stored on `self` between renders would be stale.
    """

    def __init__(self, diff: ChartDiff, session: Session):
        # Chart diff
        self.diff = diff
        # Session (needed to update conflict table in db)
        self.session = session
        # Compare chart configs
        self.config_compare = compare_chart_configs(
            self.diff.target_chart.config,  # ty: ignore
            self.diff.source_chart.config,
        )

    def _radio_key(self, field_key: str) -> str:
        return f"conflict-radio-{field_key}-{self.diff.chart_id}"

    def _editor_key(self, field_key: str) -> str:
        return f"conflict-editor-{field_key}-{self.diff.chart_id}"

    @property
    def fields_undecided(self) -> list[str]:
        """Conflicted fields for which no environment has been chosen yet."""
        return [f["key"] for f in self.config_compare if st.session_state.get(self._radio_key(f["key"])) is None]

    def choose_env_for_all(self, choice: int) -> None:
        """Pick the same environment for every conflicted field.

        Runs as a button callback, i.e. before the radios are instantiated in the following rerun,
        which is what makes writing their session state legal.
        """
        for field in self.config_compare:
            st.session_state[self._radio_key(field["key"])] = choice

    def show_field_resolver(self, field: dict[str, Any]) -> None:
        with st.container(border=True):
            # Title & layout
            st.markdown(f"##### {field['key']}")

            # Choose option
            choice = self._choose_env(field)

            # Show the fields values
            msg_none = "The field might be `None` because it is not present in the config, but inherited automatically from the indicator's metadata."
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("**Production**")
                    st.write(field["raw1"])
                    if field["raw1"] is None:
                        st.warning(msg_none)
            with col2:
                with st.container(border=True):
                    st.markdown("**Staging**")
                    st.write(field["raw2"])
                    if field["raw2"] is None:
                        st.warning(msg_none)

            # Merge editor
            self._show_merge_editor(field, choice)

    def _choose_env(self, field) -> int | None:
        """Choose environment to keep the value from.

        The user can edit the value later on. No environment is preselected: a default would get
        written without the user ever confirming it.
        """
        # Show option radio buttons
        return st.radio(
            "Choose config from...",
            options=[PRODUCTION, STAGING],
            index=None,
            captions=["Insert production config", "Insert staging config"],
            format_func=lambda x: ENVIRONMENT_IDS[x],
            key=self._radio_key(field["key"]),
            horizontal=True,
            label_visibility="collapsed",
        )

    def _show_merge_editor(self, field, choice: int | None) -> None:
        """Edit the content of the field.

        The editor's value is driven through session state rather than through `value=`: a keyed
        widget ignores `value=` from its second render onwards, which is why the editor used to keep
        whatever it was born with no matter which environment was picked.
        """
        editor_key = self._editor_key(field["key"])
        applied_key = f"{editor_key}-applied-choice"
        rendered_key = f"{editor_key}-rendered"

        rendered_value = None if choice is None else field[f"raw{choice}"]
        rendered_text = "" if choice is None else field[f"text{choice}"]

        # Re-seed the editor whenever the chosen environment changes (and on first render).
        if st.session_state.get(applied_key) != choice:
            st.session_state[editor_key] = rendered_text
            st.session_state[applied_key] = choice
        # Remember what the editor was seeded with, to tell later whether the user edited it.
        st.session_state[rendered_key] = rendered_text

        if choice is None:
            placeholder = "Choose PRODUCTION or STAGING above."
        elif rendered_value is None:
            placeholder = f"This field is not present in {ENVIRONMENT_IDS[choice]}!"
        else:
            placeholder = ""

        st.text_area(
            label="Edit config",
            placeholder=placeholder,
            help="Edit the final config here. When clicking on 'Resolve conflicts', this value will be used to update the chart config.",
            disabled=(choice is None) or (rendered_value is None),
            key=editor_key,
        )

    def _resolutions(self) -> dict[str, Any]:
        """Value each conflicted field should take, read from the widgets."""
        resolutions = {}
        for field in self.config_compare:
            field_key = field["key"]
            choice = st.session_state[self._radio_key(field_key)]
            editor_key = self._editor_key(field_key)
            resolutions[field_key] = resolve_field_value(
                field_key,
                text=st.session_state.get(editor_key, ""),
                rendered_text=st.session_state.get(f"{editor_key}-rendered", ""),
                rendered_value=field[f"raw{choice}"],
            )
        return resolutions

    def _summary(self, resolutions: dict[str, Any]) -> str:
        """One line per field saying where its value came from."""
        lines = []
        for field in self.config_compare:
            field_key = field["key"]
            choice = st.session_state[self._radio_key(field_key)]
            origin = ENVIRONMENT_IDS[choice]
            edited = resolutions[field_key] != field[f"raw{choice}"]
            lines.append(f"- `{field_key}`: {origin}{' (edited)' if edited else ''}")
        return "\n".join(lines)

    def resolve_conflicts(self, rerun: bool = False):
        """Gather all resolved conflicts and update chart config in staging.

        Runs as a button callback, so messages are deferred to session state instead of being drawn
        here (drawing elements from a callback inside a fragment duplicates them).
        """
        message_key = f"conflict-resolver-msg-{self.diff.chart_id}"

        undecided = self.fields_undecided
        if undecided:
            st.session_state[message_key] = (
                "error",
                "Nothing was written: no environment chosen yet for " + ", ".join(f"`{f}`" for f in undecided) + ".",
            )
            return

        try:
            resolutions = self._resolutions()
        except FieldParseError as e:
            st.session_state[message_key] = (
                "error",
                f"Nothing was written: could not read the value typed for `{e.field_key}` ({e}). "
                "It must be valid JSON, matching the type of the field.",
            )
            return

        summary = self._summary(resolutions)
        log.info(
            "chart_diff.resolve_conflicts",
            chart_id=self.diff.chart_id,
            resolutions={
                field["key"]: ENVIRONMENT_IDS[st.session_state[self._radio_key(field["key"])]]
                for field in self.config_compare
            },
        )

        with st.spinner("Updating chart on staging..."):
            # Consolidate changes
            config = build_resolved_config(self.diff.source_chart.config, resolutions)

            # `validate_chart_config_and_set_defaults` drops isInheritanceEnabled (it lives in the
            # charts table, not in the config schema), but AdminAPI.update_chart needs it to set the
            # inheritance flag -- without re-attaching it, resolving that field does nothing.
            is_inheritance_enabled = config.get("isInheritanceEnabled")

            # Verify config
            try:
                config_new = validate_chart_config_and_set_defaults(
                    config, schema=get_schema_from_url(config["$schema"])
                )
            except Exception as e:
                log.error(e)
                st.session_state[message_key] = (
                    "error",
                    f"Nothing was written: the resolved config is not valid. \n\n {e}",
                )
                return

            if is_inheritance_enabled is not None:
                config_new["isInheritanceEnabled"] = is_inheritance_enabled

            # User who last edited the chart
            user_id = self.diff.source_chart.lastEditedByUserId

            api = AdminAPI(SOURCE)
            try:
                # Push new chart to staging
                api.update_chart(
                    chart_id=self.diff.chart_id,
                    chart_config=config_new,
                    user_id=user_id,
                )
            except HTTPError as e:
                log.error(e)
                st.session_state[message_key] = (
                    "error",
                    f"An error occurred while updating the chart in staging. Please report this to #proj-new-data-workflow. If you are in a rush, you can manually integrate the changes in production [here]({SOURCE.chart_admin_site(self.diff.chart_id)}), and then click on the 'Mark as resolved' button in the conflict resolver. \n\n {e}",
                )
            else:
                # Set conflict as resolved
                self.diff.set_conflict_to_resolved(self.session)
                # Signal user that everything went well, and with which value each field ended up
                st.session_state[message_key] = (
                    "success",
                    f"Chart {self.diff.chart_id} updated on staging:\n\n{summary}",
                )
        if rerun:
            st.rerun()
