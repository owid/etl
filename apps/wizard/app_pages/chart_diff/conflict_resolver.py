import ast
import difflib
import json
from collections import Counter
from copy import deepcopy
from functools import cached_property
from typing import Any

import streamlit as st
import structlog
from requests.exceptions import HTTPError
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import CONFIG_KEYS_IGNORE, ChartDiff
from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.utils import get_staging_creation_time
from etl.files import get_schema_from_url
from etl.indicator_upgrade.schema import validate_chart_config_and_set_defaults

log = structlog.get_logger()

ADMIN_REVISIONS_URL = "https://admin.owid.io/admin/charts/{chart_id}/edit?tab=revisions"

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

    # Values shown as JSON come back as JSON, but a user may paste a Python repr (single quotes,
    # True/None), which json.loads rejects.
    try:
        value = ast.literal_eval(text)
    except Exception as e:
        # literal_eval raises a whole family of errors on input that is not a literal (ValueError,
        # SyntaxError, TypeError, MemoryError, RecursionError). They all mean the same thing here.
        raise FieldParseError(field_key, str(e)) from e

    try:
        # Not every Python literal is a value a chart config can hold: a set has no JSON form, and
        # neither does an out-of-range float.
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as e:
        raise FieldParseError(field_key, f"{type(value).__name__} is not a valid JSON value") from e


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
    """Apply the resolution of each conflicted field on top of the staging config.

    A resolution of `None` means the field should not be set at all: either the chosen environment
    does not have it, or the editor was emptied. An empty string is a different thing -- a chart can
    hold `subtitle: ""` on purpose, and dropping the key instead would let the indicator's metadata be
    inherited in its place, which is not what the user picked.
    """
    config = deepcopy(source_config)

    for field_key, value in resolutions.items():
        if value is None:
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


def field_value_diff(before: Any, after: Any) -> str:
    """A unified diff of one config field, from what production held before an edit to after it.

    Shown instead of the old value alone: for anything structured (a dimensions block, an entity
    list) the interesting part is the handful of changed lines, not the whole value.
    """
    old = as_text(before).splitlines() if before is not None else []
    new = as_text(after).splitlines() if after is not None else []
    # The ---/+++ headers name files, which there are none of here.
    lines = [line for line in difflib.unified_diff(old, new, lineterm="", n=1) if not line.startswith(("---", "+++"))]
    return "\n".join(lines)


def fetch_production_revisions(
    session: Session, chart_id: int, since: Any
) -> tuple[list[tuple[Any, str, dict[str, Any]]], dict[str, Any] | None]:
    """Production revisions of a chart since `since`, newest first, plus the one preceding them.

    That extra older revision is the baseline the earliest in-window revision is diffed against —
    without it there is nothing to compare the first change to.
    """
    rows = session.execute(
        sa_text(
            """
            SELECT r.createdAt, COALESCE(u.fullName, 'unknown') AS author, r.config
            FROM chart_revisions r
            LEFT JOIN users u ON u.id = r.userId
            WHERE r.chartId = :chart_id AND r.createdAt > :since
            ORDER BY r.createdAt DESC
            """
        ),
        {"chart_id": chart_id, "since": since},
    ).all()
    # The baseline is only a config to diff against, so it needs no author.
    baseline = session.execute(
        sa_text(
            """
            SELECT r.createdAt, '' AS author, r.config
            FROM chart_revisions r
            WHERE r.chartId = :chart_id AND r.createdAt <= :since
            ORDER BY r.createdAt DESC
            LIMIT 1
            """
        ),
        {"chart_id": chart_id, "since": since},
    ).all()

    def rows_to_revisions(raw) -> list[tuple[Any, str, dict[str, Any]]]:
        return [
            (created_at, author, config if isinstance(config, dict) else json.loads(config or "{}"))
            for created_at, author, config in raw
        ]

    baseline_rev = rows_to_revisions(baseline)
    return rows_to_revisions(rows), (baseline_rev[0][2] if baseline_rev else None)


def attribute_field_changes(
    revisions: list[tuple[Any, str, dict[str, Any]]],
    baseline_config: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Per field, who changed it in production since the fork and what it held before.

    Walks the revision chain oldest-first and attributes each changed field to the revision that
    changed it, keeping the most recent attribution. `before` is the value in the revision
    immediately preceding that change, which is what makes the annotation useful next to the
    conflict: production moved this field from `before` to its current value, while staging moved
    the same field from `before` to its own.
    """
    if baseline_config is None or not revisions:
        return {}

    chain = [(None, "", baseline_config)] + list(reversed(revisions))
    attribution: dict[str, dict[str, Any]] = {}
    for (_, _, older), (created_at, author, newer) in zip(chain, chain[1:]):
        for field in compare_chart_configs(older, newer):
            attribution[field["key"]] = {
                "author": author,
                "created_at": created_at,
                "before": field["raw1"],
                # The value this revision set, rather than production's current config value. They
                # are normally the same, but taking it from the revision keeps the diff faithful to
                # what the edit actually did.
                "after": field["raw2"],
            }
    return attribution


def summarize_production_edits(
    revisions: list[tuple[Any, str, dict[str, Any]]],
    baseline_config: dict[str, Any] | None,
    conflicted_keys: set[str],
) -> dict[str, Any] | None:
    """Describe what production did to a chart while we were working on it on staging.

    A conflict says *that* production moved, not what it moved. Without that, the reviewer has to open
    the admin's revision tab to find out whether the production edit even touches the fields they are
    being asked to resolve.

    `revisions` are the production revisions recorded since the staging server was created, newest
    first; `baseline_config` is the config immediately before them. A chart can be in conflict with no
    revisions at all — `charts.updatedAt` also moves on bulk and ETL-driven writes, which leave no
    admin revision — and that is reported as such rather than guessed at.
    """
    if not revisions:
        return {"kind": "no_revision"}

    newest_at, newest_author, latest_config = revisions[0]
    summary: dict[str, Any] = {
        "kind": "revision",
        "author": newest_author,
        "created_at": newest_at,
        "n_revisions": len(revisions),
        "keys": [],
        "overlapping": [],
    }
    if baseline_config is None:
        # Nothing earlier to diff against, so we can name the editor but not what they changed.
        return summary

    # Union of what changed across the whole window, not just the last edit: every one of these
    # revisions landed after the staging branch forked, so all of them are news to the reviewer.
    keys = sorted({field["key"] for field in compare_chart_configs(baseline_config, latest_config)})
    summary["keys"] = keys
    summary["overlapping"] = [k for k in keys if k in conflicted_keys]
    return summary


def show_field_value(value: Any) -> None:
    """Render one side's value of a conflicted field.

    A field that is not in that environment's config gets a short marker rather than a `None`: the
    chart inherits such fields from the indicator's metadata, and the difference between "absent" and
    "blank" matters when resolving (see `build_resolved_config`).
    """
    if value is None:
        st.warning("Not present", icon=":material/block:")
    else:
        st.write(value)


class ChartDiffConflictResolver:
    """Resolve conflicts between charts.

    Provides UI.

    All of the resolver's state lives in `st.session_state`: this object is re-created on every
    Streamlit rerun, and `resolve_conflicts` runs as a widget callback (i.e. before the rerun that
    would re-create it), so anything stored on `self` between renders would be stale.
    """

    def __init__(self, diff: ChartDiff, session: Session, target_session: Session | None = None):
        # Chart diff
        self.diff = diff
        # Session (needed to update conflict table in db)
        self.session = session
        # Production session, used only to read what production changed while we worked on staging.
        # Optional so the resolver still works without it (the summary is then simply not shown).
        self.target_session = target_session
        # Compare chart configs
        self.config_compare = compare_chart_configs(
            self.diff.target_chart.config,  # ty: ignore
            self.diff.source_chart.config,
        )

    @cached_property
    def _production_history(self) -> tuple[dict[str, Any] | None, dict[str, dict[str, Any]]]:
        """Production's revisions since the fork, as (headline summary, per-field attribution).

        Cached: the per-field accessor is read once per conflicted field while rendering, and each
        read would otherwise be two queries against the production DB.
        """
        if self.target_session is None:
            return None, {}
        try:
            revisions, baseline = fetch_production_revisions(
                self.target_session,
                self.diff.chart_id,
                get_staging_creation_time(self.session),
            )
        except Exception as e:  # noqa: BLE001 — context is a nicety; never break the resolver for it
            log.warning("conflict_resolver.production_edits_failed", chart_id=self.diff.chart_id, error=str(e))
            return None, {}
        summary = summarize_production_edits(revisions, baseline, {f["key"] for f in self.config_compare})
        return summary, attribute_field_changes(revisions, baseline)

    @property
    def production_edits(self) -> dict[str, Any] | None:
        """What production did to this chart since the staging server was created."""
        return self._production_history[0]

    @property
    def production_field_changes(self) -> dict[str, dict[str, Any]]:
        """Per conflicted field, who last changed it in production and what it held before."""
        return self._production_history[1]

    def show_production_edits(self) -> None:
        """Say who last changed production and whether that edit touches the fields being resolved."""
        summary = self.production_edits
        if summary is None:
            return

        link = f"[revision history]({ADMIN_REVISIONS_URL.format(chart_id=self.diff.chart_id)})"

        if summary["kind"] == "no_revision":
            # `charts.updatedAt` moved but no admin revision was recorded, which is what a bulk or
            # ETL-driven write looks like. Saying "last edited by X" here would name whoever happened
            # to make the previous manual edit, months earlier.
            st.caption(
                "Production's timestamp moved but no chart revision was recorded, so this is most "
                f"likely a bulk or ETL-driven write rather than someone editing the chart · {link}"
            )
            return

        when = summary["created_at"].strftime("%Y-%m-%d %H:%M")
        n = summary["n_revisions"]
        line = f"Production was last edited by **{summary['author']}** on **{when}**"
        if n > 1:
            line += f" ({n} edits since this staging server was created)"
        line += f" · {link}"
        st.markdown(line)

        # Which field production moved, and from what, is annotated on each field below instead of
        # summarised here — that is where the choice is actually made.
        if not any(f["key"] in self.production_field_changes for f in self.config_compare):
            st.caption(
                "None of the conflicted fields below were changed by it — they differ for some other "
                "reason (schema defaults, or an edit predating this staging server)."
            )

    @property
    def toast_key(self) -> str:
        """Outcome of a resolve, shown once as a toast. Toasts float, so they push no content down."""
        return f"conflict-toast-{self.diff.chart_id}"

    @property
    def error_key(self) -> str:
        """Why a resolve wrote nothing. Stays next to the form until the next attempt."""
        return f"conflict-error-{self.diff.chart_id}"

    def _fail(self, message: str) -> None:
        """Record that nothing was written, both next to the form and as a toast."""
        st.session_state[self.error_key] = message
        st.session_state[self.toast_key] = f"Chart {self.diff.chart_id}: nothing was written"

    def show_error(self) -> None:
        """Show why the last attempt wrote nothing, if it did not."""
        message = st.session_state.get(self.error_key)
        if message is not None:
            st.error(message)

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

    def _show_field_provenance(self, field_key: str) -> None:
        """Say who moved this field in production, and what it moved away from.

        The value it moved *from* is the point of this: staging started from the same value, so
        seeing it makes the choice concrete — production went one way from there, staging another.
        """
        change = self.production_field_changes.get(field_key)
        if change is None:
            st.caption("Unchanged in production since this staging server was created.")
            return

        when = change["created_at"].strftime("%Y-%m-%d %H:%M")
        st.caption(f":material/history: Changed by **{change['author']}** on {when}")
        diff = field_value_diff(change["before"], change["after"])
        if diff:
            st.code(diff, language="diff")

    def show_field_resolver(self, field: dict[str, Any]) -> None:
        with st.container(border=True):
            # Title & layout
            st.markdown(f"##### {field['key']}")

            # Choose option
            choice = self._choose_env(field)

            # Show the field's value on each side
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("**Production**")
                    show_field_value(field["raw1"])
                    self._show_field_provenance(field["key"])
            with col2:
                with st.container(border=True):
                    st.markdown("**Staging**")
                    show_field_value(field["raw2"])

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

        # `rendered_key` holds the text the editor was last seeded with. It is what tells an untouched
        # editor apart from a hand-edited one, both here and in `_resolutions`, so it is written only
        # when the editor is actually seeded.
        seeded_text = st.session_state.get(rendered_key)
        hand_edited = (seeded_text is not None) and (st.session_state.get(editor_key) != seeded_text)
        value_changed = rendered_text != seeded_text

        # Seed the editor when the chosen environment changes (and on first render), and also when
        # the chosen environment's own value changes under us: the diff can be refreshed while the
        # resolver is open, and an editor still showing the old text would write it back over the
        # newer edit -- which is the bug this whole resolver was fixed for, one level down.
        if (st.session_state.get(applied_key) != choice) or (value_changed and not hand_edited):
            st.session_state[editor_key] = rendered_text
            st.session_state[rendered_key] = rendered_text
        st.session_state[applied_key] = choice

        # A hand edit is never thrown away silently; say that it is now based on an outdated value.
        if hand_edited and value_changed and (choice is not None):
            st.warning(
                f"The {ENVIRONMENT_IDS[choice]} value changed while you were editing this field. Your "
                "edit is kept — pick an environment again to start from the new value."
            )

        if choice is None:
            placeholder = "Choose PRODUCTION or STAGING above."
        elif rendered_value is None:
            placeholder = f"This field is not present in {ENVIRONMENT_IDS[choice]}!"
        elif rendered_value == "":
            # Distinguish "explicitly blank" from "not set": emptying the editor removes the field,
            # which is not the same chart (an unset field can be inherited from the indicator).
            placeholder = f"Empty in {ENVIRONMENT_IDS[choice]} — saved as empty, not removed."
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

    def _decisions(self, resolutions: dict[str, Any]) -> dict[str, str]:
        """Where each field's value came from. For the log, not for the screen."""
        decisions = {}
        for field in self.config_compare:
            field_key = field["key"]
            choice = st.session_state[self._radio_key(field_key)]
            origin = ENVIRONMENT_IDS[choice]
            if resolutions[field_key] is None:
                origin += " (removed)"
            elif resolutions[field_key] != field[f"raw{choice}"]:
                origin += " (edited)"
            decisions[field_key] = origin
        return decisions

    def _summary_line(self, resolutions: dict[str, Any]) -> str:
        """One line fit for a toast: a per-field list runs to dozens of lines on a real conflict."""
        counts = Counter(
            ENVIRONMENT_IDS[st.session_state[self._radio_key(field["key"])]] for field in self.config_compare
        )
        parts = []
        for choice in (STAGING, PRODUCTION):
            env = ENVIRONMENT_IDS[choice]
            if counts[env]:
                parts.append(f"{counts[env]} from {env.lower()}")
        removed = sum(1 for value in resolutions.values() if value is None)
        if removed:
            parts.append(f"{removed} removed")
        return ", ".join(parts)

    def resolve_conflicts(self, rerun: bool = False):
        """Gather all resolved conflicts and update chart config in staging.

        Runs as a button callback, so messages are deferred to session state instead of being drawn
        here (drawing elements from a callback inside a fragment duplicates them).
        """
        # Every attempt starts from a clean slate, so a stale error never lingers next to the form.
        st.session_state.pop(self.error_key, None)

        undecided = self.fields_undecided
        if undecided:
            self._fail(
                "Nothing was written: no environment chosen yet for " + ", ".join(f"`{f}`" for f in undecided) + "."
            )
            return

        try:
            resolutions = self._resolutions()
        except FieldParseError as e:
            self._fail(
                f"Nothing was written: could not read the value typed for `{e.field_key}` ({e}). "
                "It must be valid JSON, matching the type of the field."
            )
            return

        summary = self._summary_line(resolutions)
        log.info(
            "chart_diff.resolve_conflicts",
            chart_id=self.diff.chart_id,
            decisions=self._decisions(resolutions),
        )

        with st.spinner("Updating chart on staging..."):
            # Consolidate changes
            config = build_resolved_config(self.diff.source_chart.config, resolutions)

            # Choosing staging for every field leaves the chart exactly as it is. Say so and mark the
            # conflict resolved, rather than pushing an identical config: that would add a chart
            # revision and bump updatedAt, which invalidates an existing approval of this diff.
            if config == build_resolved_config(self.diff.source_chart.config, {}):
                self.diff.set_conflict_to_resolved(self.session)
                st.session_state.pop(f"conflict-write-failed-{self.diff.chart_id}", None)
                st.session_state[self.toast_key] = (
                    f"Chart {self.diff.chart_id} left as it is on staging, conflict resolved ({summary})"
                )
                return

            # Verify the config, but send the config we built: the return value of this function has
            # every schema default filled in (38 extra top-level fields on a typical chart, from
            # `hasMapTab` to `map.region`), which would write values the chart never had as explicit
            # overrides -- they stop following the indicator's metadata and show up as differences
            # against production for ever after. Removing them again afterwards is not the answer
            # either: `validate_chart_config_and_remove_defaults` reverts genuine overrides that
            # happen to equal a schema default (#5911). Resolving a conflict should change the
            # conflicted fields and nothing else.
            try:
                validate_chart_config_and_set_defaults(config, schema=get_schema_from_url(config["$schema"]))
            except Exception as e:
                log.error(e)
                self._fail(f"Nothing was written: the resolved config is not valid. \n\n {e}")
                return

            # User who last edited the chart
            user_id = self.diff.source_chart.lastEditedByUserId

            api = AdminAPI(SOURCE)
            try:
                # Push new chart to staging. isInheritanceEnabled stays in the payload on purpose:
                # update_chart pops it and turns it into the ?inheritance= parameter.
                api.update_chart(
                    chart_id=self.diff.chart_id,
                    chart_config=config,
                    user_id=user_id,
                )
            except HTTPError as e:
                log.error(e)
                st.session_state[f"conflict-write-failed-{self.diff.chart_id}"] = True
                self._fail(
                    f"An error occurred while updating the chart in staging. Please report this to #proj-new-data-workflow. If you are in a rush, you can manually integrate the changes in production [here]({SOURCE.chart_admin_site(self.diff.chart_id)}), and then click on the 'Mark as resolved' button below. \n\n {e}"
                )
            else:
                # Set conflict as resolved
                st.session_state.pop(f"conflict-write-failed-{self.diff.chart_id}", None)
                self.diff.set_conflict_to_resolved(self.session)
                # Signal user that everything went well, and where the values came from
                st.session_state[self.toast_key] = f"Chart {self.diff.chart_id} updated on staging ({summary})"
        if rerun:
            st.rerun()
