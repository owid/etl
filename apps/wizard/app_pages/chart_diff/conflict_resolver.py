import ast
import json
from copy import deepcopy
from typing import cast

import requests
import streamlit as st
from requests.exceptions import HTTPError
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff
from apps.wizard.app_pages.chart_diff.utils import SOURCE
from etl.indicator_upgrade.schema import validate_chart_config_and_set_defaults

ENVIRONMENT_IDS = {
    1: "PRODUCTION",
    2: "STAGING",
}


class ChartDiffConflictResolver:
    """Resolve conflicts between charts.

    Provides UI.
    """

    def __init__(self, diff: ChartDiff, session: Session):
        # Chart diff
        self.diff = diff
        # Session (needed to update conflict table in db)
        self.session = session
        # Compare chart configs
        self.config_compare = compare_chart_configs(
            self.diff.target_chart.config,  # type: ignore
            self.diff.source_chart.config,
        )
        # Resolved values. key -> value, where key is conflicted field and value is the resolution.
        self.value_resolved = {}

    def run(self) -> None:
        """Run conflict resolver."""
        st.warning("This is under development! Find below a form with the different fields that present conflicts.")

        # If things to compare...
        if self.config_compare:
            st.markdown(
                "Find below the chart config fields that do not match. Choose the value you want to keep for each of the fields (or introduce a new one)."
            )

            # Show conflict resolver per field
            ## Provide tools to merge the content of each field
            for field in self.config_compare:
                self._show_field_conflict_resolver(field)

            # Button to resolve all conflicts
            if st.button(
                "Resolve conflicts",
                help="Click to resolve the conflicts and update the chart config.",
                key="resolve-conflicts-btn",
                type="primary",
            ):
                self.resolve_conflicts(rerun=True)
        else:
            st.success(
                "No conflicts found actually. Unsure why you were prompted with the conflict resolver. Please report."
            )

    def _show_field_conflict_resolver(self, field):
        with st.container(border=True):
            # Title & layout
            st.header(field["key"])

            # Choose option
            choice = self._choose_env(field)

            # Show the fields values
            col1, col2 = st.columns(2)
            with col1:
                with st.expander("PRODUCTION", expanded=True):
                    st.write(field["value1"])
            with col2:
                with st.expander("STAGING", expanded=True):
                    st.write(field["value2"])

            # Merge editor
            self._show_merge_editor(field, choice)

    def _choose_env(self, field) -> int:
        """Choose environment to keep the value from.

        The user can edit the value later on.
        """
        # Show option radio buttons
        value = st.radio(
            "Choose config from...",
            options=[1, 2],
            captions=["Insert production config", "Insert staging config"],
            format_func=lambda x: ENVIRONMENT_IDS[x],
            key=f"conflict-radio-{field['key']}",
            horizontal=True,
            label_visibility="collapsed",
        )

        return cast(int, value)

    def _show_merge_editor(self, field, choice):
        """Edit the content of the field."""
        is_none = field[f"value{choice}"] is None
        self.value_resolved[field["key"]] = st.text_area(
            "Edit config",
            value=None if is_none else str(field[f"value{choice}"]),
            placeholder=f"This field is not present in {ENVIRONMENT_IDS[choice]}!" if is_none else "",
            help="Edit the final config here. When cliking on 'Resolve conflicts', this value will be used to update the chart config.",
            disabled=is_none,
        )

    def resolve_conflicts(self, rerun: bool = False):
        """Gather all resolved conflicts and update chart config in staging."""
        with st.spinner("Updating chart on staging..."):
            # Consolidate changes
            config = deepcopy(self.diff.source_chart.config)
            for field_key, field_resolution in self.value_resolved.items():
                if self.value_resolved[field_key] is None:
                    config.pop(field_key, None)
                else:
                    # st.write(field_key)
                    config_field = as_valid_json(field_resolution)
                    config_field = as_list(config_field)
                    config[field_key] = config_field
                    # st.write(f"{field_key}: {type(config[field_key]).__name__}")

            # # Get rid of special fields
            fields_remove = [
                "bakedGrapherURL",
                "adminBaseUrl",
                "dataApiUrl",
            ]
            for field in fields_remove:
                config.pop(field, None)

            # Verify config
            config_new = validate_chart_config_and_set_defaults(config, schema=get_schema("004"))

            # Push to staging
            api = AdminAPI(SOURCE.engine, grapher_user_id=1)
            try:
                api.update_chart(
                    chart_id=self.diff.chart_id,
                    chart_config=config_new,
                )
            except HTTPError as e:
                st.error(
                    f"An error occurred while updating the chart in staging. Please report this to #proj-new-data-workflow. If you are in a rush, you can manually integrate the changes in production [here]({SOURCE.chart_admin_site(self.diff.chart_id)}).\n\n {e}"
                )
            else:
                # Set conflict as resolved
                self.diff.set_conflict_to_resolved(self.session)
                # Signal user that everything went well
                st.success(
                    "Conflicts have been resolved. The chart in staging has been updated. You can close this window."
                )
        # if rerun:
        #     st.rerun()


def as_valid_json(s):  # -> Any:
    """Return `s` as a dictionary if applicable."""
    # Preprocess the string to replace single quotes with double quotes
    s = s.replace("'", '"')

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return s


def as_list(s):
    """Return `s` as a list if applicable."""
    if isinstance(s, str):
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError):
            return s
    return s


def compare_chart_configs(c1, c2):
    """Compare to chart configs c1 and c2."""
    keys = set(c1.keys()).union(c2.keys())
    diff_list = []
    KEYS_IGNORE = {
        "bakedGrapherURL",
        "adminBaseUrl",
        "dataApiUrl",
        # "version",
    }
    for key in keys:
        if key in KEYS_IGNORE:
            continue
        value1 = c1.get(key)
        value2 = c2.get(key)
        if value1 != value2:
            if isinstance(value1, dict):
                value1 = json.dumps(value1, indent=4)
            if isinstance(value2, dict):
                value2 = json.dumps(value2, indent=4)
            diff_list.append(
                {
                    "key": key,
                    "value1": value1,
                    "value2": value2,
                }
            )

    return diff_list


def get_schema(schema_version: str = "004"):
    return requests.get(
        f"https://files.ourworldindata.org/schemas/grapher-schema.{schema_version}.json",
        timeout=20,
    ).json()


def st_show_conflict_resolver(diff: ChartDiff, session: Session) -> None:
    """Conflict resolver."""
    ChartDiffConflictResolver(diff, session).run()
