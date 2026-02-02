"""Handle submission of chart updates."""

from http.client import RemoteDisconnected
from typing import Dict, List
from urllib.error import URLError

import pandas as pd
import streamlit as st
from structlog import get_logger

import etl.grapher.model as gm
from apps.indicator_upgrade.upgrade import (
    cli_upgrade_indicators,
    get_affected_narrative_charts_cli,
    push_new_narrative_charts_cli,
)
from apps.wizard.utils import set_states
from apps.wizard.utils.components import st_toast_error, st_wizard_page_link
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV
from etl.indicator_upgrade.indicator_update import find_charts_from_variable_ids

# Logger
log = get_logger()


def trigger_chart_submission():
    if st.session_state.submitted_indicators:
        set_states({"submitted_charts": True})
    else:
        st_toast_error("You've changed the indicator mapping. Please submit the form before to update the charts.")


def get_affected_charts_and_preview(indicator_mapping: Dict[int, int]) -> List[gm.Chart]:
    """Create submission config."""
    # Get updaters and charts to update

    ########################################################
    # 1/ Get affected charts
    ########################################################
    # If user submitted variable mapping (i.e. clicked on "Next (2/3)"), then get charts and update them accordingly.
    with st.spinner("Retrieving charts to be updated. This can take up to 1 minute...", show_time=True):
        try:
            log.info("building updaters and getting charts!")
            st.session_state.indicator_mapping = indicator_mapping
            # Get charts
            charts = find_charts_from_variable_ids(set(indicator_mapping.keys()))
        except (URLError, RemoteDisconnected) as e:
            st.error(e.__traceback__)
            charts = []

    ########################################################
    # 2/ Get affected narrative charts
    ########################################################
    narrative_charts = get_affected_narrative_charts_cli(charts) if charts else []
    num_narrative_charts = len(narrative_charts)

    ########################################################
    # 3/ Preview submission
    ########################################################
    # 3.1/ Display details
    if (num_charts := len(charts)) > 0:
        with st.container(border=True):
            ## Number of charts being updated and variable mapping
            if num_narrative_charts > 0:
                popover_label = f"{num_charts} charts & {num_narrative_charts} narrative charts affected!"
            else:
                popover_label = f"{num_charts} charts affected!"
            with st.popover(popover_label):
                # Build Series with slugs
                slugs = pd.DataFrame(
                    {
                        "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],  # type: ignore
                        "url": [OWID_ENV.chart_site(chart.slug) for chart in charts],  # type: ignore
                    }
                )
                st.dataframe(
                    slugs,
                    column_config={
                        "url": st.column_config.LinkColumn(
                            label="Chart",
                            help="Link to affected chart (as-is)",
                            display_text=r"https?://.*?/grapher/(.*)",
                        ),
                        "thumbnail": st.column_config.ImageColumn(
                            "Image",
                            help="Chart thumbnail",
                        ),
                    },
                )
            with st.popover("Show variable id mapping"):
                st.write(indicator_mapping)

            # Button to finally submit the revisions
            st.button(
                label="🚀 Update charts (3/3)",
                width="stretch",
                type="primary",
                on_click=trigger_chart_submission,
            )
    else:
        st.warning("No charts found to update with the given variable mapping.")
        charts = []

    return charts


def push_new_charts() -> None:
    """Updating charts and narrative charts in the database."""
    with st.spinner("Updating charts and narrative charts..."):
        try:
            # Use the CLI function which handles both charts and narrative charts
            result = cli_upgrade_indicators(dry_run=False)

            if not result["success"]:
                # Partial failure - some charts updated successfully, but some failed
                chart_errors = result["chart_errors"]
                narrative_chart_errors = result["narrative_chart_errors"]
                total_errors = len(chart_errors) + len(narrative_chart_errors)

                st.warning(
                    f"⚠️ Indicator upgrade completed with {total_errors} errors. "
                    "Some charts were updated successfully, but there were errors with others. "
                    "You can proceed to chart-diff to review the successful updates, "
                    "but some charts may need manual attention."
                )

                # Display chart errors
                if chart_errors:
                    with st.expander(f"❌ Chart Errors ({len(chart_errors)})", expanded=True):
                        error_data = []
                        for err in chart_errors:
                            chart_url = OWID_ENV.chart_site(err["chart_slug"])
                            error_data.append(
                                {
                                    "Chart ID": err["chart_id"],
                                    "Chart URL": chart_url,
                                    "Error": err["error"],
                                }
                            )
                        st.dataframe(
                            pd.DataFrame(error_data),
                            column_config={
                                "Chart URL": st.column_config.LinkColumn(
                                    "Chart URL",
                                    display_text=r"https?://.*?/grapher/(.*)",
                                ),
                            },
                            hide_index=True,
                        )

                # Display narrative chart errors
                if narrative_chart_errors:
                    with st.expander(f"❌ Narrative Chart Errors ({len(narrative_chart_errors)})", expanded=True):
                        error_data = []
                        for err in narrative_chart_errors:
                            error_data.append(
                                {
                                    "Narrative Chart ID": err["narrative_chart_id"],
                                    "Name": err["name"],
                                    "Error": err["error"],
                                }
                            )
                        st.dataframe(pd.DataFrame(error_data), hide_index=True)

                st_wizard_page_link("anomalist")
                st_wizard_page_link("chart-diff")
            else:
                st.success(
                    "✅ The charts were successfully updated! If indicators from other datasets also need to be upgraded, simply refresh this page, otherwise move on to `chart diff` to review all changes."
                )
                st_wizard_page_link("anomalist")
                st_wizard_page_link("chart-diff")

        except Exception as e:
            st.error(
                "❌ Something went wrong! Maybe the server was not properly launched? Check the job on the GitHub pull request."
            )
            st.exception(e)


def save_variable_mapping(
    indicator_mapping: Dict[int, int], dataset_id_new: int, dataset_id_old: int, comments: str = ""
) -> None:
    WizardDB.add_variable_mapping(
        mapping=indicator_mapping,
        dataset_id_new=dataset_id_new,
        dataset_id_old=dataset_id_old,
        comments=comments,
    )


def undo_indicator_upgrade(indicator_mapping):
    mapping_inverted = {v: k for k, v in indicator_mapping.items()}
    with st.spinner("Undoing upgrade..."):
        # Get affected charts
        charts = get_affected_charts_and_preview(
            mapping_inverted,
        )

        # Get affected narrative charts (children of affected charts)
        narrative_charts = get_affected_narrative_charts_cli(charts)

        # TODO: instead of pushing new charts, we should revert the changes!
        # To do this, we should have kept a copy or reference to the original revision.
        # Idea: when 'push_new_charts' is called, store in a table the original revision of the chart.
        push_new_charts()

        # Undo narrative chart upgrades
        if narrative_charts:
            push_new_narrative_charts_cli(narrative_charts, mapping_inverted, dry_run=False)

        # Reset variable mapping
        WizardDB.delete_variable_mapping()


@st.dialog("Undo upgrade", width="large")
def undo_upgrade_dialog():
    mapping = WizardDB.get_variable_mapping()

    if mapping != {}:
        st.markdown(
            "The following table shows the indicator mapping that has been applied to the charts. Undoing means inverting this mapping."
        )
        data = {
            "id_old": list(mapping.keys()),
            "id_new": list(mapping.values()),
        }
        st.dataframe(data)
        st.button(
            "Undo upgrade",
            on_click=lambda m=mapping: undo_indicator_upgrade(m),
            icon=":material/undo:",
            help="Undo all indicator upgrades",
            type="primary",
            key="btn_undo_upgrade_2",
        )
        st.warning(
            "Charts will still appear in chart-diff. This is because the chart configs have actually changed (their version has beem bumped). In the future, we do not want to show these charts in chart-diff. For the time being, you should reject these chart diffs."
        )
    else:
        st.markdown("No indicator mapping found. Nothing to undo.")
