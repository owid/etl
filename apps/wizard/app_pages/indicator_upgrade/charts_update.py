"""Handle submission of chart updates."""
from http.client import RemoteDisconnected
from typing import Dict, List
from urllib.error import URLError

import pandas as pd
import streamlit as st
from structlog import get_logger

import etl.grapher_model as gm
from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.utils import set_states, st_page_link, st_toast_error
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV
from etl.helpers import get_schema_from_url
from etl.indicator_upgrade.indicator_update import find_charts_from_variable_ids, update_chart_config

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
    with st.spinner("Retrieving charts to be updated. This can take up to 1 minute..."):
        try:
            log.info("building updaters and getting charts!")
            st.session_state.indicator_mapping = indicator_mapping
            # Get charts
            charts = find_charts_from_variable_ids(set(indicator_mapping.keys()))
        except (URLError, RemoteDisconnected) as e:
            st.error(e.__traceback__)
            charts = []

    ########################################################
    # 2/ Preview submission
    ########################################################
    # 2.1/ Display details
    if (num_charts := len(charts)) > 0:
        with st.container(border=True):
            ## Number of charts being updated and variable mapping
            with st.popover(f"{num_charts} charts affected!"):
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
                use_container_width=True,
                type="primary",
                on_click=trigger_chart_submission,
            )
    else:
        st.warning("No charts found to update with the given variable mapping.")
        charts = []

    return charts


def push_new_charts(charts: List[gm.Chart]) -> None:
    """Updating charts in the database."""
    # API to interact with the admin tool
    # HACK: Forcing grapher user to be Admin so that it is detected by chart sync.
    api = AdminAPI(OWID_ENV, grapher_user_id=1)
    # Update charts
    progress_text = "Updating charts..."
    bar = st.progress(0, progress_text)
    try:
        for i, chart in enumerate(charts):
            log.info(f"creating comparison for chart {chart.id}")
            # Update chart config
            config_new = update_chart_config(
                chart.config,
                st.session_state.indicator_mapping,
                get_schema_from_url(chart.config["$schema"]),
            )
            # Push new chart to DB
            if chart.id:
                chart_id = chart.id
            elif "id" in chart.config:
                chart_id = chart.config["id"]
            else:
                raise ValueError(f"Chart {chart} does not have an ID in config.")
            api.update_chart(
                chart_id=chart_id,
                chart_config=config_new,
            )
            # Show progress bar
            percent_complete = int(100 * (i + 1) / len(charts))
            bar.progress(percent_complete, text=f"{progress_text} {percent_complete}%")
    except Exception as e:
        st.error(
            "Something went wrong! Maybe the server was not properly launched? Check the job on the GitHub pull request."
        )
        st.exception(e)
    else:
        st.success(
            "The charts were successfully updated! If indicators from other datasets also need to be upgraded, simply refresh this page, otherwise move on to `chart diff` to review all changes."
        )
        st_page_link("chart-diff")


def save_variable_mapping(
    indicator_mapping: Dict[int, int], dataset_id_new: int, dataset_id_old: int, comments: str = ""
) -> None:
    WizardDB.add_variable_mapping(
        mapping=indicator_mapping,
        dataset_id_new=dataset_id_new,
        dataset_id_old=dataset_id_old,
        comments=comments,
    )
