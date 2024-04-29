"""Handle submission of chart revisions."""
from http.client import RemoteDisconnected
from typing import Any, Dict, List
from urllib.error import URLError

import pandas as pd
import streamlit as st
from structlog import get_logger

import etl.grapher_model as gm
from apps.staging_sync.admin_api import AdminAPI
from apps.wizard.utils import set_states, st_page_link
from apps.wizard.utils.env import OWID_ENV
from etl.chart_revision.v3.indicator_update import find_charts_from_variable_ids, update_chart_config
from etl.db import get_engine

# Logger
log = get_logger()
# Session state variable to track gpt tweaks
st.session_state["gpt_tweaks"] = st.session_state.get("gpt_tweaks", {})
# Limit the charts to preview
NUM_CHARTS_LIMIT = 100


def get_affected_charts_and_preview(indicator_mapping: Dict[int, int]) -> List[gm.Chart]:
    """Create submission config."""
    # Get updaters and charts to update

    ########################################################
    # 1/ Get affected charts
    ########################################################
    # If user submitted variable mapping (i.e. clicked on "Next (2/3)"), then get charts and update them accordingly.
    with st.spinner("Retrieving charts to be updated. This can take up to 1 minute..."):
        try:
            log.info("chart_revision: building updaters and getting charts!")
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
    if num_charts := len(charts) > 0:
        with st.container(border=True):
            ## Number of charts being updated and variable mapping
            with st.popover(f"{num_charts} charts affected!"):
                # Build Series with slugs
                slugs = pd.DataFrame(
                    {
                        "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],
                        "url": [OWID_ENV.chart_site(chart.slug) for chart in charts],
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
                on_click=lambda: set_states({"submitted_charts": True}),
            )
    else:
        st.warning("No charts found to update with the given variable mapping.")
        charts = []

    return charts


def push_new_charts(charts: List[gm.Chart], schema_chart_config: Dict[str, Any]) -> None:
    """Push submissions to the database."""
    # API to interact with the admin tool
    engine = get_engine()
    api = AdminAPI(engine)
    # Update charts
    progress_text = "Submitting chart revisions..."
    bar = st.progress(0, progress_text)
    for i, chart in enumerate(charts):
        log.info(f"chart_revision: creating comparison for chart {chart.id}")
        # Update chart config
        config_new = update_chart_config(
            chart.config,
            st.session_state.indicator_mapping,
            schema_chart_config,
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

    # Push new charts to live
    try:
        st.write("Push new charts to the database")
    except Exception as e:
        st.error(f"Something went wrong! {e}")
    else:
        st.success("The charts were successfully updated! Review the changes with `chart diff`")
        st_page_link("chart_diff")
