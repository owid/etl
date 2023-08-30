"""Handle submission of chart revisions."""
from http.client import RemoteDisconnected
from typing import Any, Dict, List
from urllib.error import URLError

import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

import etl.grapher_model as gm

# from etl.chart_revision.v2.base import ChartUpdater
from apps.wizard.charts.utils import OWIDEnv
from apps.wizard.charts.variable_config import VariableConfig
from etl.chart_revision.v2.core import (
    build_updaters_and_get_charts,
    create_chart_comparison,
    submit_chart_comparisons,
    update_chart_config,
)

# Logger
log = get_logger()


def create_submission(variable_config: VariableConfig, schema_chart_config: Dict[str, Any]) -> "SubmissionConfig":
    """Create submission config."""
    # Get updaters and charts to update

    # If user submitted variable mapping (i.e. clicked on "Next"), then get updaters and charts.
    # This if-else is here to avoid re-runs whenever the user just checks/unchecks boxes or changes dropdown values.
    # We want to recreate the experience of having a "form".
    submission = SubmissionConfig()
    if st.session_state.submitted_variables:
        with st.spinner("Retrieving charts to be updated. This can take up to 1 minute..."):
            try:
                log.info("chart_revision: building updaters and getting charts!")
                st.session_state.variable_mapping = variable_config.variable_mapping
                updaters, charts = build_updaters_and_get_charts_cached(
                    variable_mapping=variable_config.variable_mapping,
                    schema_chart_config=schema_chart_config,
                    skip_slider_check_limit=variable_config.skip_slider_check_limit,
                )
            except (URLError, RemoteDisconnected) as e:
                st.error(e.__traceback__)
            else:
                submission = SubmissionConfig(charts=charts, updaters=updaters)
    # Otherwise, get cached values
    else:
        try:
            log.info("chart_revision: building updaters and getting charts CACHED!")
            updaters, charts = build_updaters_and_get_charts_cached(
                variable_mapping=st.session_state.variable_mapping,
                schema_chart_config=schema_chart_config,
                skip_slider_check_limit=variable_config.skip_slider_check_limit,
            )
        except (URLError, RemoteDisconnected) as e:
            st.error(e.__traceback__)
        else:
            submission = SubmissionConfig(charts=charts, updaters=updaters)
        # st.session_state.charts_obtained = True

    # If we managed to get the charts and updaters, show results.
    if submission.is_valid:
        log.info(f"chart_revision: Submission is valid: {submission}")
        # Display details
        num_charts = len(charts)  # type: ignore
        with st.container():
            st.info(f"""Number of charts to be updated: {num_charts}""")
        with st.expander("🔎  Show variable id mapping"):
            st.write(variable_config.variable_mapping)
        with st.expander("📊  Show affected charts (before update)"):
            st.warning("Charts that are not public at ourworldindata.org will not be rendered correctly.")
            for chart in charts:  # type: ignore
                slug = chart.config["slug"]
                st.markdown(
                    f"""<iframe src="https://ourworldindata.org/grapher/{slug}" loading="lazy" style="width: 100%; height: 600px; border: 0px none;"></iframe>""",
                    unsafe_allow_html=True,
                )

        # Button to finally submit the revisions
        submitted_revisions = st.button(label="🚀 SUBMIT CHART REVISIONS", use_container_width=True, type="primary")
        if submitted_revisions:
            st.session_state.submitted_revisions = True
            log.info(
                f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
            )
        else:
            st.session_state.submitted_revisions = False
        st.divider()
        st.session_state.submitted_variables = False
    return submission


def push_submission(submission_config: "SubmissionConfig", owid_env: OWIDEnv) -> None:
    """Push submissions to the database."""
    # Create chart comparisons
    progress_text = "Submitting chart revisions..."
    bar = st.progress(0, progress_text)
    comparisons = []
    for i, chart in enumerate(submission_config.charts):
        log.info(f"chart_revision: creating comparison for chart {chart.id}")
        # Update chart config
        config_new = update_chart_config(chart.config, submission_config.updaters)
        # Create chart comparison and add to list
        comparison = create_chart_comparison(chart.config, config_new)
        comparisons.append(comparison)
        # Show progress bar
        percent_complete = int(100 * (i + 1) / submission_config.num_charts)
        bar.progress(percent_complete, text=f"{progress_text} {percent_complete}%")

    # Submit chart comparisons
    try:
        submit_chart_comparisons(comparisons)
    except Exception as e:
        st.error(f"Something went wrong! {e}")
    else:
        st.balloons()
        if owid_env.env_type_id == "unknown":
            live_link = "https://owid.cloud/admin/suggested-chart-revisions/review"
            staging_link = "https://staging.owid.cloud/admin/suggested-chart-revisions/review"
            local_link = "http://localhost:3030/admin/suggested-chart-revisions/review"

            st.success(
                f"""
            Chart revisions submitted successfully!

            Now review these at the approval tool:

            - [Live]({live_link})
            - [Staging]({staging_link})
            - [Local]({local_link})
            """
            )
        else:
            st.success(
                f"Chart revisions submitted successfully! Now review these at the [approval tool]({owid_env.chart_approval_tool_url})!"
            )


@st.cache_data(show_spinner=False)
def build_updaters_and_get_charts_cached(variable_mapping, schema_chart_config, skip_slider_check_limit):
    # st.write(variable_mapping)
    return build_updaters_and_get_charts(
        variable_mapping=variable_mapping,
        schema_chart_config=schema_chart_config,
        skip_slider_check_limit=skip_slider_check_limit,
    )


class SubmissionConfig(BaseModel):
    """Form 1."""

    is_valid: bool = False
    charts: List[gm.Chart]
    updaters: List[Any]

    def __init__(self, **data: Any) -> None:
        """Constructor."""
        if "charts" not in data:
            data["charts"] = []
        if "updaters" not in data:
            data["updaters"] = []
        if "charts" in data and "updaters" in data:
            data["is_valid"] = True
        super().__init__(**data)

    @property
    def num_charts(self) -> int:
        """Number of charts in the submission."""
        if self.charts is not None:
            return len(self.charts)
        raise ValueError("Charts have not been set yet! Invalid submission configuration.")
