"""Handle submission of chart revisions."""
from http.client import RemoteDisconnected
from typing import Any, Dict, List
from urllib.error import URLError

import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

import etl.grapher_model as gm
from apps.wizard.pages.charts.variable_config import VariableConfig
from apps.wizard.utils import set_states

# from etl.chart_revision.v2.base import ChartUpdater
from apps.wizard.utils.env import OWID_ENV
from etl.chart_revision.v2.chartgpt import SYSTEM_PROMPT_TEXT, suggest_new_config_fields
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

    ########################################################
    # 1/ Create submission
    ########################################################
    # If user submitted variable mapping (i.e. clicked on "Next (2/3)"), then get updaters and charts in order to create a SubmissionConfig object.
    submission = SubmissionConfig()
    with st.spinner("Retrieving charts to be updated. This can take up to 1 minute..."):
        try:
            log.info("chart_revision: building updaters and getting charts!")
            st.session_state.variable_mapping = variable_config.variable_mapping
            updaters, charts = build_updaters_and_get_charts_cached(
                variable_mapping=variable_config.variable_mapping,
                schema_chart_config=schema_chart_config,
            )
        except (URLError, RemoteDisconnected) as e:
            st.error(e.__traceback__)
        else:
            submission = SubmissionConfig(charts=charts, updaters=updaters)

    ########################################################
    # 2/ Show Submission (allow for GPT tweaks)
    ########################################################
    # If we managed to get the charts and updaters, show results.
    if submission.is_valid:
        # log.info(f"chart_revision: Submission is valid: {submission}")

        # 2.1/ Display details
        num_charts = len(charts)  # type: ignore
        with st.container(border=True):
            st.header(body="Preview")
            col1, col2 = st.columns(2)
            ## Number of charts being updated and variable mapping
            with col1:
                st.info(f"""Number of charts to be updated: {num_charts}""")
            with col2:
                with st.expander("🔎  Show variable id mapping"):
                    st.write(variable_config.variable_mapping)

            # 2.2/ Display charts. Allow for GPT tweaks.
            with st.expander("🧙 **Improve charts with chatGPT**"):
                # Warning on private chartsnot being rendered (iframe can't load them.)
                st.warning(f"Charts that are not public at {OWID_ENV.site} will not be rendered correctly.")

                # Display charts
                ## First column is for the actual chart, second column is for tweaks (GPT)
                for i, chart in enumerate(charts):  # type: ignore
                    col_1, col_2 = st.columns(2)

                    # Actual chart (plotted via iframe)
                    # TODO: Ideally we want to plot this in another way. E.g. using a home-made python viz tool.
                    with col_1:
                        slug = chart.config["slug"]
                        st.markdown(
                            f"""<iframe src="{OWID_ENV.chart_site(slug)}" loading="lazy" style="width: 100%; height: 600px; border: 0px none;"></iframe>""",
                            unsafe_allow_html=True,
                        )

                    # Chart toolkit
                    with col_2:
                        ## Button to run GPT
                        st.button(
                            "Run GPT",
                            key=f"_run_gpt_{i}",
                            on_click=lambda i=i: set_states({f"chat_experimental_{i}": True}),
                        )
                        if (f"chat_experimental_{i}" in st.session_state) and (
                            st.session_state[f"chat_experimental_{i}"]
                        ):
                            ## Form to select alternative titles and subtitles
                            with st.form(key=f"form_{i}"):
                                configs = suggest_new_config_fields(
                                    config=chart.config,
                                    system_prompt=SYSTEM_PROMPT_TEXT,
                                    num_suggestions=3,
                                    model_name="gpt-3.5",
                                )
                                ### Title alternatives
                                new_title = st.selectbox(
                                    "Alternative title", options=[config["title"] for config in configs]
                                )
                                ### Subtitle alternatives
                                new_subtitle = st.selectbox(
                                    "Alternative title", options=[config["subtitle"] for config in configs]
                                )
                                ### Save the new config
                                st.form_submit_button("Update")

            # Button to finally submit the revisions
            st.button(
                label="🚀 SUBMIT CHART REVISIONS (3/3)",
                use_container_width=True,
                type="primary",
                on_click=lambda: set_states({"submitted_revisions": True}),
            )
    return submission


def push_submission(submission_config: "SubmissionConfig") -> None:
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
        if OWID_ENV.env_type_id == "unknown":
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
                f"Chart revisions submitted successfully! Now review these at the [approval tool]({OWID_ENV.chart_approval_tool_url})!"
            )


@st.cache_data(show_spinner=False)
def build_updaters_and_get_charts_cached(variable_mapping, schema_chart_config):
    # st.write(variable_mapping)
    if not variable_mapping:
        msg_error = "No variables selected! Please select at least one variable."
        st.error(msg_error)
    return build_updaters_and_get_charts(
        variable_mapping=variable_mapping,
        schema_chart_config=schema_chart_config,
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
