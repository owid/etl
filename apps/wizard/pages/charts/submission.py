"""Handle submission of chart revisions."""
from http.client import RemoteDisconnected
from typing import Any, Dict, List, Tuple, cast
from urllib.error import URLError

import streamlit as st
from pydantic import BaseModel
from structlog import get_logger

import etl.grapher_model as gm
from apps.wizard.pages.charts.variable_config import VariableConfig
from apps.wizard.utils import set_states

# from etl.chart_revision.v2.base import ChartUpdater
from apps.wizard.utils.env import OWID_ENV
from apps.wizard.utils.gpt import GPTResponse
from etl.chart_revision.v2.chartgpt import SYSTEM_PROMPT_TEXT, suggest_new_config_fields
from etl.chart_revision.v2.core import (
    build_updaters_and_get_charts,
    create_chart_comparison,
    submit_chart_comparisons,
    update_chart_config,
)

# Logger
log = get_logger()
# Session state variable to track gpt tweaks
st.session_state["gpt_tweaks"] = st.session_state.get("gpt_tweaks", {})
# Limit the charts to preview
NUM_CHARTS_LIMIT = 100


@st.cache_data(show_spinner=True)
def suggest_new_config_fields_cached(config, num_suggestions, model_name) -> Tuple[List[Dict[str, Any]], float]:
    """Cache function to avoid multiple calls to the OpenAI API."""
    configs, response = suggest_new_config_fields(
        config=config,
        system_prompt=SYSTEM_PROMPT_TEXT,
        num_suggestions=num_suggestions,
        model_name=model_name,
    )
    response = GPTResponse(chat_completion_instance=response)
    return configs, cast(float, response.cost)


def create_submission(variable_config: VariableConfig, schema_chart_config: Dict[str, Any]) -> "SubmissionConfig":
    """Create submission config."""
    # Get updaters and charts to update

    ########################################################
    # 1/ Create submission
    ########################################################
    # If user submitted variable mapping (i.e. clicked on "Next (2/3)"), then get updaters and charts in order to create a SubmissionConfig object.
    submission_config = SubmissionConfig()
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
            submission_config = SubmissionConfig(charts=charts, updaters=updaters)

    ########################################################
    # 2/ Show Submission (allow for GPT tweaks)
    ########################################################
    # If we managed to get the charts and updaters, show results.
    if submission_config.is_valid:
        # log.info(f"chart_revision: Submission is valid: {submission}")

        # 2.1/ Display details
        num_charts = len(submission_config.charts)  # type: ignore
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
            if num_charts > NUM_CHARTS_LIMIT:
                expander_text = (
                    f"🧙 **Improve charts with chatGPT** (Only the first {NUM_CHARTS_LIMIT} charts are displayed)"
                )
            else:
                expander_text = "🧙 **Improve charts with chatGPT**"
            with st.expander(expander_text):
                # Warning on private chartsnot being rendered (iframe can't load them.)
                if num_charts > NUM_CHARTS_LIMIT:
                    st.warning(
                        f"Too many charts ({num_charts})! Only the first {NUM_CHARTS_LIMIT} charts are displayed."
                    )
                st.warning(f"Charts that are not public at {OWID_ENV.site} will not be rendered correctly.")

                # Display charts
                ## First column is for the actual chart, second column is for tweaks (GPT)
                for i, chart in enumerate(submission_config.charts[:NUM_CHARTS_LIMIT]):  # type: ignore
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
                        show_chart_gpt_toolkit(i, chart)

                    st.divider()

            # Button to finally submit the revisions
            st.button(
                label="🚀 Finish (3/3)",
                use_container_width=True,
                type="primary",
                on_click=lambda: set_states({"submitted_revisions": True}),
            )

    # Add gpt tweaks to submission config
    submission_config.gpt_tweaks = st.session_state.gpt_tweaks
    return submission_config


def show_chart_gpt_toolkit(i: int, chart: gm.Chart) -> None:
    """GPT toolkit for charts."""
    with st.form(key=f"form_gpt_{i}"):
        col1, _ = st.columns([0.4, 0.6])
        with col1:
            model = st.radio(
                label="Model",
                options=["gpt-4-turbo-preview", "gpt-3.5-turbo"],
                help="Select the model to use for generating suggestions.",
                key=f"charts.gpt_model-{i}",
            )
            num_suggestions = st.number_input(
                label="Number of suggestions",
                min_value=1,
                max_value=10,
                value=3,
                help="Select the number of suggestions to generate.",
                key=f"charts.num_suggestions-{i}",
            )
        st.form_submit_button(
            "Ask GPT to improve the FASTT",
            # key=f"_run_gpt_{i}",
            on_click=lambda i=i: set_states({f"chart-experimental-{i}": True, "submitted_revisions": False}),
        )
    if (f"chart-experimental-{i}" in st.session_state) and (st.session_state[f"chart-experimental-{i}"]):
        ## Form to select alternative titles and subtitles
        with st.form(key=f"form_{i}"):
            configs, cost = suggest_new_config_fields_cached(
                config=chart.config,
                num_suggestions=num_suggestions,
                model_name=model,
            )
            ### Title alternatives
            new_title = st.selectbox(
                "Alternative title",
                options=["(Keep current title)"] + [config["title"] for config in configs],
                index=1,
            )
            if "subtitle" in chart.config:
                ### Subtitle alternatives
                new_subtitle = st.selectbox(
                    "Alternative subtitle",
                    options=["(Keep current subtitle)"] + [config["subtitle"] for config in configs],
                    index=1,
                )
            else:
                new_subtitle = None
            st.write(f"💸 Cost: {cost:.4f} USD")
            ### Save the new config
            btn = st.form_submit_button("Update", on_click=lambda: set_states({"submitted_revisions": False}))

        if btn:
            if new_title == "(Keep current title)":
                new_title = None
            if new_subtitle == "(Keep current subtitle)":
                new_subtitle = None
            st.session_state.gpt_tweaks[i] = {
                "title": new_title,
                "subtitle": new_subtitle,
            }
            st.success(
                "🎉 The fields have been saved to the new chart configuration. They are not visible here, but they will be submitted with the chart revisions."
            )


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
        # Fine tune FASTT if GPT tweaks were submitted
        if i in submission_config.gpt_tweaks:
            tweak = submission_config.gpt_tweaks[i]
            if tweak["title"] is not None:
                config_new["title"] = tweak["title"]
            if tweak["subtitle"] is not None:
                config_new["subtitle"] = tweak["subtitle"]
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


@st.cache_data(show_spinner="Querying ChatGPT...")
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
    gpt_tweaks: Dict[int, Any] = {}

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

    def add_gpt_tweaks(self, index: int, title: str | None = None, subtitle: str | None = None) -> None:
        """Add GPT tweaks."""
        if index not in self.gpt_tweaks:
            self.gpt_tweaks[index] = {}
        if title is not None:
            self.gpt_tweaks[index]["title"] = title
        if subtitle is not None:
            self.gpt_tweaks[index]["subtitle"] = subtitle
