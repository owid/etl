import random

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.similar_charts import data, scoring
from apps.wizard.utils import embeddings as emb
from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, url_persist
from etl.config import OWID_ENV

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Similar Charts",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# FUNCTIONS
########################################################################################################################


@st.cache_data(show_spinner=False, persist="disk")
def get_charts() -> list[data.Chart]:
    with st.spinner("Loading charts..."):
        # Get charts from the database..
        df = data.get_raw_charts()

        charts = df.to_dict(orient="records")

    ret = []
    for c in charts:
        c["tags"] = c["tags"].split(";") if c["tags"] else []
        ret.append(data.Chart(**c))  # type: ignore

    return ret


def st_chart_info(chart: data.Chart) -> None:
    chart_url = OWID_ENV.chart_site(chart.slug)
    title = f"#### [{chart.title}]({chart_url})"
    if chart.gpt_reason:
        title += " 🤖"
    st.markdown(title)
    st.markdown(f"Slug: {chart.slug}")
    st.markdown(f"Subtitle: {chart.subtitle}")
    st.markdown(f"Tags: **{', '.join(chart.tags)}**")
    st.markdown(f"Pageviews: **{chart.views_365d}**")


def st_chart_scores(chart: data.Chart, sim_components: pd.DataFrame) -> None:
    st.markdown(f"#### Similarity: {chart.similarity:.0%}")
    st.table(sim_components.loc[chart.chart_id].to_frame("score").style.format("{:.0%}"))
    if chart.gpt_reason:
        st.markdown(f"**GPT Diversity Reason**:\n{chart.gpt_reason}")


def st_display_chart(
    chart: data.Chart,
    sim_components: pd.DataFrame = pd.DataFrame(),
) -> None:
    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            st_chart_info(chart)
        with col2:
            st_chart_scores(chart, sim_components)


def split_input_string(input_string: str) -> tuple[str, list[str], list[str]]:
    """Break input string into query, includes and excludes."""
    # Break input string into query, includes and excludes
    query = []
    includes = []
    excludes = []
    for term in input_string.split():
        if term.startswith("+"):
            includes.append(term[1:].lower())
        elif term.startswith("-"):
            excludes.append(term[1:].lower())
        else:
            query.append(term)

    return " ".join(query), includes, excludes


@st.cache_data(show_spinner=False, max_entries=1)
def get_and_fit_model(charts: list[data.Chart]) -> scoring.ScoringModel:
    with st.spinner("Loading model..."):
        scoring_model = scoring.ScoringModel(emb.get_model())
    scoring_model.fit(charts)
    return scoring_model


########################################################################################################################
# Fetch all data indicators.
charts = get_charts()
# Get scoring model.
scoring_model = get_and_fit_model(charts)

########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Similar charts")

col1, col2 = st.columns(2)
with col2:
    st_multiselect_wider()
    with st_horizontal():
        random_chart = st.button("Random chart", help="Get a random chart.")

        # Filter indicators
        diversity_gpt = url_persist(st.checkbox, default=True)(
            "Diversity with GPT",
            key="diversity_gpt",
            help="Use GPT to select 5 most diverse charts from the top 30 similar charts.",
        )

    # Random chart was pressed or no search text
    if random_chart or not st.query_params.get("chart_search_text"):
        chart_slug = random.sample(charts, 1)[0].slug
        st.session_state["chart_search_text"] = chart_slug

    # chart_search_text = url_persist(st.text_input)(
    #     key="chart_search_text",
    #     label="Chart slug or ID",
    #     placeholder="Type something...",
    # )

    chart_search_text = url_persist(st.selectbox)(
        "Select a chart",
        key="chart_search_text",
        options=[c.slug for c in charts],
    )

    # Advanced expander.
    st.session_state.sim_charts_expander_advanced_options = st.session_state.get(
        "sim_charts_expander_advanced_options", False
    )

    # Weights for each score
    with st.expander("Advanced options", expanded=st.session_state.sim_charts_expander_advanced_options):
        # Add text area for system prompt
        system_prompt = url_persist(st.text_area, default=scoring.DEFAULT_SYSTEM_PROMPT)(
            "GPT prompt for selecting diverse results",
            key="gpt_system_prompt",
            height=150,
        )

        for score_name in ["title", "subtitle", "tags", "pageviews", "share_indicator"]:
            # For some reason, if the slider minimum value is zero, streamlit raises an error when the slider is
            # dragged to the minimum. Set it to a small, non-zero number.
            key = f"w_{score_name}"

            # Set default values
            if key not in st.session_state:
                st.session_state[key] = scoring.DEFAULT_WEIGHTS[score_name]

            url_persist(st.slider, default=scoring.DEFAULT_WEIGHTS[score_name])(
                f"Weight for {score_name} score",
                min_value=1e-9,
                max_value=1.0,
                # step=0.001,
                key=key,
            )

            scoring_model.weights[score_name] = st.session_state[key]


# Find a chart based on inputs
chosen_chart = next(
    (chart for chart in charts if chart.slug == chart_search_text or str(chart.chart_id) == chart_search_text),
    None,
)
if not chosen_chart:
    st.error(f"Chart with slug {chart_search_text} not found.")

    # # Find a chart by title
    # chart_id = scoring_model.similar_chart_by_title(chart_search_text)
    # chosen_chart = next((chart for chart in charts if chart.chart_id == chart_id), None)

assert chosen_chart

# Display chosen chart
with col1:
    st_chart_info(chosen_chart)


# Horizontal divider
st.markdown("---")

sim_dict = scoring_model.similarity(chosen_chart)
sim_components = scoring_model.similarity_components(chosen_chart)

for chart in charts:
    chart.similarity = sim_dict[chart.chart_id]

sorted_charts = sorted(charts, key=lambda x: x.similarity, reverse=True)  # type: ignore

# Postprocess charts with GPT and prioritize diversity
if diversity_gpt:
    with st.spinner("Diversifying chart results..."):
        slugs_to_reasons = scoring.gpt_diverse_charts(chosen_chart, sorted_charts, system_prompt=system_prompt)
    for chart in sorted_charts:
        if chart.slug in slugs_to_reasons:
            chart.gpt_reason = slugs_to_reasons[chart.slug]

    # Put charts that are diverse at the top
    # sorted_charts = sorted(sorted_charts, key=lambda x: (x.gpt_reason is not None, x.similarity), reverse=True)

# Use pagination
items_per_page = 20
pagination = Pagination(
    items=sorted_charts,
    items_per_page=items_per_page,
    pagination_key=f"pagination-di-search-{chosen_chart.slug}",
)

if len(charts) > items_per_page:
    pagination.show_controls(mode="bar")

# Show items (only current page)
for item in pagination.get_page_items():
    # Don't show the chosen chart
    if item.slug == chosen_chart.slug:
        continue
    st_display_chart(item, sim_components)
