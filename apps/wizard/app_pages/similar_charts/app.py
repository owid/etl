import random
import re
import time

import pandas as pd
import streamlit as st
import torch
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger

from apps.wizard.app_pages.insight_search import embeddings as emb
from apps.wizard.app_pages.similar_charts import data, scoring
from apps.wizard.utils import cached, set_states, url_persist
from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, tag_in_md
from etl.config import OWID_ENV

DEVICE = "cpu"

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


# TODO: convert chart to dataclass
def st_display_chart(chart, show_score=True):
    tags = chart["tags"].split(";") if chart["tags"] else []

    # TODO: fix this URL
    chart_url = OWID_ENV.chart_site(chart["slug"])
    url_admin = "xxx"

    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"#### [{chart['title']}]({chart_url})")
            st.markdown(f"Subtitle: {chart["subtitle"]}")
            st.markdown(f"Tags: **{', '.join(tags)}**")
        if show_score:
            with col2:
                st.markdown(f"#### Similarity: {chart['similarity']:.0%}")
                st.table(
                    pd.Series(
                        {
                            "Tags": 0.1,
                            "Semantic": 0.2,
                        }
                    )
                    .to_frame("score")
                    .style.format("{:.0%}")
                )
                # TODO: Add scoring information here
                # st.write("Tags: +10%")
                # st.write("Semantic: +20%")

    return


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


def indicator_query(indicator: dict) -> str:
    return indicator["name"] + " " + indicator["description"] + " " + (indicator["catalogPath"] or "")


def chart_text(chart: dict) -> str:
    return chart["title"]


########################################################################################################################
# Get embedding model.
MODEL = emb.get_model()
# Fetch all data indicators.
charts = data.get_charts()

scoring_model = scoring.ScoringModel(MODEL, weights={"title": 1.0, "subtitle": 1e-9})

scoring_model.fit(charts)

########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Similar charts")

# Box for input text.
chart_slug_or_id = st.text_input(
    label="Chart slug or ID",
    placeholder="Type something...",
    value="human-trafficking-victims-over-18-years-old-male-vs-female",
    help="Keep it empty to get a random chart.",
)

st_multiselect_wider()
with st_horizontal():
    # Filter indicators
    pass


if chart_slug_or_id == "":
    # pick random chart
    chosen_chart = random.sample(charts, 1)[0]
else:
    # find chart by slug or id
    chosen_chart = next(
        (chart for chart in charts if chart.slug == chart_slug_or_id or str(chart.chart_id) == chart_slug_or_id),
        None,
    )
    if not chosen_chart:
        st.error(f"Chart with slug or ID '{chart_slug_or_id}' not found.")
        st.stop()

# Get the sorted indicators.
# sorted_inds = emb.get_sorted_documents_by_similarity(MODEL, query, docs=indicators, embeddings=embeddings)  # type: ignore

# Display chosen chart
st_display_chart(chosen_chart, show_score=False)

# Advanced expander.
st.session_state.sim_charts_expander_advanced_options = st.session_state.get(
    "sim_charts_expander_advanced_options", False
)

# Scores.
# These are the default thresholds for the different scores.
st.session_state.w_title = st.session_state.get("w_title", 1.0)
st.session_state.w_subtitle = st.session_state.get("w_subtitle", 1e-9)
st.session_state.w_tags = st.session_state.get("w_tags", 1e-9)
st.session_state.w_pageviews = st.session_state.get("w_pageviews", 1e-9)


with st.expander("Advanced options", expanded=st.session_state.sim_charts_expander_advanced_options):
    for score_name in ["title", "subtitle", "tags", "pageviews"]:
        # For some reason, if the slider minimum value is zero, streamlit raises an error when the slider is
        # dragged to the minimum. Set it to a small, non-zero number.
        url_persist(st.slider)(
            f"Weight for {score_name} score",
            min_value=1e-9,
            max_value=1.0,
            # step=0.001,
            key=f"w_{score_name}",
        )

scoring_model.set_weights(
    {
        "title": st.session_state.w_title,
        "subtitle": st.session_state.w_subtitle,
        "tags": st.session_state.w_tags,
        "pageviews": st.session_state.w_pageviews,
    }
)

# Horizontal divider
st.markdown("---")

similarity_dict = scoring_model.similarity(chosen_chart)

for chart in charts:
    chart.similarity = similarity_dict[chart.chart_id]

sorted_charts = sorted(charts, key=lambda x: x.similarity, reverse=True)


# Use pagination
items_per_page = 10
pagination = Pagination(
    items=sorted_charts,
    items_per_page=items_per_page,
    pagination_key=f"pagination-di-search-{chart_slug_or_id}",
)

if len(charts) > items_per_page:
    pagination.show_controls(mode="bar")

# Show items (only current page)
for item in pagination.get_page_items():
    # Don't show the chosen chart
    if item.slug == chosen_chart.slug:
        continue
    st_display_chart(item)
