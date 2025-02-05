import datetime as dt
import random
from typing import List

import numpy as np
import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.similar_charts import data, scoring
from apps.wizard.utils import embeddings as emb
from apps.wizard.utils.cached import get_grapher_user
from apps.wizard.utils.components import st_horizontal, st_multiselect_wider, url_persist
from etl import paths
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.git_helpers import log_time

# PROFILER = start_profiler()

ITEMS_PER_PAGE = 20

# Initialize logger.
log = get_logger()

# Database engine.
engine = get_engine()

# Get reviewer's name (if needed).
reviewer = get_grapher_user().fullName

# Page configuration.
st.set_page_config(
    page_title="Wizard: Similar Charts",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# CONSTANTS & FUNCTIONS
########################################################################################################################


@st.cache_data(show_spinner=False, ttl="1h")
def get_charts() -> list[data.Chart]:
    """Fetch chart metadata from the database and return a list of Chart objects."""
    with st.spinner("Loading charts..."):
        df = data.get_raw_charts()
        if len(df) == 0:
            raise ValueError("No charts found in the database.")
        charts = df.to_dict(orient="records")

    results = []
    for c in charts:
        c["tags"] = c["tags"].split(";") if c["tags"] else []
        results.append(data.Chart(**c))  # type: ignore
    return results


@log_time
@st.cache_data(show_spinner=False)
def get_coviews() -> pd.Series:
    """Return a Series with the number of coviewed sessions for each chart over the last 365 days."""
    with st.spinner("Loading coviews..."):
        return data.get_coviews_sessions(after_date=str(dt.date.today() - dt.timedelta(days=365)), min_sessions=3)


@log_time
@st.cache_data(show_spinner=False)
def get_directional_coviews() -> pd.DataFrame:
    """Load pre-processed directional coviews from a Feather file."""
    return pd.read_feather(paths.BASE_DIR / "apps/wizard/app_pages/similar_charts/playground_coviews.feather")


def st_chart_info(chart: data.Chart, show_coviews: bool = True) -> None:
    """Display title, subtitle, tags, pageviews, and optional coviews of a given chart."""
    chart_url = OWID_ENV.chart_site(chart.slug)
    title = f"[{chart.title}]({chart_url})"
    # Add GPT marker if present
    if chart.gpt_reason:
        title += " 🤖"

    st.subheader(title, anchor=chart.slug)
    st.markdown(f"**Slug**: {chart.slug}")
    st.markdown(f"**Subtitle**: {chart.subtitle}")
    st.markdown(f"**Tags**: {', '.join(chart.tags)}")
    st.markdown(f"**Pageviews (365d)**: {chart.views_365d}")
    if show_coviews:
        st.markdown(f"**Coviews**: {chart.coviews}")


@log_time
@st.cache_data(
    show_spinner=False,
    max_entries=1,
    hash_funcs={list[data.Chart]: lambda charts: len(charts)},
)
def get_and_fit_model(charts: list[data.Chart]) -> scoring.ScoringModel:
    """Load an embedding model and fit it to the charts for similarity scoring."""
    with st.spinner("Loading model..."):
        scoring_model = scoring.ScoringModel(emb.get_model())
    with st.spinner("Fitting model..."):
        scoring_model.fit(charts)
    return scoring_model


def st_related_charts_table(
    df: pd.DataFrame, n: int = 6, drop_cols: list[str] = ["score", "coviews_after", "coviews_before"]
) -> None:
    """
    Displays a table of related charts, sorted by `score`.

    Columns displayed in the table:
    - Link to open the chart
    - Chart ID (hidden in config)
    - Title
    - Slug
    - Tags
    - Views (365d)
    - Coviews
    - Coviews_after
    - Coviews_before
    - Score
    - Rank
    """
    # Sort by the existing `score` column, descending
    df = df.sort_values("score", ascending=False).head(n).copy()  # type: ignore
    df["rank"] = range(1, len(df) + 1)

    # Create a clickable link
    df["link"] = df["slug"].apply(lambda x: OWID_ENV.chart_site(x))

    final_cols = [
        "link",
        "chart_id",
        "title",
        "slug",
        "tags",
        "views_365d",
        "coviews",
        "coviews_after",
        "coviews_before",
        "score",
        "rank",
    ]
    df = df[final_cols]

    # Drop optional cols
    df = df.drop(columns=drop_cols)

    # Link column config
    column_config = {
        "link": st.column_config.LinkColumn("Open", display_text="Open"),
        "chart_id": None,  # hide column name
        "slug": None,  # hide column name
    }

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )


def add_coviews_to_charts(charts: List[data.Chart], chosen_chart: data.Chart, coviews: pd.Series) -> List[data.Chart]:
    """
    For the chosen chart, fetch its coview info from a coview Series and attach
    to each chart object as `chart.coviews`.
    """
    try:
        chosen_chart_coviews = coviews.loc[chosen_chart.slug].to_dict()
    except KeyError:
        chosen_chart_coviews = {}

    for c in charts:
        c.coviews = chosen_chart_coviews.get(c.slug, 0)

    return charts


########################################################################################################################
# DATA & MODEL
########################################################################################################################

charts = get_charts()
coviews = get_coviews()

scoring_model = get_and_fit_model(charts)
# Ensure `charts` property is set on the model, especially if loaded from cache
scoring_model.charts = charts

# Create a map for quick lookups by chart_id (optional usage)
chart_map = {chart.chart_id: chart for chart in charts}

# Pick top 100 charts by pageviews
top_100_charts = sorted(charts, key=lambda x: x.views_365d, reverse=True)[:100]  # type: ignore

########################################################################################################################
# SIDEBAR / SEARCH
########################################################################################################################

st.title(":material/search: Similar charts")

col1, col2 = st.columns(2)
with col2:
    st_multiselect_wider()
    with st_horizontal():
        random_chart = st.button("Random chart", help="Pick a random chart, weighted by views.")
        random_100_chart = st.button("Random top 100 chart", help="Pick a random chart from the top 100 charts.")

    # If "Random chart" or no slug is provided, choose a random chart
    if random_chart or not st.query_params.get("slug"):
        weights = np.array([c.views_365d for c in charts])
        weights = np.nan_to_num(weights, nan=0)
        chart = random.choices(charts, weights=weights, k=1)[0]  # type: ignore
        st.session_state["slug"] = chart.slug
    elif random_100_chart:
        chart_slug = random.sample(top_100_charts, 1)[0].slug
        st.session_state["slug"] = chart_slug

    # Main selectbox for charts
    slug = url_persist(st.selectbox)(
        "Select a chart",
        key="slug",
        options=[c.slug for c in charts],
    )

    # Advanced options
    st.session_state.sim_charts_expander_advanced_options = st.session_state.get(
        "sim_charts_expander_advanced_options", False
    )
    with st.expander("Advanced options", expanded=st.session_state.sim_charts_expander_advanced_options):
        # Number of recommendations to show
        url_persist(st.slider)(
            "# of recommendations",
            key="nr_recommendations",
            min_value=5,
            max_value=20,
            value=6,
            step=1,
            format="%.0f",
        )
        nr_recommendations = st.session_state["nr_recommendations"]

# Find the chosen chart
chosen_chart = next((c for c in charts if c.slug == slug or str(c.chart_id) == slug), None)
if not chosen_chart:
    st.error(f"Chart with slug `{slug}` not found.")
    st.stop()

# Attach coviews to each chart object
charts = add_coviews_to_charts(charts, chosen_chart, coviews)

# Compute similarity components for all charts
sim_components = scoring_model.similarity_components(chosen_chart)

# Convert charts to a DataFrame
charts_df = pd.DataFrame([c.to_dict() for c in charts]).set_index("chart_id")

# Join charts with similarity scores
charts_df = charts_df.join(sim_components)

# Exclude the chosen chart itself
charts_df = charts_df.loc[charts_df.index != chosen_chart.chart_id]

# Join directional coviews
dir_cov = get_directional_coviews()
charts_df["coviews_after"] = (
    dir_cov[dir_cov.slug1 == chosen_chart.slug]
    .set_index("slug2")["sessions_coviewed"]
    .reindex(charts_df["slug"])
    .values
)
charts_df["coviews_before"] = (
    dir_cov[dir_cov.slug2 == chosen_chart.slug]
    .set_index("slug1")["sessions_coviewed"]
    .reindex(charts_df["slug"])
    .values
)

########################################################################################################################
# DISPLAY
########################################################################################################################

with col1:
    st_chart_info(chosen_chart, show_coviews=False)


def show_section_others_viewed(df: pd.DataFrame, n: int) -> None:
    st.markdown("---")
    st.header("Others also viewed")
    st.markdown(
        "These charts have the **highest number of coviews** (undirected) with the current chart. "
        "For completeness, we also split total coviews into coviews_before the selected chart and coviews_after."
    )

    df = df.copy()
    df["score"] = df["coviews"]
    st_related_charts_table(df, n=n, drop_cols=["score"])


def show_section_related_charts(df: pd.DataFrame, n: int) -> None:
    st.markdown("---")
    st.header("Related charts by title/subtitle")
    st.markdown("This section shows charts with **high semantic similarity** in their titles and subtitles.")

    df = df.copy()
    df["score"] = (df["title_score"] + df["subtitle_score"]) * 0.5
    st_related_charts_table(df, n=n)


def show_section_other_providers(df: pd.DataFrame, n: int) -> None:
    st.markdown("---")
    st.header("Other providers")
    st.markdown(
        "Identifying other providers is a hard problem. Here we show charts with **very similar titles**."
        "More precise identification would require GPT or deeper data relationships."
    )

    df = df.copy()
    # Filter to only extremely close title matches
    df = df[df["title_score"] > 0.985]
    df["score"] = df["title_score"]
    st_related_charts_table(df, n=n)


def show_section_explore_included_data(df: pd.DataFrame, n: int) -> None:
    st.markdown("---")
    st.header("Explore charts that include this data")
    st.markdown("These charts **share at least one indicator** (the same data) with the current chart.")

    df = df.copy()
    df = df[df["share_indicator"] == 1]
    df["score"] = df["share_indicator"]
    st_related_charts_table(df, n=n)


# Render the different sections
reset_df = charts_df.reset_index()
show_section_others_viewed(reset_df, nr_recommendations)
show_section_related_charts(reset_df, nr_recommendations)
show_section_other_providers(reset_df, nr_recommendations)
show_section_explore_included_data(reset_df, nr_recommendations)
