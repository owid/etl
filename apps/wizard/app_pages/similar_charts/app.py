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

# Initialize log.
log = get_logger()

# Database engine.
engine = get_engine()

# Get reviewer's name.
reviewer = get_grapher_user().fullName

# PAGE CONFIG
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
    with st.spinner("Loading charts..."):
        df = data.get_raw_charts()

        if len(df) == 0:
            raise ValueError("No charts found in the database.")

        charts = df.to_dict(orient="records")

    ret = []
    for c in charts:
        c["tags"] = c["tags"].split(";") if c["tags"] else []
        ret.append(data.Chart(**c))  # type: ignore

    return ret


@log_time
@st.cache_data(show_spinner=False)
def get_coviews() -> pd.Series:
    # Load coviews for all charts for the past 365 days.
    with st.spinner("Loading coviews..."):
        return data.get_coviews_sessions(after_date=str(dt.date.today() - dt.timedelta(days=365)), min_sessions=3)


@log_time
@st.cache_data(show_spinner=False)
def get_directional_coviews() -> pd.DataFrame:
    df = pd.read_feather(paths.BASE_DIR / "apps/wizard/app_pages/similar_charts/playground_coviews.feather")
    return df


def st_chart_info(chart: data.Chart, show_coviews=True) -> None:
    """Displays general info about a single chart."""
    chart_url = OWID_ENV.chart_site(chart.slug)
    # title = f"#### [{chart.title}]({chart_url})"
    title = f"[{chart.title}]({chart_url})"
    if chart.gpt_reason:
        title += " 🤖"
    st.subheader(title, anchor=chart.slug)
    st.markdown(f"Slug: {chart.slug}")
    st.markdown(f"Subtitle: {chart.subtitle}")
    st.markdown(f"Tags: **{', '.join(chart.tags)}**")
    st.markdown(f"Pageviews: **{chart.views_365d}**")
    if show_coviews:
        st.markdown(f"Coviews: **{chart.coviews}**")


@log_time
@st.cache_data(
    show_spinner=False,
    max_entries=1,
    hash_funcs={list[data.Chart]: lambda charts: len(charts)},
)
def get_and_fit_model(charts: list[data.Chart]) -> scoring.ScoringModel:
    with st.spinner("Loading model..."):
        scoring_model = scoring.ScoringModel(emb.get_model())
    with st.spinner("Fitting model..."):
        scoring_model.fit(charts)
    return scoring_model


########################################################################################################################
# NEW COMPONENTS
########################################################################################################################


def st_related_charts_table(df: pd.DataFrame, n=6) -> None:
    # 1) Convert the list of RelatedChart objects to a DataFrame

    # Sort by preferred score
    df = df.sort_values(["score"], ascending=False).iloc[:n]
    df["rank"] = range(1, len(df) + 1)

    # 6) Create a new column "link"
    df["link"] = df["slug"].apply(lambda x: OWID_ENV.chart_site(x))

    # 7) Build the final column order
    final_cols = [
        "link",
        "chart_id",
        "slug",
        "title",
        "tags",
        "views_365d",
        "coviews",
        "coviews_AB",
        "coviews_BA",
        "score",
        "rank",
    ]
    df = df[final_cols]

    # 8) Configure columns for st.dataframe
    column_config = {
        "link": st.column_config.LinkColumn(
            "Open",
            display_text="Open",
        ),
        "chart_id": None,
    }

    # 9) Show the result using st.data_editor
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )


def add_coviews_to_charts(charts: List[data.Chart], chosen_chart: data.Chart, coviews: pd.Series) -> List[data.Chart]:
    try:
        chosen_chart_coviews = coviews.loc[chosen_chart.slug].to_dict()
    except KeyError:
        chosen_chart_coviews = {}

    for c in charts:
        c.coviews = chosen_chart_coviews.get(c.slug, 0)

    return charts


########################################################################################################################
# FETCH DATA & MODEL
########################################################################################################################

charts = get_charts()
coviews = get_coviews()

scoring_model = get_and_fit_model(charts)
# Re-set charts if the model comes from cache
scoring_model.charts = charts


# Build a chart map for quick lookups by chart_id
chart_map = {chart.chart_id: chart for chart in charts}

# Pick top 100 charts by pageviews.
top_100_charts: list[data.Chart] = sorted(charts, key=lambda x: x.views_365d, reverse=True)[:100]  # type: ignore

########################################################################################################################
# RENDER
########################################################################################################################

st.title(":material/search: Similar charts")

col1, col2 = st.columns(2)
with col2:
    st_multiselect_wider()
    with st_horizontal():
        random_chart = st.button("Random chart", help="Get a random chart.")
        random_100_chart = st.button("Random top 100 chart", help="Get a random chart from the top 100 charts.")

    # Random chart was pressed or no search text
    if random_chart or not st.query_params.get("slug"):
        # weighted by views
        weights = np.array([c.views_365d for c in charts])
        weights = np.nan_to_num(weights, nan=0)
        chart = random.choices(charts, weights=weights, k=1)[0]  # type: ignore
        # non-weighted sample
        # chart = random.sample(charts, 1)[0]
        st.session_state["slug"] = chart.slug
    elif random_100_chart:
        chart_slug = random.sample(top_100_charts, 1)[0].slug
        st.session_state["slug"] = chart_slug

    # Dropdown select for chart.
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
        # Regularization for coviews
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

# Find a chart
chosen_chart = next(
    (chart for chart in charts if chart.slug == slug or str(chart.chart_id) == slug),
    None,
)
if not chosen_chart:
    st.error(f"Chart with slug {slug} not found.")
    st.stop()

# Add coviews
charts = add_coviews_to_charts(charts, chosen_chart, coviews)

# Compute various scores for all charts
sim_components = scoring_model.similarity_components(chosen_chart)

# Create dataframe from charts
charts_df = pd.DataFrame([chart.to_dict() for chart in charts]).set_index("chart_id")

# Join charts with scores
charts_df = charts_df.join(sim_components)

# Exclude chosen chart
charts_df = charts_df[charts_df.index != chosen_chart.chart_id]

# Join directional coviews
dir_cov = get_directional_coviews()
charts_df["coviews_AB"] = (
    dir_cov[dir_cov.slug1 == chosen_chart.slug].set_index("slug2")["sessions_coviewed"].reindex(charts_df.slug).values
)
charts_df["coviews_BA"] = (
    dir_cov[dir_cov.slug2 == chosen_chart.slug].set_index("slug1")["sessions_coviewed"].reindex(charts_df.slug).values
)

# Display chosen chart
with col1:
    st_chart_info(chosen_chart, show_coviews=False)


class OthersViewedBlock:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def display(self):
        st.markdown("---")
        st.header("Other people also viewed")
        st.markdown("""
        Choose charts with the most coviews (undirected). Directed coviews are also shown in the table.
        """)

        df = self.df
        df["score"] = df["coviews"]

        st_related_charts_table(df, n=nr_recommendations)


class OtherProvidersBlock:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def display(self):
        st.markdown("---")
        st.header("Other providers")
        st.markdown("""
        Choose charts with extremely similar titles. Choosing different providers of the same data is harder than expected and would need either
        GPT or relation graph with providers.
        """)

        df = self.df
        df["score"] = df["title_score"]

        # Only keep very close matches
        df = df[df["score"] > 0.985]

        st_related_charts_table(df, n=nr_recommendations)


class RelatedChartsBlock:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def display(self):
        st.markdown("---")
        st.header("Related charts")
        st.markdown("Choose charts with high semantic similarity of title and subtitle")

        df = self.df
        df["score"] = (df["title_score"] + df["subtitle_score"]) * 0.5

        st_related_charts_table(df, n=nr_recommendations)


class ExploreChartsIncludeDataBlock:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def display(self):
        st.markdown("---")
        st.header("Explore charts that include this data")
        st.markdown("Show charts sharing an indicator (what we currently show on data page)")

        df = self.df
        df = df[df["share_indicator"] == 1]
        df["score"] = df["share_indicator"]

        st_related_charts_table(df, n=nr_recommendations)


# Blocks
OthersViewedBlock(charts_df.reset_index().copy()).display()
RelatedChartsBlock(charts_df.reset_index().copy()).display()
OtherProvidersBlock(charts_df.reset_index().copy()).display()
ExploreChartsIncludeDataBlock(charts_df.reset_index().copy()).display()

# PROFILER.stop()
