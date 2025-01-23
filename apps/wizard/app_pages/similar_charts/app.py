import datetime as dt
import random
from typing import List, get_args

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.similar_charts import data, scoring
from apps.wizard.utils import embeddings as emb
from apps.wizard.utils import start_profiler
from apps.wizard.utils.cached import get_grapher_user
from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, url_persist
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.git_helpers import log_time
from etl.grapher import model as gm

PROFILER = start_profiler()

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

DISPLAY_STATE_OPTIONS = {
    "good": {
        "label": "Good",
        "color": "green",
        "icon": "✅",
    },
    "bad": {
        "label": "Bad",
        "color": "red",
        "icon": "❌",
    },
    "neutral": {
        "label": "Neutral",
        "color": "gray",
        "icon": "⏳",
    },
}

CHART_LABELS = get_args(gm.RELATED_CHART_LABEL)


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


def st_chart_scores(chart: data.Chart, sim_components: pd.DataFrame) -> None:
    """Displays scoring info (score, breakdown table) for a single chart."""
    st.markdown(f"#### Score: {chart.similarity:.0%}")
    st.table(sim_components.loc[chart.chart_id].to_frame("score").style.format("{:.0%}"))
    if chart.gpt_reason:
        st.markdown(f"**GPT Diversity Reason**:\n{chart.gpt_reason}")


def split_input_string(input_string: str) -> tuple[str, list[str], list[str]]:
    """Break input string into query, includes and excludes."""
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


class RelatedChartDisplayer:
    """
    Encapsulates the logic for displaying and labeling a related chart,
    including any database updates and UI feedback.
    """

    def __init__(self, engine, chosen_chart: data.Chart, sim_components: pd.DataFrame):
        self.engine = engine
        self.chosen_chart = chosen_chart
        self.sim_components = sim_components

    def display(
        self,
        chart: data.Chart,
        label: gm.RELATED_CHART_LABEL = "neutral",
    ) -> None:
        """
        Renders the chart block (info, scores, and label radio).
        Also hooks up the callback for label changes.
        """
        with st.container():
            col1, col2 = st.columns(2)
            with col1:
                st_chart_info(chart)
                st.radio(
                    label="**Review Related Chart**",
                    key=f"label-{chart.chart_id}",
                    options=CHART_LABELS,
                    index=CHART_LABELS.index(label),
                    horizontal=True,
                    format_func=lambda x: f":{DISPLAY_STATE_OPTIONS[x]['color']}-background[{DISPLAY_STATE_OPTIONS[x]['label']}]",
                    on_change=self._push_status,
                    kwargs={"chart": chart},
                )
            with col2:
                st_chart_scores(chart, self.sim_components)

    def _push_status(self, chart: data.Chart) -> None:
        """
        Callback: triggered on label change. Saves to the DB and
        shows an appropriate toast.
        """
        label: gm.RELATED_CHART_LABEL = st.session_state[f"label-{chart.chart_id}"]

        with Session(self.engine) as session:
            gm.RelatedChart(
                chartId=self.chosen_chart.chart_id,
                relatedChartId=chart.chart_id,
                label=label,
                reviewer=reviewer,
            ).upsert(session)
            session.commit()

        # Notify user
        with st.spinner():
            match label:
                case "good":
                    st.toast(":green[Recommendation labeled as **good**]", icon="✅")
                case "bad":
                    st.toast(":red[Recommendation labeled as **bad**]", icon="❌")
                case "neutral":
                    st.toast("**Resetting** recommendation to neutral", icon=":material/restart_alt:")


def st_related_charts_table(
    related_charts: list[gm.RelatedChart], chart_map: dict[int, data.Chart], chosen_chart: data.Chart
) -> None:
    """
    Shows a "matrix" of reviews in a pivoted table using st.dataframe:
      - Row per related chart
      - Columns for slug, title, views_365d, link, and one column per reviewer (icon)
      - Hides chart_id
    """
    if not related_charts:
        st.info("No related charts have been selected yet.")
        return

    # 1) Convert the list of RelatedChart objects to a DataFrame
    rows = []
    for rc in related_charts:
        c = chart_map.get(rc.relatedChartId)
        if not c:
            # Skip if missing
            continue

        # rev = rc.reviewer
        # if not rev.startswith("🤖"):
        #     reviewer = "👤" + " " + reviewer

        rows.append(
            {
                "chart_id": c.chart_id,
                "slug": c.slug,
                "title": c.title,
                "views_365d": c.views_365d,
                "coviews": c.coviews,
                "score": c.similarity,
                "reviewer": rc.reviewer,
                "label": rc.label,
            }
        )
    df = pd.DataFrame(rows)

    # Exclude neutral reviews
    df = df[df["label"] != "neutral"]

    # 2) Pivot so that each reviewer is a column, with the label as the cell value
    pivot_df = df.pivot(
        index=["chart_id", "slug", "title", "views_365d", "coviews", "score"], columns="reviewer", values="label"
    ).fillna("neutral")

    reviewer_cols = list(pivot_df.columns)

    # Move Coviews and Jaccard to the beginning
    front_cols = [c for c in ["🤖 Jaccard", "🤖 Coviews", "🤖 GPT", "🤖 Score", "🤖 Current"] if c in reviewer_cols]
    reviewer_cols = front_cols + [col for col in reviewer_cols if col not in front_cols]

    if reviewer in reviewer_cols:
        pivot_df["favorite"] = pivot_df[reviewer] == "good"
        pivot_df["dislike"] = pivot_df[reviewer] == "bad"
        del pivot_df[reviewer]
        reviewer_cols.remove(reviewer)
    else:
        pivot_df["favorite"] = False
        pivot_df["dislike"] = False

    print(pivot_df[["favorite", "dislike"]])

    # 3) Map each label (good/bad/neutral) to an icon
    def label_to_icon(label: str) -> str:
        if label == "neutral":
            return ""
        else:
            return DISPLAY_STATE_OPTIONS.get(label, DISPLAY_STATE_OPTIONS["neutral"])["icon"]

    pivot_df[reviewer_cols] = pivot_df[reviewer_cols].applymap(label_to_icon)

    # 4) Flatten the multi-index so 'chart_id', 'slug', etc. become columns
    pivot_df.reset_index(inplace=True)

    # 6) Create a new column "link"
    pivot_df["link"] = pivot_df["slug"].apply(lambda x: OWID_ENV.chart_site(x))
    # TODO: jump to anchor
    # pivot_df["link"] = pivot_df["slug"].apply(lambda x: f"#{x}")

    # TODO: DRY this with scores
    pivot_df["jaccard"] = pivot_df.coviews / (pivot_df.views_365d + chosen_chart.views_365d - pivot_df.coviews)

    # 7) Build the final column order
    final_cols = (
        ["link", "chart_id", "slug", "title", "views_365d", "coviews"] + reviewer_cols + ["favorite", "dislike"]
    )

    # Sort by preferred score
    pivot_df = pivot_df.sort_values(["jaccard"], ascending=False)[final_cols]

    # Add reason, currently disabled
    pivot_df["reason"] = ""

    # 8) Configure columns for st.dataframe
    column_config = {
        # The link column becomes a clickable link
        "link": st.column_config.LinkColumn(
            "Open",
            # display_text="Jump to detail",
            display_text="Open",
        ),
        "favorite": st.column_config.CheckboxColumn(
            "Like?",
            help="Check if you like the related chart",
            # default=False,
        ),
        "dislike": st.column_config.CheckboxColumn(
            "Dislike?",
            help="Check if you dislike the related chart",
            # default=False,
        ),
        # "reason": st.column_config.TextColumn(
        #     "Reason",
        #     help="Add reason for liking / disliking the chart",
        # ),
        "reason": None,
        "🤖 Jaccard": st.column_config.Column(help="Jaccard similarity from coviews"),
        "🤖 Score": st.column_config.Column(help="Complex score without diversity"),
        "🤖 GPT": st.column_config.Column(help="Complex score with diversity by GPT"),
        "🤖 Coviews": st.column_config.Column(help="Most coviews"),
        "🤖 Current": st.column_config.Column(help="What we currently show"),
        "chart_id": None,
    }
    # You could also configure text columns or numeric columns (like "views_365d").
    # styled_df = pivot_df.style.format("{:.0%}", subset=["score"]).format("{:.2%}", subset=["jaccard"])
    styled_df = pivot_df

    # Disable all columns except "favorite" and "dislike"
    disabled_cols = [col for col in pivot_df.columns if col not in ("favorite", "dislike", "reason")]

    # 9) Show the result using st.data_editor
    updated_df = st.data_editor(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
        disabled=disabled_cols,
    )

    print("updated!")
    print(updated_df[["favorite", "dislike"]])

    update_likes_and_dislikes(pivot_df, updated_df, chosen_chart)


def update_likes_and_dislikes(orig_df: pd.DataFrame, updated_df: pd.DataFrame, chosen_chart: data.Chart) -> None:
    old_favorites = set(orig_df[orig_df["favorite"]].chart_id)
    old_dislikes = set(orig_df[orig_df["dislike"]].chart_id)

    new_favorites = set(updated_df[updated_df["favorite"]].chart_id)
    new_dislikes = set(updated_df[updated_df["dislike"]].chart_id)

    print("Old favorites", old_favorites)
    print("New favorites", new_favorites)
    print("Old dislikes", old_dislikes)
    print("New dislikes", new_dislikes)

    # Save favorites
    with Session(engine) as session:
        for chart_id in new_favorites - old_favorites:
            gm.RelatedChart(
                chartId=chosen_chart.chart_id,
                relatedChartId=chart_id,
                label="good",
                reviewer=reviewer,
            ).upsert(session)

        for chart_id in new_dislikes - old_dislikes:
            gm.RelatedChart(
                chartId=chosen_chart.chart_id,
                relatedChartId=chart_id,
                label="bad",
                reviewer=reviewer,
            ).upsert(session)

        for chart_id in (old_dislikes | old_favorites) - new_favorites - new_dislikes:
            # Delete the related chart
            session.query(gm.RelatedChart).filter_by(
                chartId=chosen_chart.chart_id,
                relatedChartId=chart_id,
                reviewer=reviewer,
            ).delete()

        session.commit()

    # Save dislikes
    with Session(engine) as session:
        for chart_id in new_dislikes - old_dislikes:
            gm.RelatedChart(
                chartId=chosen_chart.chart_id,
                relatedChartId=chart_id,
                label="bad",
                reviewer=reviewer,
            ).upsert(session)

        for chart_id in old_dislikes - new_dislikes:
            session.query(gm.RelatedChart).filter_by(
                chartId=chosen_chart.chart_id,
                relatedChartId=chart_id,
                reviewer=reviewer,
            ).delete()

        session.commit()


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

        # Filter indicators
        diversity_gpt = url_persist(st.checkbox)(
            "Diversity with GPT",
            key="diversity_gpt",
            value=True,
            help="Use GPT to select 5 most diverse charts from the top 30 similar charts.",
        )

    # Random chart was pressed or no search text
    if random_chart or not st.query_params.get("slug"):
        # weighted by views
        chart = random.choices(charts, weights=[c.views_365d for c in charts], k=1)[0]  # type: ignore
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
        # Add text area for system prompt
        system_prompt = url_persist(st.text_area)(
            "GPT prompt for selecting diverse results",
            key="gpt_system_prompt",
            value=scoring.DEFAULT_SYSTEM_PROMPT,
            height=150,
        )

        # Regularization for coviews
        url_persist(st.slider)(
            "Coviews regularization",
            key="coviews_regularization",
            min_value=0.0,
            max_value=0.001,
            value=scoring.DEFAULT_COVIEWS_REGULARIZATION,
            step=0.0001,
            format="%.3f",
            help="Penalize coviews score by subtracting this value times pageviews.",
        )
        scoring_model.coviews_regularization = st.session_state["coviews_regularization"]

        for score_name in ["title", "subtitle", "tags", "share_indicator", "pageviews_score", "coviews_score"]:
            key = f"w_{score_name}"
            url_persist(st.slider)(
                f"Weight for {score_name} score",
                key=key,
                min_value=1e-9,
                max_value=1.0,
                value=scoring.DEFAULT_WEIGHTS[score_name],
            )
            scoring_model.weights[score_name] = st.session_state[key]

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

# Load "official" related charts from DB
with Session(engine) as session:
    related_charts_db = gm.RelatedChart.load(session, chart_id=chosen_chart.chart_id)

# Compute similarity for all charts
sim_dict = scoring_model.similarity(chosen_chart)
sim_components = scoring_model.similarity_components(chosen_chart)

# Assign similarity
for c in charts:
    c.similarity = sim_dict[c.chart_id]

# Sort by similarity
sorted_charts = sorted(charts, key=lambda x: x.similarity, reverse=True)  # type: ignore


def get_reviews(scores: pd.Series, reviewer: str, k=5) -> list[gm.RelatedChart]:
    # Add reviews to top charts by similarity
    related_charts = []

    for chart_id, _ in scores.sort_values(ascending=False).items():
        c = chart_map[chart_id]

        if c.chart_id == chosen_chart.chart_id:  # type: ignore
            continue

        # Add to related charts
        related_charts.append(
            gm.RelatedChart(
                chartId=chosen_chart.chart_id,  # type: ignore
                relatedChartId=c.chart_id,
                label="good",
                reviewer=reviewer,
            )
        )

        if len(related_charts) >= k:
            return related_charts

    return related_charts


score = pd.Series(sim_dict)
related_charts_db += get_reviews(score, reviewer="🤖 Score")


# Possibly re-rank with GPT for diversity
if diversity_gpt:
    with st.spinner("Diversifying chart results..."):
        slugs_to_reasons = scoring.gpt_diverse_charts(chosen_chart, sorted_charts, system_prompt=system_prompt)

    gpt_candidates = []
    for c in sorted_charts:
        if c.slug in slugs_to_reasons:
            c.gpt_reason = slugs_to_reasons[c.slug]
            gpt_candidates.append(c.chart_id)

    related_charts_db += get_reviews(score.loc[gpt_candidates], reviewer="🤖 GPT")

scores = sim_components["coviews_score"]
related_charts_db += get_reviews(scores, reviewer="🤖 Coviews")

scores = sim_components["jaccard_score"]
related_charts_db += get_reviews(scores, reviewer="🤖 Jaccard")

scores = sim_components.loc[sim_components["share_indicator"] > 0, "share_indicator"]
related_charts_db += get_reviews(scores, reviewer="🤖 Current")


# Display chosen chart
with col1:
    st_chart_info(chosen_chart, show_coviews=False)

# Divider
st.markdown("---")
st.header("Reviewed Related Charts")
st_related_charts_table(related_charts_db, chart_map, chosen_chart)

# Divider
st.markdown("---")
st.header("Recommended Related Charts")

# Create our new chart display component
displayer = RelatedChartDisplayer(engine, chosen_chart, sim_components)

# Use pagination
pagination = Pagination(
    items=sorted_charts[:100],
    items_per_page=ITEMS_PER_PAGE,
    pagination_key=f"pagination-di-search-{chosen_chart.slug}",
)
if len(sorted_charts) > ITEMS_PER_PAGE:
    pagination.show_controls(mode="bar")

# Display only the current page
for item in pagination.get_page_items():
    if item.slug == chosen_chart.slug:
        continue

    # Check if we have a DB label for the related chart from us
    labels = [r.label for r in related_charts_db if r.relatedChartId == item.chart_id and r.reviewer == reviewer]
    label = labels[0] if labels else "neutral"

    # Use the new component to display
    displayer.display(chart=item, label=label)  # type: ignore

PROFILER.stop()
