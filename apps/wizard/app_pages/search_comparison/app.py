"""Compare Algolia keyword search with semantic (AI) search side-by-side."""

import json
import time
from typing import Generator

import requests
import streamlit as st

from apps.wizard.app_pages.search_comparison.random_queries import get_random_search_query
from apps.wizard.utils.components import st_horizontal, st_title_with_expert, url_persist
from etl.config import OWID_ENV, STAGING
from etl.db import read_sql

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Wizard: Search Comparison",
    page_icon="🪄",
    layout="wide",
)

# API base URL - localhost:8788 for local dev, OWID_ENV.site for staging/prod
API_BASE = OWID_ENV.site if STAGING else "http://localhost:8788"


@st.cache_data(ttl=3600)
def get_pageviews_for_slugs(slugs: tuple[str, ...]) -> dict[str, int]:
    """Fetch pageviews from MySQL for given slugs."""
    if not slugs:
        return {}

    # Build SQL query with quoted slugs
    quoted_slugs = ", ".join(f"'{s}'" for s in slugs if s)
    query = f"""
    SELECT
        SUBSTRING_INDEX(url, '/', -1) as slug,
        views_365d
    FROM analytics_pageviews
    WHERE url LIKE '%%/grapher/%%'
      AND SUBSTRING_INDEX(url, '/', -1) IN ({quoted_slugs})
    """
    df = read_sql(query)
    return dict(zip(df["slug"], df["views_365d"]))


@st.cache_data(ttl=3600)
def get_fm_ranks_for_slugs(slugs: tuple[str, ...]) -> dict[str, int]:
    """Fetch featured metric ranks from MySQL for given slugs.

    Returns the minimum (best) rank for each slug since a chart can be
    featured in multiple topics.
    """
    if not slugs:
        return {}

    # Build SQL query with quoted slugs
    quoted_slugs = ", ".join(f"'{s}'" for s in slugs if s)
    query = f"""
    SELECT
        SUBSTRING_INDEX(url, '/', -1) as slug,
        MIN(ranking) as fm_rank
    FROM featured_metrics
    WHERE incomeGroup = 'default'
      AND url LIKE '%%/grapher/%%'
      AND SUBSTRING_INDEX(url, '/', -1) IN ({quoted_slugs})
    GROUP BY slug
    """
    df = read_sql(query)
    return dict(zip(df["slug"], df["fm_rank"]))


def fetch_semantic_search(query: str, hits_per_page: int) -> tuple[dict, float]:
    """Fetch results from semantic (AI) search API."""
    start = time.time()
    try:
        response = requests.get(
            f"{API_BASE}/api/ai-search/charts",
            params={"q": query, "hitsPerPage": hits_per_page},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "hits": []}, time.time() - start


def fetch_algolia_search(query: str, hits_per_page: int) -> tuple[dict, float]:
    """Fetch results from Algolia keyword search API."""
    start = time.time()
    try:
        response = requests.get(
            f"{API_BASE}/api/search",
            params={"q": query, "hitsPerPage": hits_per_page},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "hits": []}, time.time() - start


def fetch_answer_stream(query: str) -> Generator[str, None, None]:
    """Stream answer from AI search SSE endpoint."""
    try:
        response = requests.get(
            f"{API_BASE}/api/ai-search/answer",
            params={"q": query},
            stream=True,
            timeout=60,
        )
        response.raise_for_status()
        # Force UTF-8 encoding for proper Unicode handling (e.g., CO₂)
        response.encoding = "utf-8"

        for line in response.iter_lines(decode_unicode=True):
            if line and line.startswith("data:"):
                data = line[5:].strip()
                if data:
                    chunk = json.loads(data)
                    yield chunk.get("text", "")
    except requests.RequestException as e:
        yield f"\n\n*Error: {e}*"


def get_rank_change_indicator(current_rank: int, other_rank: int | None) -> str:
    """Get an indicator showing rank change between search results."""
    if other_rank is None:
        return ":violet[**new**]"

    diff = other_rank - current_rank
    if diff > 0:
        # Moved up (was lower/worse rank in other, now higher/better)
        return f":green[**↑{diff}**]"
    elif diff < 0:
        # Moved down (was higher/better rank in other, now lower/worse)
        return f":red[**↓{abs(diff)}**]"
    else:
        # Same rank
        return ":gray[=]"


def is_explorer(hit: dict) -> bool:
    """Check if the hit is an explorer (not a chart)."""
    url = hit.get("url", "")
    return "/explorers/" in url


def display_hit(hit: dict, index: int, search_type: str, other_rank: int | None):
    """Display a single search hit."""
    title = hit.get("title", "Untitled")
    subtitle = hit.get("subtitle", "")
    # Use URL from API response, or fallback to current environment's site
    url = hit.get("url")

    # Check if this is an explorer
    explorer = is_explorer(hit)
    type_icon = "🧭" if explorer else "📊"

    # Get score based on search type
    if search_type == "semantic":
        score = hit.get("aiSearchScore", hit.get("score", 0))
    else:
        score = None

    # View stats
    views_365d = hit.get("views_365d", 0)
    fm_rank = hit.get("fmRank")

    # Rank change indicator
    rank_indicator = get_rank_change_indicator(index, other_rank)

    with st.container(border=True):
        # Title with type icon and rank change
        st.markdown(f"**{index}.** {type_icon} [{title}]({url}) {rank_indicator}")

        # Subtitle
        if subtitle:
            st.caption(subtitle[:120] + "..." if len(subtitle) > 120 else subtitle)

        # Compact stats line
        stats = []
        if score is not None:
            stats.append(f"Score: **{score:.3f}**")
        if fm_rank is not None:
            stats.append(f"FM Rank: **{fm_rank}**")
        if views_365d:
            stats.append(f"Views: **{views_365d:,}**")
        if stats:
            st.markdown(" · ".join(stats), help="FM Rank = Fusion Model rank, Views = annual pageviews")


def build_slug_to_rank(hits: list[dict]) -> dict[str, int]:
    """Build a mapping from slug to rank (1-indexed)."""
    return {hit["slug"]: i + 1 for i, hit in enumerate(hits) if hit.get("slug")}


def main():
    st_title_with_expert("Search Comparison", icon=":material/compare:")

    st.markdown(
        "Compare **Algolia keyword search** with **semantic (AI) search** side-by-side. "
        "Enter a query to see how results differ between the two approaches."
    )

    # Handle random query selection (must be before widget creation)
    if st.session_state.get("_set_random_query"):
        new_query = st.session_state.pop("_set_random_query")
        # Clear session state for query widget so url_persist picks up new value
        if "query" in st.session_state:
            del st.session_state["query"]
        st.query_params["query"] = new_query
        st.rerun()

    # Query input - wide text input
    col_query, col_results = st.columns([4, 1])
    with col_query:
        query = url_persist(st.text_input)(
            key="query",
            label="Search query",
            placeholder="Enter search query (e.g., gdp, climate change, life expectancy)",
        )
    with col_results:
        hits_per_page = st.selectbox(
            "Results",
            options=[5, 10, 20, 50],
            index=1,
            key="hits_per_page",
        )

    # Quick actions
    with st_horizontal():
        st.markdown("**Try:**")
        if st.button("🎲 Random query", help="Pick a random query from real user searches (weighted by popularity)"):
            st.session_state["_set_random_query"] = get_random_search_query(require_hits=True)
            st.rerun()
        if st.button("🔍 Zero Algolia results", help="Pick a query that returned zero results in Algolia"):
            st.session_state["_set_random_query"] = get_random_search_query(require_hits=False)
            st.rerun()

    if not query:
        st.info("Enter a search query above to compare results.")
        return

    if len(query) < 2:
        st.warning("Please enter at least 2 characters.")
        return

    # Fetch results from both APIs
    with st.spinner("Searching..."):
        semantic_data, semantic_time = fetch_semantic_search(query, hits_per_page)
        algolia_data, algolia_time = fetch_algolia_search(query, hits_per_page)

    # Get hits and build rank mappings
    semantic_hits = semantic_data.get("hits", [])
    algolia_hits = algolia_data.get("hits", [])
    max_hits = max(len(semantic_hits), len(algolia_hits))

    # Fetch pageviews and FM ranks for Algolia results (they don't include these)
    algolia_slugs = tuple(h.get("slug") for h in algolia_hits if h.get("slug"))
    if algolia_slugs:
        pageviews = get_pageviews_for_slugs(algolia_slugs)
        fm_ranks = get_fm_ranks_for_slugs(algolia_slugs)
        # Enrich Algolia hits with pageview and FM rank data
        for hit in algolia_hits:
            slug = hit.get("slug")
            if slug:
                if slug in pageviews:
                    hit["views_365d"] = pageviews[slug]
                if slug in fm_ranks:
                    hit["fmRank"] = fm_ranks[slug]

    semantic_slug_to_rank = build_slug_to_rank(semantic_hits)
    algolia_slug_to_rank = build_slug_to_rank(algolia_hits)

    # Create placeholder for AI answer at the top (will be filled after charts render)
    st.subheader("🤖 AI Answer")
    ai_answer_placeholder = st.empty()

    # Headers - Algolia on left, Semantic on right
    col_algolia, col_semantic = st.columns(2)
    with col_algolia:
        st.subheader("🔤 Algolia (Keyword)")
        if "error" not in algolia_data:
            total = algolia_data.get("nbHits", len(algolia_hits))
            st.info(f"**{total:,}** total results ({algolia_time:.2f}s)")
    with col_semantic:
        st.subheader("🧠 Semantic (AI)")
        if "error" in semantic_data:
            st.error(f"API Error: {semantic_data['error']}")
        else:
            total = semantic_data.get("nbHits", len(semantic_hits))
            st.info(f"**{total:,}** total results ({semantic_time:.2f}s)")

    # Results row by row - Algolia on left, Semantic on right
    for i in range(max_hits):
        col_algolia, col_semantic = st.columns(2)

        with col_algolia:
            if i < len(algolia_hits):
                hit = algolia_hits[i]
                slug = hit.get("slug")
                other_rank = semantic_slug_to_rank.get(slug)
                display_hit(hit, i + 1, "algolia", other_rank)

        with col_semantic:
            if i < len(semantic_hits):
                hit = semantic_hits[i]
                slug = hit.get("slug")
                other_rank = algolia_slug_to_rank.get(slug)
                display_hit(hit, i + 1, "semantic", other_rank)

    # Now stream the AI answer into the placeholder (charts are already rendered)
    # Manually accumulate text since placeholder.write_stream doesn't work correctly
    full_response = ""
    for chunk in fetch_answer_stream(query):
        full_response += str(chunk) if chunk else ""
        ai_answer_placeholder.markdown(full_response + "▌")
    # Final render without cursor
    ai_answer_placeholder.markdown(full_response)


main()
