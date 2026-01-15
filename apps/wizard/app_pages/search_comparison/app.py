"""Compare Algolia keyword search with semantic (AI) search side-by-side."""

import time

import requests
import streamlit as st

from apps.wizard.app_pages.search_comparison.random_queries import get_random_search_query
from apps.wizard.utils.components import st_horizontal, st_title_with_expert, url_persist
from etl.db import read_sql

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Wizard: Search Comparison",
    page_icon="🪄",
    layout="wide",
)

# API base URL (local dev server)
API_BASE = "http://127.0.0.1:8788"


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


def display_hit(hit: dict, index: int, search_type: str, other_rank: int | None):
    """Display a single search hit."""
    title = hit.get("title", "Untitled")
    slug = hit.get("slug", "")
    subtitle = hit.get("subtitle", "")
    url = hit.get("url", f"{API_BASE}/grapher/{slug}")

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
        # Title with rank change as suffix
        st.markdown(f"**{index}.** [{title}]({url}) {rank_indicator}")

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
    return {hit.get("slug"): i + 1 for i, hit in enumerate(hits)}


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

    # Fetch pageviews for Algolia results (they don't include views)
    algolia_slugs = tuple(h.get("slug") for h in algolia_hits if h.get("slug"))
    if algolia_slugs:
        pageviews = get_pageviews_for_slugs(algolia_slugs)
        # Enrich Algolia hits with pageview data
        for hit in algolia_hits:
            slug = hit.get("slug")
            if slug and slug in pageviews:
                hit["views_365d"] = pageviews[slug]

    semantic_slug_to_rank = build_slug_to_rank(semantic_hits)
    algolia_slug_to_rank = build_slug_to_rank(algolia_hits)

    # Headers - Algolia on left, Semantic on right
    col_algolia, col_semantic = st.columns(2)
    with col_algolia:
        st.subheader("🔤 Algolia (Keyword)")
        if "error" not in algolia_data:
            total = algolia_data.get("nbHits", len(algolia_hits))
            st.info(f"**{total:,}** total results ({algolia_time:.2f}s)")
    with col_semantic:
        st.subheader("🧠 Semantic (AI)")
        if "error" not in semantic_data:
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


main()
