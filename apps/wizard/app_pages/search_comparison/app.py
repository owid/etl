"""Compare Algolia keyword search with semantic (AI) search side-by-side."""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

import requests
import streamlit as st

from apps.wizard.app_pages.search_comparison.random_queries import get_random_search_query
from apps.wizard.utils.components import st_horizontal, st_title_with_expert, url_persist
from etl.config import ENV_IS_REMOTE, OWID_ENV, STAGING
from etl.db import read_sql

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Wizard: Search Comparison",
    page_icon="🪄",
    layout="wide",
)

# API base URL - OWID_ENV.site for staging/prod, localhost for local dev
API_BASE = "http://localhost:8788" if not ENV_IS_REMOTE and not STAGING else OWID_ENV.site


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


def fetch_semantic_search(
    query: str,
    hits_per_page: int,
    include_explorers: bool = False,
    rerank: bool = False,
    rewrite: bool = False,
    llm_rerank: bool = False,
    llm_model: str | None = None,
) -> tuple[dict, float]:
    """Fetch results from semantic (AI) search API."""
    start = time.time()
    try:
        params = {"q": query, "hitsPerPage": hits_per_page}
        if not include_explorers:
            params["type"] = "chart"
        if rerank:
            params["rerank"] = "true"
        if rewrite:
            params["rewrite"] = "true"
        if llm_rerank:
            params["llmRerank"] = "true"
            if llm_model:
                params["llmModel"] = llm_model
        response = requests.get(
            f"{API_BASE}/api/ai-search/charts",
            params=params,
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


def fetch_topics(query: str, limit: int = 3, mode: str = "semantic") -> tuple[dict, float]:
    """Fetch recommended topics from AI search API.

    Args:
        query: Search query string
        limit: Maximum number of topics to return
        mode: Either "semantic" for vector search or "llm" for LLM-based recommendations
    """
    start = time.time()
    try:
        response = requests.get(
            f"{API_BASE}/api/ai-search/topics",
            params={"q": query, "limit": limit, "mode": mode},
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
    col_query, col_base, col_results = st.columns([4, 1, 1])
    with col_query:
        query = url_persist(st.text_input)(
            key="query",
            label="Search query",
            placeholder="Enter search query (e.g., gdp, climate change, life expectancy)",
        )
    with col_base:
        left_source = url_persist(st.selectbox)(
            key="left_source",
            label="Compare with",
            options=["Algolia (Keyword)", "Semantic (AI)"],
            index=0,
            help="Choose baseline for left column comparison",
        )
    with col_results:
        hits_per_page = st.selectbox(
            "Results",
            options=[5, 10, 20, 50],
            index=1,
            key="hits_per_page",
        )

    # Quick actions and options
    col1, col2 = st.columns([1, 3])

    with col1:
        with st_horizontal():
            st.markdown("**Try:**")
            if st.button(
                "🎲 Random query", help="Pick a random query from real user searches (weighted by popularity)"
            ):
                st.session_state["_set_random_query"] = get_random_search_query(require_hits=True)
                st.rerun()
            if st.button("🔍 Zero Algolia results", help="Pick a query that returned zero results in Algolia"):
                st.session_state["_set_random_query"] = get_random_search_query(require_hits=False)
                st.rerun()

    with col2:
        with st_horizontal():
            enable_ai_answer = url_persist(st.checkbox)(
                key="ai_answer",
                label="AI Answer",
                value=False,
            )
            include_explorers = url_persist(st.checkbox)(
                key="include_explorers",
                label="Include explorers",
                value=False,
            )
            enable_rerank = url_persist(st.checkbox)(
                key="rerank",
                label="Rerank",
                value=False,
                help="Enable reranking with BGE reranker model",
            )
            enable_rewrite = url_persist(st.checkbox)(
                key="rewrite",
                label="Query rewrite",
                value=False,
                help="Enable query rewriting for better retrieval",
            )
            enable_llm_rerank = url_persist(st.checkbox)(
                key="llm_rerank",
                label="LLM rerank",
                value=False,
                help="Enable LLM-based reranking",
            )
            llm_model = url_persist(st.selectbox)(
                key="llm_model",
                label="LLM model",
                options=["small", "large"],
                index=0,
                help="Model size for LLM reranking",
            )
            topic_threshold = st.number_input(
                "Topic threshold",
                min_value=0.0,
                max_value=1.0,
                value=0.65,
                step=0.05,
                help="Minimum relevance score for topic recommendations",
                key="topic_threshold",
            )

    if not query:
        st.info("Enter a search query above to compare results.")
        return

    if len(query) < 2:
        st.warning("Please enter at least 2 characters.")
        return

    # Fetch results from all APIs in parallel
    with st.spinner("Searching..."):
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all tasks in parallel
            right_future = executor.submit(
                fetch_semantic_search,
                query,
                hits_per_page,
                include_explorers,
                enable_rerank,
                enable_rewrite,
                enable_llm_rerank,
                llm_model,
            )
            if left_source == "Algolia (Keyword)":
                left_future = executor.submit(fetch_algolia_search, query, hits_per_page)
                left_search_type = "algolia"
            else:
                left_future = executor.submit(fetch_semantic_search, query, hits_per_page, include_explorers)
                left_search_type = "semantic"
            topics_llm_future = executor.submit(fetch_topics, query, 5, "llm")
            topics_semantic_future = executor.submit(fetch_topics, query, 5, "semantic")

            # Collect results
            right_data, right_time = right_future.result()
            left_data, left_time = left_future.result()
            topics_llm, topics_llm_time = topics_llm_future.result()
            topics_semantic, topics_semantic_time = topics_semantic_future.result()

    # Get hits and build rank mappings
    right_hits = right_data.get("hits", [])
    left_hits = left_data.get("hits", [])
    max_hits = max(len(right_hits), len(left_hits))

    # Fetch pageviews and FM ranks for left results if Algolia (they don't include these)
    if left_search_type == "algolia":
        left_slugs = tuple(h.get("slug") for h in left_hits if h.get("slug"))
        if left_slugs:
            pageviews = get_pageviews_for_slugs(left_slugs)
            fm_ranks = get_fm_ranks_for_slugs(left_slugs)
            # Enrich left hits with pageview and FM rank data
            for hit in left_hits:
                slug = hit.get("slug")
                if slug:
                    if slug in pageviews:
                        hit["views_365d"] = pageviews[slug]
                    if slug in fm_ranks:
                        hit["fmRank"] = fm_ranks[slug]

    right_slug_to_rank = build_slug_to_rank(right_hits)
    left_slug_to_rank = build_slug_to_rank(left_hits)

    # Display recommended topics - both modes for comparison
    st.subheader("📚 Recommended Topics")

    # LLM Topics
    st.markdown("**🤖 LLM Recommendations**")
    if "error" not in topics_llm and topics_llm.get("hits"):
        filtered_llm = [t for t in topics_llm["hits"] if t.get("score", 0) >= topic_threshold]
        if filtered_llm:
            topic_cols = st.columns(min(len(filtered_llm), 5))
            for idx, topic in enumerate(filtered_llm):
                with topic_cols[idx]:
                    with st.container(border=True):
                        score = topic.get("score", 0)
                        st.markdown(f"[**{topic['name']}**]({topic['url']})", help=f"Relevance score: {score:.3f}")
                        st.caption(topic.get("excerpt", "")[:100] + "...")
        else:
            st.info("No LLM topics above threshold")
    else:
        st.error(f"LLM Error: {topics_llm.get('error', 'Unknown error')}")

    # Semantic Topics
    st.markdown("**🔍 Semantic (Vector Search)**")
    if "error" not in topics_semantic and topics_semantic.get("hits"):
        filtered_semantic = [t for t in topics_semantic["hits"] if t.get("score", 0) >= topic_threshold]
        if filtered_semantic:
            topic_cols = st.columns(min(len(filtered_semantic), 5))
            for idx, topic in enumerate(filtered_semantic):
                with topic_cols[idx]:
                    with st.container(border=True):
                        score = topic.get("score", 0)
                        st.markdown(f"[**{topic['name']}**]({topic['url']})", help=f"Relevance score: {score:.3f}")
                        st.caption(topic.get("excerpt", "")[:100] + "...")
        else:
            st.info("No semantic topics above threshold")
    else:
        st.error(f"Semantic Error: {topics_semantic.get('error', 'Unknown error')}")

    # Create placeholder for AI answer at the top (will be filled after charts render)
    if enable_ai_answer:
        st.subheader("🤖 AI Answer")
        ai_answer_placeholder = st.empty()

    # Headers - left column (Algolia or Semantic base), right column (Semantic with options)
    left_header = "🔤 Algolia (Keyword)" if left_search_type == "algolia" else "🧠 Semantic (base)"
    right_header = "🧠 Semantic (AI)"
    if enable_rerank or enable_rewrite or enable_llm_rerank:
        options = []
        if enable_rerank:
            options.append("rerank")
        if enable_rewrite:
            options.append("rewrite")
        if enable_llm_rerank:
            options.append("LLM rerank")
        right_header += f" + {', '.join(options)}"

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader(left_header)
        if "error" not in left_data:
            total = left_data.get("nbHits", len(left_hits))
            st.info(f"**{total:,}** total results ({left_time:.2f}s)")
        else:
            st.error(f"API Error: {left_data['error']}")
    with col_right:
        st.subheader(right_header)
        if "error" in right_data:
            st.error(f"API Error: {right_data['error']}")
        else:
            total = right_data.get("nbHits", len(right_hits))
            st.info(f"**{total:,}** total results ({right_time:.2f}s)")

    # Results row by row
    for i in range(max_hits):
        col_left, col_right = st.columns(2)

        with col_left:
            if i < len(left_hits):
                hit = left_hits[i]
                slug = hit.get("slug")
                other_rank = right_slug_to_rank.get(slug)
                display_hit(hit, i + 1, left_search_type, other_rank)

        with col_right:
            if i < len(right_hits):
                hit = right_hits[i]
                slug = hit.get("slug")
                other_rank = left_slug_to_rank.get(slug)
                display_hit(hit, i + 1, "semantic", other_rank)

    # Now stream the AI answer into the placeholder (charts are already rendered)
    if enable_ai_answer:
        # Manually accumulate text since placeholder.write_stream doesn't work correctly
        full_response = ""
        for chunk in fetch_answer_stream(query):
            full_response += str(chunk) if chunk else ""
            ai_answer_placeholder.markdown(full_response + "▌")
        # Final render without cursor
        ai_answer_placeholder.markdown(full_response)


main()
