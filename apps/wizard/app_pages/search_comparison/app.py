"""Compare Algolia keyword search with semantic (AI) search side-by-side."""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Generator

import requests
import streamlit as st

from apps.wizard.app_pages.search_comparison.random_queries import get_random_search_query
from apps.wizard.utils.components import st_horizontal, st_title_with_expert, url_persist
from etl.config import ENV_IS_REMOTE, OWID_ENV, STAGING
from etl.db import read_sql


# Search source types
class SearchSource:
    ALGOLIA = "Algolia (Keyword)"
    SEMANTIC = "Semantic (AI)"
    AGENT = "Agent"


@dataclass
class SearchOptions:
    """Options for a search source."""

    # Semantic options
    llm_rerank: bool = False
    llm_model: str = "small"
    include_explorers: bool = False

    # Agent options
    agent_model: str = "gemini-2.5-flash-lite"
    agent_search: str = "keyword"
    agent_type: str = "all"  # all, chart, explorer, or multiDim


AGENT_MODELS = [
    # Gemini models
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-3-flash",
    # OpenAI models
    "openai",
]

AGENT_SEARCH_MODES = ["keyword", "semantic"]

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


def fetch_query_rewrite(query: str) -> tuple[dict, float]:
    """Fetch rewritten query keywords from AI search API."""
    start = time.time()
    try:
        response = requests.get(
            f"{API_BASE}/api/ai-search/rewrite",
            params={"q": query},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "keywords": []}, time.time() - start


def fetch_agent_search(
    query: str,
    hits_per_page: int,
    model: str = "gemini-2.5-flash-lite",
    search: str = "keyword",
    type_filter: str = "chart",
) -> tuple[dict, float]:
    """Fetch results from Agent recommend endpoint."""
    start = time.time()
    try:
        params = {"q": query, "model": model, "search": search}
        if type_filter and type_filter != "all":
            params["type"] = type_filter
        response = requests.get(
            f"{API_BASE}/api/ai-search/recommend",
            params=params,
            timeout=60,  # Agent may take longer
        )
        response.raise_for_status()
        data = response.json()
        # Normalize response format to match other endpoints
        recommendations = data.get("recommendations", [])[:hits_per_page]
        # Add URL if missing and convert to hits format
        hits = []
        for rec in recommendations:
            hit = {
                "title": rec.get("title", ""),
                "subtitle": rec.get("subtitle", ""),
                "slug": rec.get("slug", ""),
                "url": rec.get("url", f"{API_BASE}/grapher/{rec.get('slug', '')}"),
                "type": rec.get("type", "chart"),
            }
            hits.append(hit)
        return {"hits": hits, "nbHits": len(hits), "model": data.get("model")}, time.time() - start
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


def render_source_options(side: str, source: str) -> SearchOptions:
    """Render options for a search source and return the options object."""
    options = SearchOptions()

    if source == SearchSource.SEMANTIC:
        with st_horizontal():
            options.llm_rerank = url_persist(st.checkbox)(
                key=f"{side}_llm_rerank",
                label="LLM rerank",
                value=False,
            )
            options.llm_model = url_persist(st.selectbox)(
                key=f"{side}_llm_model",
                label="Model",
                options=["small", "large"],
                index=0,
            )
            options.include_explorers = url_persist(st.checkbox)(
                key=f"{side}_include_explorers",
                label="Explorers",
                value=False,
            )
    elif source == SearchSource.AGENT:
        with st_horizontal():
            options.agent_model = url_persist(st.selectbox)(
                key=f"{side}_agent_model",
                label="Model",
                options=AGENT_MODELS,
            )
            options.agent_search = url_persist(st.selectbox)(
                key=f"{side}_agent_search",
                label="Search",
                options=AGENT_SEARCH_MODES,
            )
            options.agent_type = url_persist(st.selectbox)(
                key=f"{side}_agent_type",
                label="Type",
                options=["all", "chart", "explorer", "multiDim"],
                help="Filter results by type",
            )
    # Algolia has no options

    return options


def fetch_for_source(
    source: str,
    query: str,
    hits_per_page: int,
    options: SearchOptions,
) -> tuple[dict, float]:
    """Fetch results based on the selected source and options."""
    if source == SearchSource.ALGOLIA:
        return fetch_algolia_search(query, hits_per_page)
    elif source == SearchSource.SEMANTIC:
        return fetch_semantic_search(
            query,
            hits_per_page,
            options.include_explorers,
            False,  # rerank
            False,  # rewrite
            options.llm_rerank,
            options.llm_model,
        )
    elif source == SearchSource.AGENT:
        return fetch_agent_search(
            query,
            hits_per_page,
            options.agent_model,
            options.agent_search,
            options.agent_type,
        )
    else:
        return {"error": f"Unknown source: {source}", "hits": []}, 0.0


def get_source_header(source: str, options: SearchOptions) -> str:
    """Get the header text for a search source."""
    if source == SearchSource.ALGOLIA:
        return "🔤 Algolia (Keyword)"
    elif source == SearchSource.SEMANTIC:
        header = "🧠 Semantic (AI)"
        if options.llm_rerank:
            header += f" + LLM ({options.llm_model})"
        return header
    elif source == SearchSource.AGENT:
        return f"🤖 Agent ({options.agent_model})"
    return source


def get_search_type(source: str) -> str:
    """Get the search type string for display purposes."""
    if source == SearchSource.ALGOLIA:
        return "algolia"
    elif source == SearchSource.SEMANTIC:
        return "semantic"
    elif source == SearchSource.AGENT:
        return "agent"
    return "unknown"


def main():
    st_title_with_expert("Search Comparison", icon=":material/compare:")

    st.markdown(
        "Compare different search approaches side-by-side: **Algolia keyword search**, "
        "**semantic (AI) search**, and **Agent-based recommendations**."
    )

    # Handle random query selection (must be before widget creation)
    if st.session_state.get("_set_random_query"):
        new_query = st.session_state.pop("_set_random_query")
        # Clear session state for query widget so url_persist picks up new value
        if "query" in st.session_state:
            del st.session_state["query"]
        st.query_params["query"] = new_query
        st.rerun()

    # Query input and global options
    col_query, col_results = st.columns([5, 1])
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

    # Quick actions and global options
    col1, col2 = st.columns([1, 2])

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

    # Fetch topics and keywords early (in parallel)
    with ThreadPoolExecutor(max_workers=2) as executor:
        topics_llm_future = executor.submit(fetch_topics, query, 5, "llm")
        rewrite_future = executor.submit(fetch_query_rewrite, query)
        topics_llm, _topics_llm_time = topics_llm_future.result()
        rewrite_data, _rewrite_time = rewrite_future.result()

    # Display suggested keywords and topics
    keywords = rewrite_data.get("keywords", [])
    topics = []
    if "error" not in topics_llm and topics_llm.get("hits"):
        topics = [t for t in topics_llm["hits"] if t.get("score", 0) >= topic_threshold]

    if keywords or topics:
        with st.container(border=True):
            if keywords:
                keyword_links = [f"[{kw}](https://ourworldindata.org/search?q={kw.replace(' ', '+')})" for kw in keywords]
                st.markdown(f"🔑 **Suggested keywords:** {' · '.join(keyword_links)}")
            if topics:
                topic_links = [f"[{t['name']}]({t['url']})" for t in topics]
                st.markdown(f"📚 **Recommended topics:** {' · '.join(topic_links)}")

    # Source selectors for left and right panels
    st.markdown("---")
    all_sources = [SearchSource.ALGOLIA, SearchSource.SEMANTIC, SearchSource.AGENT]

    col_left, col_right = st.columns(2)

    with col_left:
        left_source = url_persist(st.selectbox)(
            key="left_source",
            label="Left panel",
            options=all_sources,
        )
        left_options = render_source_options("left", left_source)

    with col_right:
        # Default to Semantic (second option) if no URL param
        if "right_source" not in st.query_params:
            st.query_params["right_source"] = SearchSource.SEMANTIC
        right_source = url_persist(st.selectbox)(
            key="right_source",
            label="Right panel",
            options=all_sources,
        )
        right_options = render_source_options("right", right_source)

    # Fetch search results from both panels in parallel
    with st.spinner("Searching..."):
        with ThreadPoolExecutor(max_workers=2) as executor:
            left_future = executor.submit(
                fetch_for_source, left_source, query, hits_per_page, left_options
            )
            right_future = executor.submit(
                fetch_for_source, right_source, query, hits_per_page, right_options
            )
            left_data, left_time = left_future.result()
            right_data, right_time = right_future.result()

    # Get hits and build rank mappings
    left_hits = left_data.get("hits", [])
    right_hits = right_data.get("hits", [])
    max_hits = max(len(right_hits), len(left_hits))

    # Fetch pageviews and FM ranks for results that don't include them (Algolia, Agent)
    def enrich_hits_with_stats(hits: list[dict], source: str) -> None:
        """Enrich hits with pageviews and FM ranks if the source doesn't provide them."""
        if source in (SearchSource.ALGOLIA, SearchSource.AGENT):
            slugs = tuple(h.get("slug") for h in hits if h.get("slug"))
            if slugs:
                pageviews = get_pageviews_for_slugs(slugs)
                fm_ranks = get_fm_ranks_for_slugs(slugs)
                for hit in hits:
                    slug = hit.get("slug")
                    if slug:
                        if slug in pageviews:
                            hit["views_365d"] = pageviews[slug]
                        if slug in fm_ranks:
                            hit["fmRank"] = fm_ranks[slug]

    enrich_hits_with_stats(left_hits, left_source)
    enrich_hits_with_stats(right_hits, right_source)

    left_slug_to_rank = build_slug_to_rank(left_hits)
    right_slug_to_rank = build_slug_to_rank(right_hits)

    # Create placeholder for AI answer at the top (will be filled after charts render)
    if enable_ai_answer:
        st.subheader("🤖 AI Answer")
        ai_answer_placeholder = st.empty()

    # Headers with source info
    left_header = get_source_header(left_source, left_options)
    right_header = get_source_header(right_source, right_options)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader(left_header)
        if "error" not in left_data:
            total = left_data.get("nbHits", len(left_hits))
            st.info(f"**{total:,}** results ({left_time:.2f}s)")
        else:
            st.error(f"API Error: {left_data['error']}")
    with col_right:
        st.subheader(right_header)
        if "error" in right_data:
            st.error(f"API Error: {right_data['error']}")
        else:
            total = right_data.get("nbHits", len(right_hits))
            st.info(f"**{total:,}** results ({right_time:.2f}s)")

    # Results row by row
    left_search_type = get_search_type(left_source)
    right_search_type = get_search_type(right_source)

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
                display_hit(hit, i + 1, right_search_type, other_rank)

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
