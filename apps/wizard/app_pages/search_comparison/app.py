"""Compare Algolia keyword search with semantic (AI) search side-by-side."""

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

import requests
import streamlit as st

from apps.wizard.app_pages.search_comparison.random_queries import get_benchmark_query, get_random_search_query
from apps.wizard.utils.components import st_horizontal, st_title_with_expert, url_persist
from etl.config import OWID_ENV  # noqa: F401 -- needed after TODO revert
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
    agent_type: str = "all"  # all, chart, explorer, or mdim


AGENT_MODELS = [
    "gemini",
    "gemini-lite",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-3-flash",
]

AGENT_SEARCH_MODES = ["keyword", "semantic"]

# Page config must be first Streamlit command
st.set_page_config(
    page_title="Wizard: Search Comparison",
    page_icon="🪄",
    layout="wide",
)

# TODO: revert to `OWID_ENV.site` once ai-search-api endpoints are deployed to prod
API_BASE = "http://staging-site-ai-search-api"


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
    api_base: str,
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
            f"{api_base}/api/ai-search/charts",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "hits": []}, time.time() - start


def fetch_algolia_search(query: str, hits_per_page: int, api_base: str) -> tuple[dict, float]:
    """Fetch results from Algolia keyword search API."""
    start = time.time()
    try:
        response = requests.get(
            f"{api_base}/api/search",
            params={"q": query, "hitsPerPage": hits_per_page},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "hits": []}, time.time() - start


def fetch_topics(query: str, api_base: str, limit: int = 3, source: str = "semantic") -> tuple[dict, float]:
    """Fetch recommended topics from AI search API.

    Args:
        query: Search query string
        api_base: Base URL for the API
        limit: Maximum number of topics to return
        source: Either "semantic" for vector search or "llm" for LLM-based recommendations
    """
    start = time.time()
    try:
        response = requests.get(
            f"{api_base}/api/ai-search/topics",
            params={"q": query, "limit": limit, "source": source},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), time.time() - start
    except requests.RequestException as e:
        return {"error": str(e), "hits": []}, time.time() - start


def fetch_keywords(query: str, api_base: str) -> tuple[dict, float]:
    """Fetch suggested keywords from AI search API."""
    start = time.time()
    try:
        response = requests.get(
            f"{api_base}/api/ai-search/keywords",
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
    api_base: str,
    model: str = "gemini-2.5-flash-lite",
    search: str = "keyword",
    type_filter: str = "chart",
) -> tuple[dict, float]:
    """Fetch results from Agent endpoint."""
    start = time.time()
    try:
        params = {"q": query, "model": model, "search": search}
        if type_filter and type_filter != "all":
            params["type"] = type_filter
        response = requests.get(
            f"{api_base}/api/ai-search/agent",
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
                "url": rec.get("url", f"{api_base}/grapher/{rec.get('slug', '')}"),
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


def get_hit_type(hit: dict) -> str:
    """Determine the type of a search hit: 'explorer', 'mdim', or 'chart'."""
    hit_type = hit.get("type", "")
    if hit_type == "multiDimView":
        return "mdim"
    url = hit.get("url", "")
    if "/explorers/" in url:
        return "explorer"
    return "chart"


HIT_TYPE_ICONS = {
    "chart": "📊",
    "mdim": "🔷",
    "explorer": "🧭",
}

HIT_TYPE_LABELS = {
    "chart": "Chart",
    "mdim": "Multi-dimensional indicator",
    "explorer": "Explorer",
}


def display_hit(hit: dict, index: int, search_type: str, other_rank: int | None):
    """Display a single search hit."""
    title = hit.get("title", "Untitled")
    subtitle = hit.get("subtitle", "")
    # Normalize URL to use OWID_ENV.site (API may return localhost URLs)
    raw_url = hit.get("url", "")
    if OWID_ENV.site and raw_url:
        parsed = urlparse(raw_url)
        site_parsed = urlparse(OWID_ENV.site)
        url = urlunparse((site_parsed.scheme, site_parsed.netloc, parsed.path, "", parsed.query, ""))
    else:
        url = raw_url

    hit_type = get_hit_type(hit)
    type_icon = HIT_TYPE_ICONS.get(hit_type, "📊")

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
        container_title = hit.get("containerTitle", "")
        container_suffix = f" :gray[({container_title})]" if container_title and container_title != title else ""
        type_label = HIT_TYPE_LABELS.get(hit_type, "Chart")
        st.markdown(
            f"**{index}.** {type_icon} [{title}]({url}){container_suffix} {rank_indicator}",
            help=type_label,
        )

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
                options=["all", "chart", "explorer", "mdim"],
                help="Filter results by type",
            )
    # Algolia has no options

    return options


def fetch_for_source(
    source: str,
    query: str,
    hits_per_page: int,
    options: SearchOptions,
    api_base: str,
) -> tuple[dict, float]:
    """Fetch results based on the selected source and options."""
    if source == SearchSource.ALGOLIA:
        return fetch_algolia_search(query, hits_per_page, api_base)
    elif source == SearchSource.SEMANTIC:
        return fetch_semantic_search(
            query,
            hits_per_page,
            api_base,
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
            api_base,
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

    # Query input
    col_query, col_btns = st.columns([5, 3])
    with col_query:
        query = url_persist(st.text_input)(
            key="query",
            label="Search query",
            placeholder="Enter search query (e.g., gdp, climate change, life expectancy)",
        )
    with col_btns:
        st.markdown("")  # align with text input
        with st_horizontal():
            if st.button("🎲 Random", help="Random query from real user searches (weighted by popularity)"):
                st.session_state["_set_random_query"] = get_random_search_query(require_hits=True)
                st.rerun()
            if st.button("🔍 Zero hits", help="Query that returned zero results in Algolia"):
                st.session_state["_set_random_query"] = get_random_search_query(require_hits=False)
                st.rerun()
            if st.button("🔤 Keyword Benchmark", help="Random keyword-style benchmark query (1-5 words)"):
                st.session_state["_set_random_query"] = get_benchmark_query("keyword")
                st.rerun()
            if st.button("💬 NL Benchmark", help="Random natural language benchmark query"):
                st.session_state["_set_random_query"] = get_benchmark_query("natural_language")
                st.rerun()

    # Options expander
    with st.expander("Options", expanded=False):
        # API servers
        col_search_server, col_suggestions_server = st.columns(2)
        with col_search_server:
            api_base = url_persist(st.text_input)(
                key="api_base",
                label="Search API server",
                value=API_BASE,
                help="Base URL for search APIs: /api/search, /api/ai-search/charts, /api/ai-search/agent",
            )
            api_base = api_base.rstrip("/")
        with col_suggestions_server:
            suggestions_api_base = url_persist(st.text_input)(
                key="suggestions_api_base",
                label="Suggestions API server",
                value=API_BASE,
                help="Base URL for suggestion APIs: /api/ai-search/topics, /api/ai-search/keywords",
            )
            suggestions_api_base = suggestions_api_base.rstrip("/")

        # Panel configuration and other options
        col_left_cfg, col_right_cfg, col_misc = st.columns([2, 2, 1])

        all_sources = [SearchSource.ALGOLIA, SearchSource.SEMANTIC, SearchSource.AGENT]

        with col_left_cfg:
            left_source = url_persist(st.selectbox)(
                key="left_source",
                label="Left panel",
                options=all_sources,
            )
            left_options = render_source_options("left", left_source)

        with col_right_cfg:
            # Default to Semantic (second option) if no URL param
            if "right_source" not in st.query_params:
                st.query_params["right_source"] = SearchSource.SEMANTIC
            right_source = url_persist(st.selectbox)(
                key="right_source",
                label="Right panel",
                options=all_sources,
            )
            right_options = render_source_options("right", right_source)

        with col_misc:
            hits_per_page = st.selectbox(
                "Results per page",
                options=[5, 10, 20, 50],
                index=1,
                key="hits_per_page",
            )
            type_filter = st.multiselect(
                label="Result type",
                options=["Chart", "Multi-dim", "Explorer"],
                default=["Chart", "Multi-dim", "Explorer"],
                help="Filter results by type",
                key="type_filter",
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
        topics_llm_future = executor.submit(fetch_topics, query, suggestions_api_base, 5, "llm")
        keywords_future = executor.submit(fetch_keywords, query, suggestions_api_base)
        topics_llm, _topics_llm_time = topics_llm_future.result()
        rewrite_data, _rewrite_time = keywords_future.result()

    # Display suggested keywords and topics
    keywords = rewrite_data.get("keywords", [])
    topics = []
    if "error" not in topics_llm and topics_llm.get("hits"):
        topics = [t for t in topics_llm["hits"] if t.get("score", 0) >= topic_threshold]

    if keywords or topics:
        with st.container(border=True):
            if keywords:
                keyword_links = [
                    f"[{kw}](https://ourworldindata.org/search?q={kw.replace(' ', '+')})" for kw in keywords
                ]
                st.markdown(f"🔑 **Suggested keywords:** {' · '.join(keyword_links)}")
            if topics:
                topic_links = [f"[{t['name']}]({t['url']})" for t in topics]
                st.markdown(f"📚 **Recommended topics:** {' · '.join(topic_links)}")

    # Fetch search results from both panels in parallel
    with st.spinner("Searching..."):
        with ThreadPoolExecutor(max_workers=2) as executor:
            left_future = executor.submit(fetch_for_source, left_source, query, hits_per_page, left_options, api_base)
            right_future = executor.submit(
                fetch_for_source, right_source, query, hits_per_page, right_options, api_base
            )
            left_data, left_time = left_future.result()
            right_data, right_time = right_future.result()

    # Get hits, apply type filter, and build rank mappings
    TYPE_FILTER_MAP = {"Chart": "chart", "Multi-dim": "mdim", "Explorer": "explorer"}
    allowed_types = {TYPE_FILTER_MAP[t] for t in type_filter if t in TYPE_FILTER_MAP}

    left_hits = left_data.get("hits", [])
    right_hits = right_data.get("hits", [])
    if allowed_types:
        left_hits = [h for h in left_hits if get_hit_type(h) in allowed_types]
        right_hits = [h for h in right_hits if get_hit_type(h) in allowed_types]
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

    # Headers with source info
    left_header = get_source_header(left_source, left_options)
    right_header = get_source_header(right_source, right_options)

    col_left, col_right = st.columns(2)
    with col_left:
        if "error" not in left_data:
            total = left_data.get("nbHits", len(left_hits))
            st.subheader(f"{left_header} · {total:,} results ({left_time:.2f}s)")
        else:
            st.subheader(left_header)
            st.error(f"API Error: {left_data['error']}")
    with col_right:
        if "error" not in right_data:
            total = right_data.get("nbHits", len(right_hits))
            st.subheader(f"{right_header} · {total:,} results ({right_time:.2f}s)")
        else:
            st.subheader(right_header)
            st.error(f"API Error: {right_data['error']}")

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


main()
