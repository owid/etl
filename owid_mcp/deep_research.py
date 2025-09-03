import io
import re
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import structlog
from fastmcp import FastMCP
from pydantic import BaseModel

from owid_mcp.config import HTTP_TIMEOUT
from owid_mcp.data_utils import country_name_to_iso3, make_algolia_request

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Pydantic models for structured responses
# ---------------------------------------------------------------------------


class SearchResult(BaseModel):
    """Search result item following Deep Research specification."""

    id: str  # unique ID for the document or search result item
    title: str  # string title for the search result item
    text: str  # relevant snippet of text for the search terms
    url: str  # URL to the document or search result item


class FetchResult(BaseModel):
    """Fetch result item following Deep Research specification."""

    id: str  # unique ID for the document or search result item
    title: str  # string title for the search result item
    text: str  # full text of the document or item
    url: str  # URL to the document or search result item
    metadata: Optional[Dict[str, Any]] = None  # optional key/value pairing of data


# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

# Country filter pattern for indicator search
COUNTRY_RE = re.compile(r"\bcountry:(\w{2,3})\b", re.IGNORECASE)


INSTRUCTIONS = (
    "DEEP RESEARCH TOOLS (EXCLUSIVELY for ChatGPT Deep Research):\n"
    "• `search` - Find indicators via Algolia for simplified CSV access\n"
    "• `fetch` - Download processed CSV data from search results\n\n"
    "NOTE: These tools are designed EXCLUSIVELY for ChatGPT's Deep Research feature.\n"
    "They follow the OpenAI Deep Research specification and should NOT be used by\n"
    "other MCP clients. For general MCP clients, use the richer indicator and chart tools.\n\n"
    "INTERACTIVE CHARTS:\n"
    "• Search results include interactive chart URLs - ALWAYS show these to users\n"
    "• Users can explore data interactively by opening the provided links\n\n"
    "USAGE:\n"
    "• Include country names in search queries: 'population France', 'emissions China'\n"
    "• Use simple indicator names: 'coal production', 'population density'\n"
    "• Returns simplified CSV format (Entity column removed)"
)

mcp = FastMCP()


async def _search_chart_internal(query: str) -> List[SearchResult]:
    """Search OWID using Algolia and return grapher CSV URLs.

    IMPORTANT: Include country names in your query for country-specific data.
    Examples: "population density france", "co2 emissions china", "gdp germany"

    Args:
        query: Free‑text query. Always include country names when seeking country data.

    Returns:
        List of SearchResult objects with CSV URLs. URLs are automatically filtered
        for countries mentioned in the query.
    """
    limit = 10  # Fixed limit for deep research compatibility
    log.debug("search_chart.start", query=query, limit=limit)

    hits = await make_algolia_request(query, limit)

    results: List[SearchResult] = []
    for hit in hits:
        # Not needed
        hit.pop("availableEntities", [])

        slug = hit["slug"]
        title = hit.get("title") or slug.replace("-", " ").title()
        subtitle = (
            hit.get("_snippetResult", {})
            .get("subtitle", {})
            .get("value", "")
            .replace("<mark>", "")
            .replace("</mark>", "")
        )

        # Attempt to deduce countries from highlight results for tighter charts
        country_codes: List[str] = []
        highlight_entities = hit.get("_highlightResult", {}).get("availableEntities", [])

        for ent in highlight_entities:
            if ent.get("matchedWords"):
                country_name = ent["value"].replace("<mark>", "").replace("</mark>", "")
                country_iso3 = country_name_to_iso3(country_name)
                if country_iso3:
                    country_codes.append(country_iso3)

        # &time=earliest..latest probably wrong...
        grapher_url = f"https://ourworldindata.org/grapher/{slug}.csv?tab=line&csvType=filtered"

        # Perhaps we should use this?
        # https://ourworldindata.org/grapher/population-density.csv?v=1&csvType=filtered&useColumnShortNames=true&tab=line&country=FRA~DEU

        if country_codes:
            # Format multiple countries as country=FRA~DEU~...
            country_param = "~".join(country_codes)
            grapher_url += f"&country={country_param}"

        # NOTE: we could get metadata from https://ourworldindata.org/grapher/urban-area-long-term.metadata.json?v=1&csvType=filtered&useColumnShortNames=true
        # TODO: we return grapher_url as id only because of deep research compatibility. Once it starts supporting regular MPC, we should
        #   stop adding `tab=line&csvType=filtered`
        results.append(
            SearchResult(
                id=grapher_url,
                title=title,
                text=subtitle or title,
                url=grapher_url,
            )
        )

    log.debug("search_chart.done", returned=len(results))
    return results


async def _fetch_chart_data_internal(id: str, time: Optional[str] = None) -> FetchResult:
    """Download a grapher CSV and return the processed data.

    The returned CSV includes:
    - Entity: Country/region names (removed if Code column has no empty values)
    - Code: Country/region ISO codes
    - Year: Time period
    - [Metric columns]: The actual data values

    IMPORTANT: Only provide data that exists in the fetched CSV. Do not hallucinate
    or interpolate missing values.

    Args:
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        FetchResult with `text` containing processed CSV data and metadata about
        the dataset structure.
    """
    log.info("chart.fetch_data", url=id, time=time)

    if not id.startswith("http"):
        return FetchResult(
            id=id,
            title="Invalid ID (expected URL)",
            text="",
            url=id,
            metadata={"error": "ID must be a grapher CSV URL"},
        )

    # Parse URL and add required parameters
    parsed = urllib.parse.urlparse(id)
    query_params = urllib.parse.parse_qs(parsed.query)

    # Add tab=line&csvType=filtered if not already present
    if "tab" not in query_params:
        query_params["tab"] = ["line"]
    if "csvType" not in query_params:
        query_params["csvType"] = ["filtered"]

    # Add time parameter if provided
    if time:
        query_params["time"] = [time]

    new_query = urllib.parse.urlencode(query_params, doseq=True)
    fetch_url = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(fetch_url)
        resp.raise_for_status()
        csv_content = resp.text

    # Process CSV - remove Entity column if Code column has no empty values
    df = pd.read_csv(io.StringIO(csv_content))

    # Check if Code column exists and has no empty/null values
    if "Code" in df.columns and "Entity" in df.columns:
        # Check for empty, null, or whitespace-only values in Code column
        code_has_empty = df["Code"].isna().any() or (df["Code"].astype(str).str.strip() == "").any()
        if not code_has_empty:
            # Remove Entity column as it's redundant when Code is complete
            df = df.drop(columns=["Entity"])

    # Convert back to CSV string
    processed_csv: str = df.to_csv(index=False)  # type: ignore

    slug = urllib.parse.urlparse(id).path.rsplit("/", 1)[-1].split(".")[0]
    title = slug.replace("-", " ").title() if slug else "OWID Dataset"

    return FetchResult(
        id=id,
        title=title,
        text=processed_csv,
        url=fetch_url,
        metadata={
            "mime": "text/csv",
            "encoding": "utf-8",
            "size_bytes": len(processed_csv.encode("utf-8")),
            "rows": len(df),
            "columns": list(df.columns),
            "time_filter": time,
        },
    )


@mcp.tool
async def search(query: str) -> List[SearchResult]:
    """Search OWID using Algolia and return grapher CSV URLs.

    EXCLUSIVELY FOR CHATGPT DEEP RESEARCH - Do not use with other MCP clients.

    IMPORTANT: Include country names in your query for country-specific data.
    Examples: "population density france", "co2 emissions china", "gdp germany"

    Args:
        query: Free‑text query. Always include country names when seeking country data.

    Returns:
        List of SearchResult objects with CSV URLs. URLs are automatically filtered
        for countries mentioned in the query.
    """
    log.debug("search.start", query=query)

    try:
        # Call the internal search_chart function
        results = await _search_chart_internal(query)

        log.debug("search.done", returned=len(results))
        return results

    except Exception as exc:
        log.error("search.error", query=query, error=str(exc))
        return []


# ---------------------------------------------------------------------------
# FETCH TOOL – downloads CSV and processes it
# ---------------------------------------------------------------------------


@mcp.tool
async def fetch(id: str) -> FetchResult:
    """Download a chart data and return the processed data.

    EXCLUSIVELY FOR CHATGPT DEEP RESEARCH - Do not use with other MCP clients.

    The returned CSV has the Entity column removed and contains only:
    - Code: Country/region ISO codes
    - Year: Time period
    - [Metric columns]: The actual data values

    IMPORTANT: Only provide data that exists in the fetched CSV. Do not hallucinate
    or interpolate missing values.

    Args:
        id: The full grapher CSV URL returned by `search`.

    Returns:
        FetchResult with `text` containing processed CSV data and metadata about
        the dataset structure.
    """
    # Call the internal fetch_chart_data function
    return await _fetch_chart_data_internal(id)
