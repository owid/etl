import re
from typing import Any, Dict, List, Optional

import structlog
from fastmcp import FastMCP
from pydantic import BaseModel

from etl.config import enable_sentry
from owid_mcp import charts

enable_sentry(enable_logs=True)

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
    "CHART SEARCH AND FETCH:\n"
    "• `search` - Find grapher charts via Algolia, returns CSV URLs\n"
    "• `fetch` - Download CSV data from chart URLs (with optional time filtering)\n\n"
    "USAGE:\n"
    "• Include country names in search queries for country-specific data: 'population France', 'emissions China'\n"
    "• Use simple, generic indicator names: 'coal production', 'population density', 'GDP per capita'\n"
    "• Try broad terms first, then narrow down if needed\n"
    "• Use time parameter in fetch for filtering (e.g., '1990..2010', 'earliest..2010', '1990..latest')\n"
    "• The fetch tool returns CSV data with Entity column removed - only Code, Year, and metric columns remain"
)

mcp = FastMCP()


@mcp.tool
async def search(query: str) -> List[SearchResult]:
    """Search OWID using Algolia and return grapher CSV URLs.

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
        # Call the internal search_chart function from charts module
        chart_results = await charts._search_chart_internal(query)
        
        # Convert ChartSearchResult to SearchResult
        results: List[SearchResult] = []
        for chart_result in chart_results:
            results.append(
                SearchResult(
                    id=chart_result.id,
                    title=chart_result.title,
                    text=chart_result.text,
                    url=chart_result.url,
                )
            )

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
    # Call fetch_chart_data from charts module and convert result to FetchResult
    chart_result = await charts.fetch_chart_data(id)

    return FetchResult(
        id=chart_result.id,
        title=chart_result.title,
        text=chart_result.text,
        url=chart_result.url,
        metadata=chart_result.metadata,
    )
