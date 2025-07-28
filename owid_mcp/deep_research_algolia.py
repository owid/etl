import asyncio
import base64
import io
import logging
import os
import re
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pandas as pd
import structlog
import yaml
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.utilities.types import Image
from pydantic import BaseModel

from etl.config import enable_sentry
from mcp.types import ImageContent
from owid_mcp.config import COMMON_ENTITIES, DATASETTE_BASE, HTTP_TIMEOUT, OWID_API_BASE
from owid_mcp.data_utils import build_rows, fetch_json, rows_to_csv, country_name_to_iso3, make_algolia_request

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
    "Search and fetch charts and indicators from Our World in Data..\n\n"
    "AVAILABLE TOOLS:\n"
    "• `search` - Find grapher charts via Algolia, returns CSV URLs\n"
    "• `fetch` - Download CSV data from chart URLs (with optional time filtering)\n"
    "• `fetch_chart` - Download PNG images from chart URLs\n"
    "• `search_indicators` - Find indicators by name/description, supports country: filter\n"
    "• `fetch_indicator` - Download indicator data with metadata\n\n"
    "INDICATOR SEARCH (search_indicators/fetch_indicator):\n"
    "• Call `search_indicators` to find indicators by their NAME or DESCRIPTION (e.g., 'population density', 'GDP per capita', 'life expectancy')\n"
    "• Do NOT include entity/country names in search queries - search only for the indicator concept itself\n"
    "• Use optional country: filter for specific countries (e.g., 'population density country:US')\n"
    "• Fetch indicators via returned IDs for all data or country-filtered data\n"
    "• Entity names must match exactly as they appear in OWID:\n"
    f"{COMMON_ENTITIES}\n\n"
    "GENERAL GUIDELINES:\n"
    "• If fetched data doesn't contain the values you need, inform the user rather than making up data\n"
    "• Search results automatically filter for mentioned countries when detected in queries\n\n"
    "SEARCH OPTIMIZATION:\n"
    "• DO use simple, generic indicator names: 'coal production', 'population density', 'GDP per capita'\n"
    "• DO include country names directly in chart queries: 'population France', 'emissions China'\n"
    "• DO try exact phrase matching with quotes for specific metrics: 'coal production per capita'\n"
    "• DO use broad terms first, then narrow down if needed\n"
    "• DON'T include 'OWID' in search queries\n"
    "• DON'T use overly specific queries like 'coal production per capita France Germany OWID'\n"
    "• DON'T include terms like 'dataset', 'grapher', 'Our World in Data' in searches\n"
    "• DON'T include quotes of any kind\n"
    "• DON'T combine too many filters in a single query\n\n"
    "SEARCH STRATEGY:\n"
    "1. For charts: Start with simple indicator + country: 'coal production France'\n"
    "2. For indicators: Use search_indicators with concept only: 'coal production'\n"
    "3. If that fails, try just the indicator: 'coal production'\n"
    "4. Use alternative phrasings: 'Per Capita production coal' instead of 'coal production per capita'\n"
    "5. Avoid technical terms - search for concepts, not database field names"
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
    limit = 10  # Fixed limit for deep research compatibility
    log.debug("search.start", query=query, limit=limit)

    try:
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
            results.append(
                SearchResult(
                    id=grapher_url,
                    title=title,
                    text=subtitle or title,
                    url=grapher_url,
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
async def fetch(id: str, time: Optional[str] = None) -> FetchResult:
    """Download a grapher CSV and return the processed data.

    The returned CSV has the Entity column removed and contains only:
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
    log.info("deep‑research.fetch", url=id, time=time)

    if not id.startswith("http"):
        return FetchResult(
            id=id,
            title="Invalid ID (expected URL)",
            text="",
            url=id,
            metadata={"error": "ID must be a grapher CSV URL"},
        )

    # Add time parameter to URL if provided
    fetch_url = id
    if time:
        # Parse existing URL to add time parameter
        parsed = urllib.parse.urlparse(id)
        query_params = urllib.parse.parse_qs(parsed.query)
        query_params["time"] = [time]
        new_query = urllib.parse.urlencode(query_params, doseq=True)
        fetch_url = urllib.parse.urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
        )

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(fetch_url)
            resp.raise_for_status()
            csv_content = resp.text

        # Process CSV to remove Entity column
        df = pd.read_csv(io.StringIO(csv_content))
        if "Entity" in df.columns:
            df = df.drop(columns=["Entity"])

        # Convert back to CSV string
        processed_csv: str = df.to_csv(index=False)

        slug = urllib.parse.urlparse(id).path.rsplit("/", 1)[-1].split(".")[0]
        title: str = slug.replace("-", " ").title() if slug else "OWID Dataset"

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

    except Exception as exc:  # noqa: BLE001
        log.warning("deep‑research.fetch.error", url=fetch_url, error=str(exc))
        return FetchResult(
            id=id,
            title="Error fetching CSV",
            text=f"Failed to download CSV: {exc}",
            url=fetch_url,
            metadata={"error": str(exc)},
        )
