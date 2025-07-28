"""
OWID Deep Research MCP Module
-----------------------------
Provides **search** and **fetch** tools compatible with OpenAI Deep‑Research.

* `search(query)` returns grapher **CSV URLs** (one per hit).
* `fetch(id)` downloads that CSV and returns the processed data (Entity column removed).

No special `country:` token parsing—each hit already hints at the most relevant
country via Algolia’s `availableEntities` list.
"""

import asyncio
import base64
import io
import logging
import os
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import structlog
import yaml
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.utilities.types import Image
from pydantic import BaseModel
from sentry_sdk import capture_exception
from sentry_sdk import logger as sentry_logger

from etl.config import enable_sentry
from mcp.types import ImageContent
from owid_mcp.config import HTTP_TIMEOUT

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
ALGOLIA_APP_ID = "ASCB5XMYF2"
ALGOLIA_API_KEY = "bafe9c4659e5657bf750a38fbee5c269"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"

# Global mapping cache
_NAME_TO_CODE_MAPPING: Optional[Dict[str, str]] = None


def _load_regions_mapping() -> Dict[str, str]:
    """Load OWID regions mapping from YAML file."""
    global _NAME_TO_CODE_MAPPING

    if _NAME_TO_CODE_MAPPING is not None:
        return _NAME_TO_CODE_MAPPING

    # Path to the regions file
    regions_file = (
        Path(__file__).parent.parent / "etl" / "steps" / "data" / "garden" / "regions" / "2023-01-01" / "regions.yml"
    )

    mapping: Dict[str, str] = {}

    with open(regions_file, "r", encoding="utf-8") as f:
        regions = yaml.safe_load(f)

    for region in regions:
        if not isinstance(region, dict):
            continue

        code = region.get("code")
        name = region.get("name")

        if code and name:
            # Map name to code (case-insensitive)
            mapping[name.lower()] = code

            # Map aliases to code
            aliases = region.get("aliases", [])
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str):
                        mapping[alias.lower()] = code

    log.info("regions.mapping.loaded", count=len(mapping))

    _NAME_TO_CODE_MAPPING = mapping
    return mapping


def country_name_to_iso3(name: Optional[str]) -> Optional[str]:
    """Convert country name to ISO-3 code using OWID regions mapping."""
    if not name:
        return None

    mapping = _load_regions_mapping()
    return mapping.get(name.lower())


# ---------------------------------------------------------------------------
# Create the Deep‑Research MCP server
# ---------------------------------------------------------------------------

# NOTE:
# Because the ChatGPT connector doesn’t perform a session‑ID handshake (it just fires off JSON‑RPC POSTs),
# you must run your FastMCP server in stateless mode. Otherwise FastMCP won’t recognize the incoming
# paths and will return 404.
# NOTE:
# I don't fully trust the note above, though I couldn't make it work without stateless_http=True. Whenever
# I run a request from https://platform.openai.com/chat/edit?prompt=pmpt_6881e40843788196aaa9923785c429b20de09e18aac0a654&version=1
# it successfully makes the first request but the subsequent request fails with 404. So it's likely something
# about the session ID.

mcp = FastMCP(
    stateless_http=True,
    name="OWID Deep Research",
    instructions=(
        "Search OWID charts via Algolia and fetch CSV data for Deep‑Research workflows.\n\n"
        "USAGE GUIDELINES:\n"
        "• Use `search` to find relevant grapher datasets, then `fetch` to get CSV data\n"
        "• IMPORTANT: Always include country names in your search query when looking for country-specific data (e.g., 'population France' not just 'population')\n"
        "• The fetch tool returns CSV data with Entity column removed - only Code, Year, and metric columns remain\n"
        "• INTERACTIVE CHARTS: Users can view interactive charts by removing '.csv' from search result URLs\n"
        "  - Always inform users they can open interactive charts using the provided links\n"
        "  - Example: https://ourworldindata.org/grapher/population-density becomes interactive chart\n"
        "• ALWAYS be specific with countries and time ranges to minimize data size:\n"
        "  - Use specific country names in search queries to get filtered results\n"
        "  - Use time parameter in fetch/fetch_image (e.g., '1990..2010', 'earliest..2010', '1990..latest')\n"
        "  - Prefer narrow time ranges over full historical data when possible\n"
        "• If fetched data doesn't contain the values you need, inform the user rather than making up data\n"
        "• Search results automatically filter for mentioned countries when detected in queries\n\n"
        "SEARCH OPTIMIZATION:\n"
        "• DO use simple, generic indicator names: 'coal production', 'population density', 'GDP per capita'\n"
        "• DO include country names directly in queries: 'population France', 'emissions China'\n"
        "• DO try exact phrase matching with quotes for specific metrics: 'coal production per capita'\n"
        "• DO use broad terms first, then narrow down if needed\n"
        "• DON'T include 'OWID' in search queries\n"
        "• DON'T use overly specific queries like 'coal production per capita France Germany OWID'\n"
        "• DON'T include terms like 'dataset', 'grapher', 'Our World in Data' in searches\n"
        "• DON'T include quotes of any kind\n"
        "• DON'T combine too many filters in a single query\n\n"
        "SEARCH STRATEGY:\n"
        "1. Start with simple indicator + country: 'coal production France'\n"
        "2. If that fails, try just the indicator: 'coal production'\n"
        "3. Use alternative phrasings: 'Per Capita production coal' instead of 'coal production per capita'\n"
        "4. Avoid technical terms - search for concepts, not database field names"
    ),
)


# ————————————————
# 3. FastMCP middleware: log method and payload
# ————————————————
class RequestLoggingMiddleware(Middleware):
    async def on_message(self, context: MiddlewareContext, call_next):
        attributes = {
            "request_id": str(uuid.uuid4()),
            "method": context.method,
            "message": str(context.message),
        }

        # Log incoming request
        sentry_logger.info(
            "request started",
            attributes=attributes,
        )

        # handle request
        try:
            result = await call_next(context)
        except Exception as e:
            capture_exception(e)
            raise e

        return result


# Add the logging middleware
mcp.add_middleware(RequestLoggingMiddleware())

# ---------------------------------------------------------------------------
# SEARCH TOOL – Algolia proxy
# ---------------------------------------------------------------------------


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
        payload = {
            "requests": [
                {
                    "indexName": "explorer-views-and-charts",
                    "attributesToRetrieve": [
                        "title",
                        "slug",
                        "availableEntities",
                        "variantName",
                        "type",
                    ],
                    "query": query,
                    "facetFilters": [[], "isIncomeGroupSpecificFM:false"],
                    "highlightPreTag": "<mark>",
                    "highlightPostTag": "</mark>",
                    "facets": ["tags"],
                    "hitsPerPage": limit,
                    "page": 0,
                }
            ]
        }

        headers = {
            "x-algolia-api-key": ALGOLIA_API_KEY,
            "x-algolia-application-id": ALGOLIA_APP_ID,
            "x-algolia-agent": "OWID-MCP (python)",
        }

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(ALGOLIA_URL, json=payload, headers=headers)
            resp.raise_for_status()
            response_data = resp.json()
            hits = response_data["results"][0].get("hits", [])
            log.debug("algolia.response", hits_count=len(hits))

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


# ---------------------------------------------------------------------------
# FETCH_IMAGE TOOL – downloads PNG image from CSV URL
# ---------------------------------------------------------------------------


@mcp.tool
async def fetch_image(id: str, time: Optional[str] = None) -> ImageContent:
    """Download a grapher PNG image by converting CSV URL to PNG URL.

    Takes a CSV URL from search results and converts it to PNG format by replacing
    the .csv extension with .png, then returns the image as base64-encoded content.

    Args:
        id: The full grapher CSV URL returned by `search`.
        time: Optional time range filter (e.g., '1990..2010', 'earliest..2010', '1990..latest').

    Returns:
        ImageContent with base64-encoded PNG data.
    """
    log.info("deep‑research.fetch_image", url=id, time=time)

    if not id.startswith("http"):
        # Return a text response for invalid URLs - FastMCP will handle the conversion
        raise ValueError("Invalid ID (expected URL)")

    # Convert CSV URL to PNG URL by replacing .csv with .png
    png_url = id.replace(".csv", ".png")

    # Remove CSV-specific query parameters that don't make sense for PNG
    parsed = urllib.parse.urlparse(png_url)
    query_params = urllib.parse.parse_qs(parsed.query)

    # Keep only PNG-relevant parameters
    png_params = {}
    if "country" in query_params:
        png_params["country"] = query_params["country"]
    
    # Use time parameter from function argument or existing URL
    if time:
        png_params["time"] = [time]
    elif "time" in query_params:
        png_params["time"] = query_params["time"]

    # Add chart tab parameter for PNG
    png_params["tab"] = ["chart"]

    # Reconstruct URL with PNG-appropriate parameters
    new_query = urllib.parse.urlencode(png_params, doseq=True)
    png_url = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(png_url)
            resp.raise_for_status()
            png_bytes = resp.content

        # Use FastMCP Image utility to create proper ImageContent
        img_obj = Image(data=png_bytes, format="png")
        return img_obj.to_image_content()

    except Exception as exc:
        log.warning("deep‑research.fetch_image.error", url=png_url, error=str(exc))
        raise ValueError(f"Failed to download PNG: {exc}")


if __name__ == "__main__":
    mcp.run()
