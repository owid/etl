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
import json
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import structlog
import yaml
from fastmcp import FastMCP
from pydantic import BaseModel

from owid_mcp.config import HTTP_TIMEOUT

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

mcp = FastMCP(
    name="OWID Deep Research",
    instructions=(
        "Search OWID charts via Algolia and fetch CSV data for Deep‑Research workflows.\n\n"
        "USAGE GUIDELINES:\n"
        "• Use `search` to find relevant grapher datasets, then `fetch` to get CSV data\n"
        "• IMPORTANT: Always include country names in your search query when looking for country-specific data (e.g., 'population France' not just 'population')\n"
        "• The fetch tool returns CSV data with Entity column removed - only Code, Year, and metric columns remain\n"
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

            # We need to add this I guess...
            grapher_url += "&time=earliest..latest"

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
async def fetch(id: str) -> FetchResult:
    """Download a grapher CSV and return the processed data.

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
    log.info("deep‑research.fetch", url=id)

    if not id.startswith("http"):
        return FetchResult(
            id=id,
            title="Invalid ID (expected URL)",
            text="",
            url=id,
            metadata={"error": "ID must be a grapher CSV URL"},
        )

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(id)
            resp.raise_for_status()
            csv_content = resp.text

        # Process CSV to remove Entity column
        df = pd.read_csv(io.StringIO(csv_content))
        if "Entity" in df.columns:
            df = df.drop(columns=["Entity"])

        # Convert back to CSV string
        processed_csv = df.to_csv(index=False)

        slug = urllib.parse.urlparse(id).path.rsplit("/", 1)[-1].split(".")[0]
        title = slug.replace("-", " ").title()

        return FetchResult(
            id=id,
            title=title,
            text=processed_csv,
            url=id,
            metadata={
                "mime": "text/csv",
                "encoding": "utf-8",
                "size_bytes": len(processed_csv.encode("utf-8")),
                "rows": len(df),
                "columns": list(df.columns),
            },
        )

    except Exception as exc:  # noqa: BLE001
        log.warning("deep‑research.fetch.error", url=id, error=str(exc))
        return FetchResult(
            id=id,
            title=f"Error fetching CSV",
            text=f"Failed to download CSV: {exc}",
            url=id,
            metadata={"error": str(exc)},
        )


if __name__ == "__main__":
    mcp.run()
