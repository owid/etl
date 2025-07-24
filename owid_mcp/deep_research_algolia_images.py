"""
OWID Deep Research MCP Module
-----------------------------
Provides **search** and **fetch** tools compatible with OpenAI Deep‑Research.

* `search(query)` returns grapher **PNG URLs** (one per hit).
* `fetch(id)` downloads that PNG and returns the **raw image as base‑64** so the
  Deep‑Research agent can embed or display it inline.

No special `country:` token parsing—each hit already hints at the most relevant
country via Algolia’s `availableEntities` list.
"""

import asyncio
import base64
import json
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx
import pycountry
import structlog
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


def country_name_to_iso3(name: Optional[str]) -> Optional[str]:
    """Best‑effort conversion from common country names to ISO‑3 codes."""
    if not name:
        return None
    
    # Manual mappings for OWID-specific entities
    manual: Dict[str, str] = {
        "World": "OWID_WRL",
        "European Union": "OWID_EUN",
        "OECD": "OWID_OECD",
        "High income": "OWID_HIN",
        "Low income": "OWID_LIN",
    }
    
    # Check manual mappings first
    if name in manual:
        return manual[name]
    
    # Try pycountry lookup
    try:
        return pycountry.countries.lookup(name).alpha_3  # type: ignore[attr-defined]
    except (LookupError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Create the Deep‑Research MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="OWID Deep Research",
    instructions=(
        "Search OWID charts via Algolia and fetch PNG charts for Deep‑Research "
        "workflows. Use `search` to find relevant grapher images and `fetch` to "
        "download the PNG (base‑64 encoded)."
    ),
)

# ---------------------------------------------------------------------------
# SEARCH TOOL – Algolia proxy
# ---------------------------------------------------------------------------


@mcp.tool
async def search(query: str, limit: int = 10) -> List[SearchResult]:
    """Search OWID using Algolia and return grapher PNG URLs.

    Args:
        query: Free‑text query (e.g. "population density france").
        limit: Maximum number of hits to return (<=60).

    Returns:
        List of SearchResult objects with id, title, text, and url fields.
    """
    log.info("deep‑research.search", query=query, limit=limit)

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
        hits = resp.json()["results"][0].get("hits", [])

    results: List[SearchResult] = []
    for hit in hits:
        # Not needed
        del hit["availableEntities"]

        slug = hit["slug"]
        title = hit.get("title") or slug.replace("-", " ").title()
        subtitle = (
            hit.get("_snippetResult", {})
            .get("subtitle", {})
            .get("value", "")
            .replace("<mark>", "")
            .replace("</mark>", "")
        )

        # Attempt to deduce a country from highlight results for tighter charts
        country_name: Optional[str] = None
        for ent in hit.get("_highlightResult", {}).get("availableEntities", []):
            if ent.get("matchedWords"):
                country_name = ent["value"].replace("<mark>", "").replace("</mark>", "")
                break
        country_iso3 = country_name_to_iso3(country_name)

        if country_iso3:
            grapher_url = f"https://ourworldindata.org/grapher/{slug}.png?tab=chart&country=~{country_iso3}"
        else:
            grapher_url = f"https://ourworldindata.org/grapher/{slug}.png?tab=chart"

        results.append(
            SearchResult(
                id=grapher_url,
                title=title,
                text=subtitle or title,
                url=grapher_url,
            )
        )

    log.info("deep‑research.search.done", returned=len(results))
    return results


# ---------------------------------------------------------------------------
# FETCH TOOL – downloads PNG and base‑64 encodes it
# ---------------------------------------------------------------------------


@mcp.tool
async def fetch(id: str) -> FetchResult:
    """Download a grapher PNG and return it as base‑64.

    Args:
        id: The full grapher URL returned by `search`.

    Returns:
        FetchResult with `text` containing the base‑64 PNG.
    """
    log.info("deep‑research.fetch", url=id)

    if not id.startswith("http"):
        return FetchResult(
            id=id,
            title="Invalid ID (expected URL)",
            text="",
            url=id,
            metadata={"error": "ID must be a grapher PNG URL"},
        )

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(id)
            resp.raise_for_status()
            png_bytes = resp.content

        b64_png = base64.b64encode(png_bytes).decode("ascii")
        slug = urllib.parse.urlparse(id).path.rsplit("/", 1)[-1].split(".")[0]
        title = slug.replace("-", " ").title()

        return FetchResult(
            id=id,
            title=title,
            text=b64_png,
            url=id,
            metadata={
                "mime": "image/png",
                "encoding": "base64",
                "size_bytes": len(png_bytes),
            },
        )

    except Exception as exc:  # noqa: BLE001
        log.warning("deep‑research.fetch.error", url=id, error=str(exc))
        return FetchResult(
            id=id,
            title=f"Error fetching image",
            text=f"Failed to download PNG: {exc}",
            url=id,
            metadata={"error": str(exc)},
        )


if __name__ == "__main__":
    mcp.run()
