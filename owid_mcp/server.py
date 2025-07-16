"""
Our World in Data – FastMCP Server (prototype v0.2)
---------------------------------------------------
• Exposes search & fetch endpoints compatible with ChatGPT Deep research
  and any other Model Context Protocol (MCP) client.
• Supports two domain‑specific search tools:
    – search_indicator → ind://{id}
    – search_chart     → chart://{slug}
• Fetch resources return:
    – JSON (data+metadata) for indicators
    – SVG (image/svg+xml) for charts, with alt‑text header

Env vars (optional overrides):
    OWID_DATASETTE_BASE   – base URL for Datasette (default public instance)
    OWID_API_BASE         – base URL for OWID v1 indicator API
    GRAPHER_BASE          – base URL for SVG charts
    PORT                  – port to serve on (default 9000)

Dependencies (pip):
    fastmcp httpx uvicorn
"""

import asyncio
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional

import httpx
from fastmcp import FastMCP
from fastmcp.server.server import Response

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ChartSearchResult:
    title: str
    resource_uri: str
    snippet: str
    metadata: Dict
    score: float

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATASETTE_BASE = os.getenv("OWID_DATASETTE_BASE", "https://datasette-public.owid.io/owid.json")
OWID_API_BASE = os.getenv("OWID_API_BASE", "https://api.ourworldindata.org/v1/indicators")
GRAPHER_BASE = os.getenv("GRAPHER_BASE", "https://ourworldindata.org/grapher")
HTTP_TIMEOUT = httpx.Timeout(10.0)

# ---------------------------------------------------------------------------
# FastMCP server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="OWID MCP Prototype",
    instructions=(
        "Search and fetch indicators & charts from Our World in Data. "
        "Call `search_indicator` or `search_chart` to discover resources. "
        "Fetch indicators via `ind://{id}` or charts via `chart://{slug}`."
    ),
)

# ---------------------------------------------------------------------------
# Search tools
# ---------------------------------------------------------------------------


@mcp.tool
async def search_indicator(query: str, limit: int = 10) -> List[Dict]:
    """Free‑text search across indicator names & descriptions."""
    sql = """
    SELECT
        id, name, description
    FROM variables
    WHERE
        name LIKE :q COLLATE NOCASE
        OR description LIKE :q COLLATE NOCASE
    LIMIT :limit
    """
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    return [
        {
            "title": row[1],
            "resource_uri": f"ind://{row[0]}",
            "snippet": (row[2] or "")[:160],
            "score": 1.0 - idx / max(1, len(rows)),
        }
        for idx, row in enumerate(rows)
    ]


@mcp.tool
async def search_chart(query: str, limit: int = 10) -> List[ChartSearchResult]:
    """Free‑text search across chart slug, title or note."""
    sql = """
    SELECT
        slug, title, note, json_extract(config,'$.dimensions[0].variableId')
    FROM charts
    WHERE
        slug LIKE :q COLLATE NOCASE
        OR title LIKE :q COLLATE NOCASE
        OR note LIKE :q COLLATE NOCASE
    LIMIT :limit
    """
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    return [
        ChartSearchResult(
            title=row[1] or row[0],
            resource_uri=f"chart://{row[0]}",
            snippet=(row[2] or "")[:160],
            metadata={"variable_id": row[3]},
            score=1.0 - idx / max(1, len(rows)),
        )
        for idx, row in enumerate(rows)
    ]


# ---------------------------------------------------------------------------
# Fetch resources
# ---------------------------------------------------------------------------


@mcp.resource("ind://{indicator_id}")
async def indicator_resource(indicator_id: int) -> Dict:
    """Return data & metadata JSON for a given indicator ID."""

    async def _fetch(kind: str):
        url = f"{OWID_API_BASE}/{indicator_id}.{kind}.json"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
            r = await c.get(url)
            r.raise_for_status()
            return r.json()

    data, metadata = await asyncio.gather(_fetch("data"), _fetch("metadata"))
    return {"data": data, "metadata": metadata}


@mcp.resource("chart://{slug}")
async def chart_resource(slug: str, **query) -> Response:
    """Return the SVG image of a chart slug; forwards query parameters (country, time…)."""
    qs = ("?" + urllib.parse.urlencode(query)) if query else ""
    source = f"{GRAPHER_BASE}/{slug}.svg{qs}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(source)
        r.raise_for_status()
        svg = r.content

    # Attempt to extract <title> for alt‑text
    match = re.search(rb"<title>([^<]{1,200})</title>", svg)
    alt = match.group(1).decode("utf-8") if match else slug.replace("-", " ")

    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={
            "X-Alt-Text": alt,
            "X-Source-URI": source,
        },
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
