"""
OWID Charts MCP Server Module
----------------------------
Provides chart search and retrieval functionality for Our World in Data.
"""

import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List

import httpx
from fastmcp import FastMCP
from fastmcp.server.server import Response

from owid_mcp.config import DATASETTE_BASE, GRAPHER_BASE, HTTP_TIMEOUT


@dataclass
class ChartSearchResult:
    title: str
    resource_uri: str
    snippet: str
    metadata: Dict
    score: float


# Create the charts MCP server
mcp = FastMCP(
    name="OWID Charts",
    instructions=(
        "Search and fetch charts from Our World in Data. "
        "Call `search_chart` to find charts by slug, title, or description. "
        "Fetch charts via `chart://{slug}` resource URIs."
    ),
)


@mcp.tool
async def search_chart(query: str, limit: int = 10) -> List[ChartSearchResult]:
    """Search for OWID charts by slug, title, or note.

    Returns a list of charts matching the query, each with:
    - title: chart title
    - resource_uri: URI to fetch the chart SVG (e.g., 'chart://population-density')
    - snippet: truncated chart note/description
    - metadata: additional chart metadata
    - score: relevance score

    Use the resource_uri with ReadMcpResourceTool to get the chart image.
    """
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
