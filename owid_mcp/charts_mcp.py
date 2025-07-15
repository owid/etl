import json
import re
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional

import httpx
from fastmcp import FastMCP
from fastmcp.server.server import Response


@dataclass
class SearchResult:
    """Simple SearchResult class for chart search."""

    slug: str
    title: str
    subtitle: str
    resource_uri: str
    metadata: dict
    score: float


# Configuration
TIMEOUT = 30.0

# Create Charts MCP server
charts_mcp = FastMCP("Charts")


@charts_mcp.tool
async def search_chart(query: str, limit: int = 10) -> List[SearchResult]:
    """
    Free‑text chart search – returns slugs that can be fetched as chart://{slug}
    """
    sql = (
        "SELECT slug, title, subtitle, json_extract(config, '$.dimensions[0].variableId') "
        "FROM charts "
        "WHERE slug LIKE :q COLLATE NOCASE OR title LIKE :q COLLATE NOCASE OR subtitle LIKE :q COLLATE NOCASE "
        "LIMIT :limit"
    )
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.get(f"https://datasette-public.owid.io/owid.json?{qs}")
        r.raise_for_status()
        rows = r.json()["rows"]

    results = []
    for i, (slug, title, subtitle, variable_id) in enumerate(rows):
        results.append(
            SearchResult(
                slug=slug,
                title=title,
                subtitle=subtitle,
                resource_uri=f"chart://{slug}",  # <── alias, see resource below
                metadata={"variable_id": variable_id},
                score=1.0 - i / max(1, len(rows)),  # simple ranking
            )
        )
    return results


@charts_mcp.resource("chart://{slug}")
async def chart_resource(slug: str, **query) -> Response:
    """
    Streams the latest Grapher SVG for a given slug.
    Any query params (e.g. ?time=2022&country=USA) are forwarded verbatim.
    """
    qs = ("?" + urllib.parse.urlencode(query)) if query else ""
    url = f"https://ourworldindata.org/grapher/{slug}.svg{qs}"
    __import__("ipdb").set_trace()
    async with httpx.AsyncClient(timeout=TIMEOUT) as c:
        r = await c.get(url)
        r.raise_for_status()
        svg_bytes = r.content

    # Minimal alt‑text from the <title> element inside the SVG
    match = re.search(rb"<title>([^<]{1,200})</title>", svg_bytes)
    alt = match.group(1).decode("utf-8") if match else slug.replace("-", " ")

    return Response(
        content=svg_bytes,
        media_type="image/svg+xml",
        headers={
            "X‑Alt‑Text": alt,  # easy to read for screen readers / LLMs
            "X‑Source-URI": url,
        },
    )
