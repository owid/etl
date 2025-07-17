"""
Our World in Data – FastMCP Server (prototype v0.3)
---------------------------------------------------
• Exposes search & fetch endpoints compatible with ChatGPT Deep research
  and any other Model Context Protocol (MCP) client.
• Supports two domain‑specific search tools:
    – search_indicator → ind://{id}
    – search_chart     → chart://{slug}
• Fetch resources return:
    – JSON (data+metadata) for indicators
    – SVG (image/svg+xml) for charts, with alt‑text header
• NEW: run_sql tool for executing read-only SQL queries against Datasette

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
from typing import Any, Dict, List, Optional

import httpx
from fastmcp import FastMCP
from fastmcp.server.server import Response

from owid_mcp.config import (
    COMMON_ENTITIES,
    DATASETTE_BASE,
    GRAPHER_BASE,
    HTTP_TIMEOUT,
    MAX_ROWS_DEFAULT,
    MAX_ROWS_HARD,
    OWID_API_BASE,
)
from owid_mcp.utils import smart_round

# ---------------------------------------------------------------------------
# Configuration for catalog integration
# ---------------------------------------------------------------------------
CATALOG_BASE = os.getenv("CATALOG_BASE", "https://catalog.ourworldindata.org")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _build_catalog_info(catalog_path: str) -> Dict[str, str]:
    """Build Parquet URL & example SQL template from catalogPath.

    Args:
        catalog_path: Path like 'grapher/biodiversity/2025-04-07/cherry_blossom/cherry_blossom#average_20_years'
    """
    # Split on '#' to separate path from column
    path, column = catalog_path.split("#")

    # Parse the path: channel/namespace/version/dataset_slug/dataset_slug
    parts = path.split("/")
    channel, namespace, version, dataset_slug, table_name = parts[0], parts[1], parts[2], parts[3], parts[4]

    parquet_url = f"{CATALOG_BASE}/{channel}/{namespace}/{version}/{dataset_slug}/{table_name}.parquet"
    sql_tpl = "SELECT country, year, {col} FROM '{url}' " "WHERE country = '??' LIMIT 100".format(
        col=column, url=parquet_url
    )
    return {
        "parquet_url": parquet_url,
        "sql_template": sql_tpl,
    }


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
# FastMCP server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="OWID MCP Prototype",
    instructions=(
        "Search and fetch indicators & charts from Our World in Data. "
        "Call `search_indicator` to find indicators by their NAME or DESCRIPTION (e.g., 'population density', 'GDP per capita', 'life expectancy'). "
        "Do NOT include entity/country names in search queries - search only for the indicator concept itself. "
        "Call `search_chart` to find charts by slug, title, or description. "
        "Fetch indicators via `ind://{id}` for all data or `ind://{id}/{entity}` for specific country/entity data. "
        "Fetch charts via `chart://{slug}`. "
        "Call `run_sql` to execute read-only SQL SELECT queries against the public Datasette. "
        "Entity names must match exactly as they appear in OWID:\n"
        f"{COMMON_ENTITIES}"
    ),
)

# ---------------------------------------------------------------------------
# Search tools
# ---------------------------------------------------------------------------


@mcp.tool
async def search_indicator(query: str, limit: int = 10) -> List[Dict]:
    """Search for OWID indicators by name or description.

    Search for indicators by their CONCEPT/NAME only (e.g., 'population density', 'GDP', 'emissions').
    Do NOT include country/entity names in the query - those are specified separately when fetching data.

    Args:
        query: Indicator concept to search for (e.g., 'population density', not 'population density USA')
        limit: Maximum number of results to return

    Returns a list of indicators matching the query, each with:
    - title: indicator name
    - resource_uri: URI to fetch the indicator data (e.g., 'ind://2118')
    - snippet: truncated description
    - score: relevance score
    - chart_count: number of charts this indicator is used in

    Use the resource_uri with ReadMcpResourceTool to get the actual data.
    """
    sql = """
    SELECT v.id, v.name, v.description,
           v.catalogPath,
           COALESCE(cd.chart_count, 0) AS chart_count
    FROM variables v
    LEFT JOIN (
        SELECT variableId, COUNT(DISTINCT chartId) AS chart_count
        FROM chart_dimensions
        GROUP BY variableId
    ) cd ON cd.variableId = v.id
    WHERE v.name LIKE :q COLLATE NOCASE OR v.description LIKE :q COLLATE NOCASE
    ORDER BY chart_count DESC, v.name
    LIMIT :limit
    """
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    results = []
    for idx, row in enumerate(rows):
        var_id, title, desc, catalog_path, chart_count = row
        meta = _build_catalog_info(catalog_path or "")
        meta["chart_count"] = chart_count
        results.append(
            {
                "title": title,
                "resource_uri": f"ind://{var_id}",
                "snippet": (desc or "")[:160],
                "score": 1.0 - idx / max(1, len(rows)),
                "metadata": meta,
            }
        )
    return results


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


# ---------------------------------------------------------------------------
# SQL query tool
# ---------------------------------------------------------------------------

SQL_SELECT_RE = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)


@mcp.tool
async def run_sql(query: str, max_rows: int = MAX_ROWS_DEFAULT) -> Dict[str, Any]:
    """Execute a **read‑only** SQL SELECT via the OWID public Datasette.

    Parameters
    ----------
    query : str
        A SQL statement starting with `SELECT`. Anything else is rejected.
    max_rows : int
        Safety cap (1‑5000). The query is rewritten with `LIMIT` if absent.
    Returns
    -------
    dict
        {"columns": [...], "rows": [[...], ...]}
    """
    if not SQL_SELECT_RE.match(query):
        raise ValueError("Only SELECT statements are allowed.")
    if max_rows < 1 or max_rows > MAX_ROWS_HARD:
        raise ValueError(f"max_rows must be 1‑{MAX_ROWS_HARD}.")

    # Append/override LIMIT to enforce row cap
    if re.search(r"\blimit\b", query, re.IGNORECASE):
        query = re.sub(r"limit\s+\d+", f"LIMIT {max_rows}", query, flags=re.IGNORECASE)
    else:
        query = f"{query} LIMIT {max_rows}"

    qs = urllib.parse.urlencode({"sql": query, "_size": "max"})
    # Remove the .json extension from DATASETTE_BASE since it's already included in config
    datasette_base = DATASETTE_BASE.replace(".json", "")
    datasette_url = f"{datasette_base}.json?{qs}"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(datasette_url)
        resp.raise_for_status()
        js = resp.json()

    return {
        "columns": js.get("columns", []),
        "rows": js.get("rows", []),
        "source": datasette_url,
    }


# ---------------------------------------------------------------------------
# Fetch resources
# ---------------------------------------------------------------------------


def _build_rows(data_json: Dict[str, Any], entities_meta: Dict[int, Dict[str, str]]) -> List[Dict[str, Any]]:
    """Convert the compact OWID arrays into a list[{entity, year, value}]."""

    values = data_json["values"]
    years = data_json["years"]
    entity_ids = data_json["entities"]

    # Guard against length mismatch
    if not (len(values) == len(years) == len(entity_ids)):
        raise ValueError("Mismatched lengths in OWID data arrays")

    rows: List[Dict[str, Any]] = []
    append = rows.append

    for v, y, eid in zip(values, years, entity_ids):
        meta = entities_meta.get(eid)
        if meta is None:
            # Skip unknown entity id (should not normally happen)
            continue
        append(
            {
                "entity": meta["name"],
                "year": y,
                "value": smart_round(v),
            }
        )

    return rows


async def _fetch_json(url: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


@mcp.resource("ind://{indicator_id}")
async def indicator_resource(indicator_id: int, entity: str | None = None) -> Dict[str, Any]:
    """Return indicator **data** and **metadata** in a flat, convenient format.

    Args:
        indicator_id: Numeric OWID indicator id (e.g. ``2118``).
        entity: Optional entity name **or** ISO‑3 code. If provided, the server
                 returns only rows matching that entity (case‑insensitive).

    Response JSON schema::

        {
          "metadata": { <OWID metadata unchanged> },
          "data": [
              {"entity": "United States", "year": 2019, "value": 36.0},
              ...
          ]
        }
    """

    # Fetch OWID raw data + metadata concurrently.
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(_fetch_json(data_url), _fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = _build_rows(data_json, entities_meta)

    # Optional server‑side filter for a single entity
    if entity is not None:
        ent_lower = entity.lower()
        # Filter by entity name or code (check against entities_meta for code matching)
        filtered_rows = []
        for r in rows:
            if r["entity"] and r["entity"].lower() == ent_lower:
                filtered_rows.append(r)
            else:
                # Check if entity matches any code in entities_meta
                for ent_meta in entities_meta.values():
                    if ent_meta["code"] and ent_meta["code"].lower() == ent_lower and ent_meta["name"] == r["entity"]:
                        filtered_rows.append(r)
                        break
        rows = filtered_rows

    return {"metadata": metadata, "data": rows}


# Convenience alias: ``ind://{indicator_id}/{entity}``
@mcp.resource("ind://{indicator_id}/{entity}")
async def indicator_resource_for_entity(indicator_id: int, entity: str) -> Dict[str, Any]:
    """Shorthand path that simply forwards to ``indicator_resource`` with ``entity``."""
    # Fetch OWID raw data + metadata concurrently.
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(_fetch_json(data_url), _fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = _build_rows(data_json, entities_meta)

    # Filter for the specific entity
    if entity is not None:
        ent_lower = entity.lower()
        # Filter by entity name or code (check against entities_meta for code matching)
        filtered_rows = []
        for r in rows:
            if r["entity"] and r["entity"].lower() == ent_lower:
                filtered_rows.append(r)
            else:
                # Check if entity matches any code in entities_meta
                for ent_meta in entities_meta.values():
                    if ent_meta["code"] and ent_meta["code"].lower() == ent_lower and ent_meta["name"] == r["entity"]:
                        filtered_rows.append(r)
                        break
        rows = filtered_rows

    return {"metadata": metadata, "data": rows}


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
