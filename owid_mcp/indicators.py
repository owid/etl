from typing import Any, Dict, List
import urllib.parse

import httpx
import structlog
from fastmcp import FastMCP

from owid_mcp.config import MAX_ROWS_DEFAULT, HTTP_TIMEOUT, DATASETTE_BASE
from owid_mcp.data_utils import build_catalog_info, fetch_indicator_data
from owid_mcp.data_utils import run_sql as _run_sql

log = structlog.get_logger()


INSTRUCTIONS = (
    "PRIMARY INDICATOR TOOLS (recommended for full MCP clients):\n"
    "• `search_indicator` - Find indicators with rich metadata and structured search results\n"
    "• MCP Resources (ind://{id}) - Fetch complete indicator data with full metadata\n"
    "• `run_sql` - Execute flexible SQL queries for custom data analysis\n\n"
    "USAGE:\n"
    "• Search by indicator concept only: 'population density', 'GDP per capita', 'life expectancy'\n"
    "• Do NOT include country names in search_indicator queries\n"
    "• Use ind://{id} for all data or ind://{id}/{entity} for specific countries\n"
    "• Provides richer metadata than simplified CSV tools"
)

mcp = FastMCP()


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
    log.info("Searching indicators", query=query, limit=limit)

    sql = """
    SELECT
        v.id,
        v.name,
        v.description,
        v.catalogPath,
        COALESCE(cd.chart_count, 0) AS chart_count
    FROM variables v
    LEFT JOIN (
        SELECT variableId, COUNT(DISTINCT chartId) AS chart_count
        FROM chart_dimensions
        GROUP BY variableId
    ) cd ON cd.variableId = v.id
    WHERE v.catalogPath IS NOT NULL AND (
            v.name LIKE :q COLLATE NOCASE
            OR
            v.description LIKE :q COLLATE NOCASE
    )
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
        log.info("Found indicator", id=var_id, title=title, catalog_path=catalog_path)
        meta = build_catalog_info(catalog_path)
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

    log.info("Search completed", found=len(results))
    return results


# Create a tool wrapper for the shared run_sql function
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
    return await _run_sql(query, max_rows)


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
    return await fetch_indicator_data(indicator_id, entity)


# Convenience alias: ``ind://{indicator_id}/{entity}``
@mcp.resource("ind://{indicator_id}/{entity}")
async def indicator_resource_for_entity(indicator_id: int, entity: str) -> Dict[str, Any]:
    """Shorthand path that simply forwards to ``indicator_resource`` with ``entity``."""
    return await fetch_indicator_data(indicator_id, entity)
