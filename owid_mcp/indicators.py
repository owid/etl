import urllib.parse
from typing import Any, Dict, List

import httpx
import structlog
from fastmcp import FastMCP

from owid_mcp.config import DATASETTE_BASE, HTTP_TIMEOUT, MAX_ROWS_DEFAULT
from owid_mcp.data_utils import build_catalog_info, fetch_indicator_data
from owid_mcp.data_utils import run_sql as _run_sql

log = structlog.get_logger()


INSTRUCTIONS = (
    "PRIMARY INDICATOR TOOLS (recommended for full MCP clients):\n"
    "• `search_indicator` - Find indicators with rich metadata and structured search results\n"
    "• `fetch_indicator_data_tool` - Fetch complete indicator data with full metadata\n"
    "• `run_sql` - Execute flexible SQL queries for custom data analysis\n\n"
    "CRITICAL USAGE RULES:\n"
    "• **ONLY use 1-2 simple words for search_indicator**: 'coal', 'temperature', 'population'\n"
    "• **NEVER use complex phrases**: NOT 'coal production mining', NOT 'global temperature warming'\n"
    "• Do NOT include country names in search_indicator queries\n"
    "• If no results, try even simpler single-word alternatives\n"
    "• Use `fetch_indicator_data_tool` with indicator_id for all data or with entity parameter for specific countries\n"
    "• Provides richer metadata than simplified CSV tools"
)

mcp = FastMCP()


@mcp.tool
async def search_indicator(query: str, limit: int = 10) -> List[Dict]:
    """Search for OWID indicators by name or description.

    Search for indicators by their CONCEPT/NAME only (e.g., 'population density', 'GDP', 'emissions').
    Do NOT include country/entity names in the query - those are specified separately when fetching data.

    CRITICAL: Use ONLY 1-2 simple words as the search uses LIKE pattern matching.
    GOOD: 'coal', 'temperature', 'population', 'GDP'
    BAD: 'coal production mining', 'global temperature warming', 'population density metrics'
    If a simple query returns no results, try an even simpler single-word alternative.

    Args:
        query: Indicator concept to search for (e.g., 'population density', not 'population density USA')
        limit: Maximum number of results to return

    Returns a list of indicators matching the query, each with:
    - title: indicator name
    - indicator_id: ID to use with fetch_indicator_data_tool (e.g., 2118)
    - snippet: truncated description
    - score: relevance score
    - chart_count: number of charts this indicator is used in

    Use the indicator_id with fetch_indicator_data_tool to get the actual data.

    IMPORTANT: When using the returned parquet URLs in SQL queries, note that OWID
    column names commonly use double underscores (__) as separators, not single
    underscores (_). For example: 'coal_production__twh' not 'coal_production_twh'.
    Check the sql_template in metadata for the correct column name format.
    """
    log.info("Searching indicators", query=query, limit=limit)
    
    # Warn if query is too complex
    word_count = len(query.split())
    if word_count > 2:
        log.warning(f"Query '{query}' has {word_count} words. Consider simpler terms like 'coal' or 'temperature' for better results.")
        # Return helpful error for overly complex queries
        return [{
            "title": f"Error: Query too complex ({word_count} words)",
            "indicator_id": 0,
            "snippet": f"Please use 1-2 simple words instead of '{query}'. Try 'coal' instead of 'coal production mining'.",
            "score": 0.0,
            "metadata": {"error": True}
        }]

    sql = """
    SELECT
        v.id,
        v.name,
        v.description,
        v.catalogPath,
        COALESCE(cd.chart_count, 0) AS chart_count
    FROM variables v
    JOIN datasets d ON d.id = v.datasetId
    LEFT JOIN (
        SELECT variableId, COUNT(DISTINCT chartId) AS chart_count
        FROM chart_dimensions
        GROUP BY variableId
    ) cd ON cd.variableId = v.id
    WHERE
        v.catalogPath IS NOT NULL
        AND not d.isArchived
        AND (
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
                "indicator_id": var_id,
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

    The most useful tables are: `variables`, `datasets`, and `entities`.

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


@mcp.tool
async def fetch_indicator_data_tool(indicator_id: int, entity: str | None = None) -> Dict[str, Any]:
    """Fetch OWID indicator data and metadata.

    Args:
        indicator_id: Numeric OWID indicator id (e.g. 2118)
        entity: Optional entity name or ISO-3 code. If provided, returns only rows matching that entity (case-insensitive)

    Returns:
        Dict with 'metadata' and 'data' keys:
        {
          "metadata": { <OWID metadata> },
          "data": [
              {"entity": "United States", "year": 2019, "value": 36.0},
              ...
          ]
        }
    """
    return await fetch_indicator_data(indicator_id, entity)
