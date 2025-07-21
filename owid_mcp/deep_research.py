"""
OWID Deep Research MCP Module
-----------------------------
Provides search and fetch tools compatible with OpenAI's deep research specification.
"""

import asyncio
import urllib.parse
from typing import Any, Dict, List

import httpx
import structlog
from fastmcp import FastMCP

from owid_mcp.config import DATASETTE_BASE, HTTP_TIMEOUT, OWID_API_BASE
from owid_mcp.data_utils import build_rows, fetch_json, rows_to_csv

log = structlog.get_logger()

# Create the deep research MCP server
mcp = FastMCP(
    name="OWID Deep Research",
    instructions=(
        "Search and fetch OWID indicators for deep research workflows. "
        "Use `search` to find relevant indicators by query, then `fetch` to retrieve full CSV data for specific indicators."
    ),
)




@mcp.tool
async def search(query: str, limit: int = 10) -> List[Dict[str, str]]:
    """Search for OWID indicators and return search results for deep research.

    This tool is designed for deep research workflows where you need to find
    relevant indicators that can later be fetched with the fetch tool.

    Args:
        query: Search query for indicator concepts (e.g., 'population density', 'GDP', 'emissions')
        limit: Maximum number of search results to return

    Returns:
        List of search result objects, each with id, title, text, and url properties
    """
    log.info("Deep research search", query=query, limit=limit)

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
    for row in rows:
        var_id, title, description, catalog_path, chart_count = row
        # Create text snippet from description
        text_snippet = (description or title)[:200] + ("..." if len(description or title or "") > 200 else "")

        # Create URL pointing to OWID chart/data page
        chart_url = f"https://ourworldindata.org/charts?variable={var_id}"

        results.append({"id": str(var_id), "title": title, "text": text_snippet, "url": chart_url})

    log.info("Deep research search completed", found=len(results))
    return results


@mcp.tool
async def fetch(id: str) -> Dict[str, Any]:
    """Fetch OWID indicator data by ID for deep research.

    This tool retrieves the full contents of an indicator identified by its ID,
    designed to work with the search tool in deep research workflows.

    Args:
        id: Indicator ID (as string) to fetch data for

    Returns:
        Single object with id, title, text (CSV data), url, and metadata properties
    """
    log.info("Fetching indicator", indicator_id=id)

    try:
        indicator_id = int(id)

        # Fetch OWID raw data + metadata concurrently
        data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
        meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
        data_json, metadata = await asyncio.gather(fetch_json(data_url), fetch_json(meta_url))

        # Build mapping from numeric id -> {name, code}
        entities_meta = {
            ent["id"]: {"name": ent["name"], "code": ent["code"]}
            for ent in metadata["dimensions"]["entities"]["values"]
        }

        rows = build_rows(data_json, entities_meta)

        # Convert to CSV format
        csv_text = rows_to_csv(rows)

        # Get indicator name from metadata
        title = metadata.get("name", f"Indicator {indicator_id}")

        # Create URL pointing to OWID chart/data page
        chart_url = f"https://ourworldindata.org/charts?variable={indicator_id}"

        result = {
            "id": id,
            "title": title,
            "text": csv_text,
            "url": chart_url,
            "metadata": {
                "description": metadata.get("description", ""),
                "unit": metadata.get("unit", ""),
                "short_unit": metadata.get("shortUnit", ""),
                "display": metadata.get("display", {}),
                "row_count": len(rows),
            },
        }

        log.info("Fetch completed", indicator_id=id, rows=len(rows))
        return result

    except Exception as e:
        log.warning("Failed to fetch indicator", indicator_id=id, error=str(e))
        # Return error result following the spec format
        return {
            "id": id,
            "title": f"Error fetching indicator {id}",
            "text": f"Failed to fetch indicator data: {str(e)}",
            "url": f"https://ourworldindata.org/charts?variable={id}",
            "metadata": {"error": str(e)},
        }
