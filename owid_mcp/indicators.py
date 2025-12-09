import asyncio
from typing import Any, Dict, List

import structlog
from fastmcp import FastMCP

from owid_mcp.config import (
    MAX_ROWS_DEFAULT,
    OWID_API_BASE,
)
from owid_mcp.data_utils import build_efficient_rows, fetch_json
from owid_mcp.data_utils import run_sql as _run_sql
from owid_mcp.semantic_search import semantic_search_indicators

log = structlog.get_logger()


INSTRUCTIONS = (
    "PRIMARY INDICATOR TOOLS (recommended for full MCP clients):\n"
    "• `search_indicator` - Find indicators using semantic search with rich metadata\n"
    "• `fetch_indicator_data` - Fetch indicator data only in efficient format\n"
    "• `fetch_indicator_metadata` - Fetch indicator metadata only\n"
    "• `run_sql` - Execute flexible SQL queries for custom data analysis\n\n"
    "SEMANTIC SEARCH USAGE:\n"
    "• Use natural language queries: 'coal production', 'global temperature', 'population density'\n"
    "• Supports complex phrases and concepts for better matching\n"
    "• Do NOT include country names in search_indicator queries\n"
    "• The search uses semantic similarity to find conceptually related indicators\n"
    "• Use `fetch_indicator_data` with indicator_id for all data or with entity parameter for specific countries\n"
    "• Use `fetch_indicator_metadata` to get metadata separately if needed\n"
    "• Provides richer metadata than simplified CSV tools"
)

mcp = FastMCP()


@mcp.tool
async def search_indicator(query: str, limit: int = 10) -> List[Dict]:
    """Search for OWID indicators using semantic similarity via API.

    Search for indicators by their CONCEPT/NAME using natural language queries.
    The search uses embeddings and semantic similarity to find conceptually related indicators.
    Do NOT include country/entity names in the query - those are specified separately when fetching data.

    SEMANTIC SEARCH: Use natural language queries for better results.
    GOOD: 'coal production', 'global temperature warming', 'population density metrics', 'economic growth'
    ALSO GOOD: 'renewable energy', 'climate change indicators', 'health outcomes'

    The semantic search can understand complex phrases and find conceptually related indicators
    even if they don't contain the exact words in your query.

    Args:
        query: Natural language description of indicator concept (e.g., 'renewable energy production')
        limit: Maximum number of results to return

    Returns a list of indicators matching the query, each with:
    - title: indicator name
    - indicator_id: ID to use with fetch_indicator_data_tool (e.g., 2118)
    - snippet: truncated description
    - score: semantic similarity score (0.0 to 1.0)
    - chart_count: number of charts this indicator is used in

    Use the indicator_id with fetch_indicator_data to get the actual data.

    IMPORTANT: When using the returned SQL template in queries, note that OWID
    column names commonly use double underscores (__) as separators, not single
    underscores (_). For example: 'coal_production__twh' not 'coal_production_twh'.
    Check the run_sql_template in metadata for the correct column name format.
    """
    return await semantic_search_indicators(query, limit)


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
        {"csv": "actual csv content", "source": datasette_csv_url}
    """
    return await _run_sql(query, max_rows)


@mcp.tool
async def fetch_indicator_metadata(indicator_id: int) -> Dict[str, Any]:
    """Fetch OWID indicator metadata only.

    Args:
        indicator_id: Numeric OWID indicator id (e.g. 2118)

    Returns:
        Dict containing filtered metadata for the indicator
    """
    # Fetch metadata only
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    metadata = await fetch_json(meta_url)

    # Filter metadata to remove large dimensions and origins data
    metadata.pop("dimensions", None)
    metadata.pop("origins", None)

    return metadata


@mcp.tool
async def fetch_indicator_data(indicator_id: int, entity: str | None = None) -> List[Dict[str, Any]]:
    """Fetch OWID indicator data only.

    Args:
        indicator_id: Numeric OWID indicator id (e.g. 2118)
        entity: Optional entity name or ISO-3 code. If provided, returns only rows matching that entity (case-insensitive)

    Returns:
        List of entities with years and values arrays:
        [
            {
              "entity": "Africa (FAO)",
              "years": [1961, 1962, 1963, 1964, 1965],
              "values": [4191055, 4718410, 4248436, 4495870, 4723099]
            },
            ...
        ]
    """
    # Fetch OWID raw data + metadata concurrently
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(fetch_json(data_url), fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = build_efficient_rows(data_json, entities_meta)

    # Optional server-side filter for a single entity
    if entity is not None:
        ent_lower = entity.lower()
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

    return rows
