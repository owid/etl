"""
Our World in Data – FastMCP Server (prototype v0.4)
---------------------------------------------------
• Composed server that combines charts and indicators modules
• Exposes search & fetch endpoints compatible with ChatGPT Deep research
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

from fastmcp import FastMCP

# Import the modular servers
from owid_mcp import charts, deep_research, indicators
from owid_mcp.config import COMMON_ENTITIES

# ---------------------------------------------------------------------------
# FastMCP server instance with composition
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

# Import the modular servers without prefixes
# Note: import_server is async, so we need to handle this during server startup
import asyncio


async def setup_server():
    """Setup the server by importing modules."""
    # await mcp.import_server(indicators.mcp)
    # await mcp.import_server(charts.mcp)
    await mcp.import_server(deep_research.mcp)


# Create an event loop and setup the server
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

loop.run_until_complete(setup_server())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
    # deep_research.mcp.run()
