import asyncio

from fastmcp import FastMCP

# Import the modular servers
from owid_mcp import deep_research_algolia

# from owid_mcp import indicators
from owid_mcp.config import COMMON_ENTITIES

INSTRUCTIONS = "Entity names must match exactly as they appear in OWID:\n" f"{COMMON_ENTITIES}"

# "Call `run_sql` to execute read-only SQL SELECT queries against the public Datasette. "

# ---------------------------------------------------------------------------
# FastMCP server instance with composition
# ---------------------------------------------------------------------------
mcp = FastMCP(
    stateless_http=True,
    name="Our World in Data MCP",
    instructions="\n\n".join([deep_research_algolia.INSTRUCTIONS, INSTRUCTIONS]),
)


# Import the modular servers without prefixes
# Note: import_server is async, so we need to handle this during server startup
async def setup_server():
    """Setup the server by importing modules."""
    # await mcp.import_server(indicators.mcp)
    await mcp.import_server(deep_research_algolia.mcp)


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
