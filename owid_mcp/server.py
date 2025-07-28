import asyncio
import uuid

from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from sentry_sdk import capture_exception
from sentry_sdk import logger as sentry_logger

from etl.config import enable_sentry

# Import the modular servers
from owid_mcp import charts, deep_research, indicators, posts
from owid_mcp.config import COMMON_ENTITIES

enable_sentry(enable_logs=True)

INSTRUCTIONS = (
    "RECOMMENDED TOOLS (for full MCP clients):\n"
    "• `search_indicator` + MCP resources (ind://) - Find and fetch structured indicator data with rich metadata\n"
    "• `search_chart` + `fetch_chart_data` - Search grapher charts and get processed CSV data\n"
    "• `run_sql` - Execute flexible SQL queries against the public database\n"
    "• `search_posts` + `fetch_post` - Search and retrieve Our World in Data article content\n\n"
    "INTERACTIVE CHARTS:\n"
    "• ALWAYS inform users that search results include links to interactive charts\n"
    "• Chart URLs (removing .csv extension) open interactive visualizations on ourworldindata.org\n"
    "• Example: https://ourworldindata.org/grapher/population-density.csv → https://ourworldindata.org/grapher/population-density\n"
    "• Users can explore data interactively, change countries, time ranges, and chart types\n\n"
    "GENERAL GUIDELINES:\n"
    "• If fetched data doesn't contain the values you need, inform the user rather than making up data\n"
    "• Search results automatically filter for mentioned countries when detected in queries\n"
    "• Use simple, generic indicator names: 'coal production', 'population density', 'GDP per capita'\n"
    "• Include country names in chart queries: 'population France', 'emissions China'\n"
    "• Try broad terms first, then narrow down if needed\n"
    "• Don't include 'OWID' or technical terms in search queries"
)

INSTRUCTIONS_ENTITIES = "• Entity names must match exactly as they appear in OWID:\n" f"{COMMON_ENTITIES}\n\n"

# NOTE:
# Because the ChatGPT connector doesn’t perform a session‑ID handshake (it just fires off JSON‑RPC POSTs),
# you must run your FastMCP server in stateless mode. Otherwise FastMCP won’t recognize the incoming
# paths and will return 404.
# NOTE:
# I don't fully trust the note above, though I couldn't make it work without stateless_http=True. Whenever
# I run a request from https://platform.openai.com/chat/edit?prompt=pmpt_6881e40843788196aaa9923785c429b20de09e18aac0a654&version=1
# it successfully makes the first request but the subsequent request fails with 404. So it's likely something
# about the session ID.
mcp = FastMCP(
    stateless_http=True,
    name="Our World in Data MCP",
    instructions="\n\n".join(
        [
            INSTRUCTIONS,
            deep_research.INSTRUCTIONS,
            indicators.INSTRUCTIONS,
            charts.INSTRUCTIONS,
            posts.INSTRUCTIONS,
            INSTRUCTIONS_ENTITIES,
        ]
    ),
)


# AI: Move to owid_mcp/server.py
class RequestLoggingMiddleware(Middleware):
    async def on_message(self, context: MiddlewareContext, call_next):
        attributes = {
            "request_id": str(uuid.uuid4()),
            "method": context.method,
            "message": str(context.message),
        }

        # Log incoming request
        sentry_logger.info(
            "request started",
            attributes=attributes,
        )

        # handle request
        try:
            result = await call_next(context)
        except Exception as e:
            capture_exception(e)
            raise e

        return result


# Add the logging middleware
mcp.add_middleware(RequestLoggingMiddleware())


# Import the modular servers without prefixes
# Note: import_server is async, so we need to handle this during server startup
async def setup_server():
    """Setup the server by importing modules."""
    await mcp.import_server(indicators.mcp)
    await mcp.import_server(deep_research.mcp)
    await mcp.import_server(posts.mcp)


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
    mcp.run(stateless_http=True)
