import asyncio
import uuid

import logfire
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from sentry_sdk import capture_exception
from sentry_sdk import logger as sentry_logger

from etl.config import LOGFIRE_TOKEN_MCP, enable_sentry

# Import the modular servers
from owid_mcp import charts, indicators, posts
from owid_mcp.config import COMMON_ENTITIES

enable_sentry(enable_logs=True)

if LOGFIRE_TOKEN_MCP:
    logfire.configure(token=LOGFIRE_TOKEN_MCP, service_name="owid_mcp")
    logfire.instrument_httpx()

    # logging.basicConfig(
    #     handlers=[logfire.LogfireLoggingHandler()],
    #     level=logging.INFO,
    # )
else:
    logfire.configure(send_to_logfire=False)

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
    "• **CRITICAL for search_indicator**: Use only 1-2 simple words: 'coal', 'temperature', 'population' (NOT 'coal production mining')\n"
    "• Include country names in chart queries: 'population France', 'emissions China'\n"
    "• Try broad terms first, then narrow down if needed\n"
    "• Don't include 'OWID' or technical terms in search queries"
)

INSTRUCTIONS_ENTITIES = "• Entity names must match exactly as they appear in OWID:\n" f"{COMMON_ENTITIES}\n\n"


# NOTE:
# Because the ChatGPT connector doesn't perform a session‑ID handshake (it just fires off JSON‑RPC POSTs),
# you must run your FastMCP server in stateless mode. Otherwise FastMCP won't recognize the incoming
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
            indicators.INSTRUCTIONS,
            charts.INSTRUCTIONS,
            posts.INSTRUCTIONS,
            INSTRUCTIONS_ENTITIES,
        ]
    ),
)


class RequestLoggingMiddleware(Middleware):
    async def on_message(self, context: MiddlewareContext, call_next):
        attrs = {
            "request_id": str(uuid.uuid4()),
            "method": context.method,
            **context.message.__dict__,
        }

        # Put every MCP call inside a Logfire span so it shows up as a trace
        msg = str(context.method)
        if attrs.get("name"):
            msg += " - " + attrs["name"]
        with logfire.span(msg, **attrs):
            # Log incoming request
            sentry_logger.info(
                "request started",
                attributes=attrs,
            )

            # handle request
            try:
                result = await call_next(context)
            except Exception as e:
                capture_exception(e)
                logfire.exception("request failed", **attrs)
                raise e

        return result


# Add the logging middleware
mcp.add_middleware(RequestLoggingMiddleware())


# Import the modular servers without prefixes
# Note: import_server is async, so we need to handle this during server startup
async def setup_server():
    """Setup the server by importing modules."""
    await mcp.import_server(indicators.mcp)
    await mcp.import_server(posts.mcp)
    await mcp.import_server(charts.mcp)
    # NOTE: disabled because it wasn't working well and was confusing LLM. Feel
    #  free to remove the whole module.
    # await mcp.import_server(deep_research.mcp)


# Create the setup task - this will be awaited when needed
_server_setup_task = setup_server()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Setup the server before running
    asyncio.run(_server_setup_task)
    mcp.run(transport="http", host="0.0.0.0", port=8080, stateless_http=True)
