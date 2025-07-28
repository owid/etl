import asyncio
import uuid

from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from sentry_sdk import capture_exception
from sentry_sdk import logger as sentry_logger

# Import the modular servers
from owid_mcp import deep_research_algolia, indicators, posts
from owid_mcp.config import COMMON_ENTITIES

INSTRUCTIONS = (
    "GENERAL GUIDELINES:\n"
    "• If fetched data doesn't contain the values you need, inform the user rather than making up data\n"
    "• Search results automatically filter for mentioned countries when detected in queries\n"
    "SEARCH OPTIMIZATION:\n"
    "• DO use simple, generic indicator names: 'coal production', 'population density', 'GDP per capita'\n"
    "• DO include country names directly in chart queries: 'population France', 'emissions China'\n"
    "• DO try exact phrase matching with quotes for specific metrics: 'coal production per capita'\n"
    "• DO use broad terms first, then narrow down if needed\n"
    "• DON'T include 'OWID' in search queries\n"
    "• DON'T use overly specific queries like 'coal production per capita France Germany OWID'\n"
    "• DON'T include terms like 'dataset', 'grapher', 'Our World in Data' in searches\n"
    "• DON'T include quotes of any kind\n"
    "• DON'T combine too many filters in a single query\n\n"
    "SEARCH STRATEGY:\n"
    "1. For charts: Start with simple indicator + country: 'coal production France'\n"
    "2. For indicators: Use search_indicators with concept only: 'coal production'\n"
    "3. If that fails, try just the indicator: 'coal production'\n"
    "4. Use alternative phrasings: 'Per Capita production coal' instead of 'coal production per capita'\n"
    "5. Avoid technical terms - search for concepts, not database field names"
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
            deep_research_algolia.INSTRUCTIONS,
            indicators.INSTRUCTIONS,
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
    await mcp.import_server(deep_research_algolia.mcp)
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
    mcp.run()
