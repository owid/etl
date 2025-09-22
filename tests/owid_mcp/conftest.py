import pytest_asyncio

from owid_mcp.server import _server_setup_task


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_mcp_server():
    """Initialize MCP server before any tests run."""
    await _server_setup_task
