"""
Basic healthcheck tests for the OWID MCP server.
"""

import pytest
from fastmcp import Client

from etl_mcp.server import mcp


@pytest.mark.asyncio
async def test_mcp_server_health():
    """Test that the MCP server starts and responds correctly."""
    async with Client(mcp) as client:
        # Test that the server is responsive by listing tools
        tools = await client.list_tools()
        assert tools is not None
        assert len(tools) > 0


@pytest.mark.asyncio
async def test_catalog_find_table_basic_functionality():
    """Test basic functionality of catalog_find_table tool."""
    async with Client(mcp) as client:
        # Test with simple parameters that should work
        result = await client.call_tool("catalog_find_table", {"channel": "garden", "namespace": "demography"})
        assert result is not None
        # The result should be a list (even if empty)
        assert isinstance(result.data, list)
