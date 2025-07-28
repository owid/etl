"""
Basic healthcheck tests for the OWID MCP server.
"""

import base64

import pytest
from fastmcp import Client

from mcp.types import BlobResourceContents
from owid_mcp.server_complex import mcp


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


@pytest.mark.asyncio
async def test_charts_search_population_density():
    """Test chart search functionality with population density query."""
    async with Client(mcp) as client:
        # Test searching for population density charts
        result = await client.call_tool("search_chart", {"query": "population density"})
        assert result is not None
        # The result should be a list (even if empty)
        assert isinstance(result.data, list)

        # Print results
        for item in result.data:
            print(f"Found chart: {item['title']} (slug: {item['slug']})")


@pytest.mark.asyncio
async def test_chart_resource():
    """Test chart resource functionality by fetching a chart."""
    async with Client(mcp) as client:
        # Test fetching a chart resource with a known slug
        result = await client.read_resource("chart://population-density")
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1

        # Check the resource content
        content = result[0]
        assert str(content.uri) == "chart://population-density"

        # Check that it's SVG content
        if isinstance(content, BlobResourceContents):
            svg_text = base64.b64decode(content.blob).decode("utf-8")
        else:
            svg_text = content.text
        assert "<svg" in svg_text or "<!DOCTYPE" in svg_text
