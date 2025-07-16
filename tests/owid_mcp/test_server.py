"""
Basic healthcheck tests for the OWID MCP server.
"""

import base64

import pytest
from fastmcp import Client

from owid_mcp.server import mcp


@pytest.mark.asyncio
async def test_mcp_server_health():
    """Test that the MCP server starts and responds correctly."""
    async with Client(mcp) as client:
        # Test that the server is responsive by listing tools
        tools = await client.list_tools()
        assert tools is not None
        assert len(tools) > 0


@pytest.mark.asyncio
async def test_search_indicator_basic_functionality():
    """Test basic functionality of search_indicator tool."""
    async with Client(mcp) as client:
        # Test searching for population indicators
        result = await client.call_tool("search_indicator", {"query": "population"})
        assert result is not None
        # The result should be a list (even if empty)
        assert isinstance(result.data, list)


@pytest.mark.asyncio
async def test_search_chart_basic_functionality():
    """Test basic functionality of search_chart tool."""
    async with Client(mcp) as client:
        # Test searching for population density charts
        result = await client.call_tool("search_chart", {"query": "population density"})
        assert result is not None
        # The result should be a list (even if empty)
        assert isinstance(result.data, list)


@pytest.mark.asyncio
async def test_indicator_resource():
    """Test indicator resource functionality by fetching an indicator."""
    async with Client(mcp) as client:
        # Test fetching an indicator resource with a known ID
        # Using GDP indicator ID which should exist
        result = await client.read_resource("ind://2118")
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1

        # Check the resource content
        content = result[0]
        assert str(content.uri) == "ind://2118"

        # Parse the JSON content
        import json

        data = json.loads(content.text)
        assert "metadata" in data
        assert "data" in data
        assert isinstance(data["data"], list)


@pytest.mark.asyncio
async def test_indicator_resource_for_entity():
    """Test indicator resource for specific entity functionality."""
    async with Client(mcp) as client:
        # Test fetching an indicator resource for a specific entity
        # Using GDP indicator ID with USA entity
        result = await client.read_resource("ind://2118/USA")
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1

        # Check the resource content
        content = result[0]
        assert str(content.uri) == "ind://2118/USA"

        # Parse the JSON content
        import json

        data = json.loads(content.text)
        assert "metadata" in data
        assert "data" in data
        assert isinstance(data["data"], list)

        # Check that all data points are for USA
        for row in data["data"]:
            assert row["code"] == "USA" or row["entity"].lower() == "united states"


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

        # Just check that we get some content back (the Response object handling might be different)
        assert content.text is not None
