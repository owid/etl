"""Tests for the ChatGPT App MCP server."""
# pyright: reportAttributeAccessIssue=false

import mcp.types as types
import pytest

from chatgpt_app.server import _call_tool_request, _list_tools


@pytest.mark.asyncio
async def test_list_tools():
    """Test that the server lists the search-charts tool."""
    tools = await _list_tools()  # type: ignore
    assert len(tools) == 1
    assert tools[0].name == "search-charts"
    assert tools[0].inputSchema["required"] == ["query"]


@pytest.mark.asyncio
async def test_call_tool_search_charts():
    """Test calling the search-charts tool returns widget with chart data."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="search-charts",
            arguments={"query": "population density"},
        ),
    )
    result = await _call_tool_request(request)
    assert not result.root.isError
    assert "Found" in result.root.content[0].text

    # Check structured content for widget
    assert result.root.structuredContent is not None
    assert "title" in result.root.structuredContent
    assert "chartUrl" in result.root.structuredContent

    # Check widget metadata
    assert result.root.meta is not None
    assert "openai.com/widget" in result.root.meta
