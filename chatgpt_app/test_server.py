"""Tests for the ChatGPT App MCP server."""

import pytest
import mcp.types as types

from chatgpt_app.server import (
    _call_tool_request,
    _list_tools,
    _list_resources,
    _list_resource_templates,
    _handle_read_resource,
)


@pytest.mark.asyncio
async def test_list_tools():
    """Test that the server lists the search-charts tool."""
    tools = await _list_tools()
    assert len(tools) == 1
    assert tools[0].name == "search-charts"
    assert tools[0].title == "Search OWID Charts"
    assert "interactive iframes" in tools[0].description
    assert tools[0].inputSchema["required"] == ["query"]


@pytest.mark.asyncio
async def test_list_resources():
    """Test that the server lists the widget resource."""
    resources = await _list_resources()
    assert len(resources) == 1
    assert resources[0].name == "OWID Chart Viewer"
    assert str(resources[0].uri) == "ui://widget/chart-viewer.html"
    assert resources[0].mimeType == "text/html+skybridge"


@pytest.mark.asyncio
async def test_list_resource_templates():
    """Test that the server lists resource templates."""
    templates = await _list_resource_templates()
    assert len(templates) == 1
    assert templates[0].name == "OWID Chart Viewer"
    assert templates[0].uriTemplate == "ui://widget/chart-viewer.html"


@pytest.mark.asyncio
async def test_handle_read_resource():
    """Test reading the widget resource."""
    request = types.ReadResourceRequest(
        method="resources/read",
        params=types.ReadResourceRequestParams(
            uri="ui://widget/chart-viewer.html"
        ),
    )
    result = await _handle_read_resource(request)
    assert isinstance(result, types.ServerResult)
    assert len(result.root.contents) == 1
    content = result.root.contents[0]
    assert content.mimeType == "text/html+skybridge"
    assert "<!doctype html>" in content.text
    assert "chart-iframe" in content.text


@pytest.mark.asyncio
async def test_handle_read_resource_unknown_uri():
    """Test reading an unknown resource returns error."""
    request = types.ReadResourceRequest(
        method="resources/read",
        params=types.ReadResourceRequestParams(uri="unknown://resource"),
    )
    result = await _handle_read_resource(request)
    assert isinstance(result, types.ServerResult)
    assert "error" in result.root.meta


@pytest.mark.asyncio
async def test_call_tool_search_charts():
    """Test calling the search-charts tool."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="search-charts",
            arguments={"query": "population density"},
        ),
    )
    result = await _call_tool_request(request)
    assert isinstance(result, types.ServerResult)
    assert not result.root.isError
    assert len(result.root.content) == 1

    # Check text content
    text_content = result.root.content[0]
    assert text_content.type == "text"
    assert "Found" in text_content.text
    assert "chart(s)" in text_content.text

    # Check structured content
    assert result.root.structuredContent is not None
    assert "title" in result.root.structuredContent
    assert "chartUrl" in result.root.structuredContent
    assert "slug" in result.root.structuredContent

    # Check widget metadata
    assert result.root.meta is not None
    assert "openai.com/widget" in result.root.meta
    assert "openai/widgetAccessible" in result.root.meta
    assert result.root.meta["openai/widgetAccessible"] is True


@pytest.mark.asyncio
async def test_call_tool_search_charts_no_results():
    """Test search-charts with query that returns no results."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="search-charts",
            arguments={"query": "xyznonexistentquery12345"},
        ),
    )
    result = await _call_tool_request(request)
    assert isinstance(result, types.ServerResult)
    assert not result.root.isError
    assert "No charts found" in result.root.content[0].text


@pytest.mark.asyncio
async def test_call_tool_unknown_tool():
    """Test calling an unknown tool returns error."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="unknown-tool",
            arguments={},
        ),
    )
    result = await _call_tool_request(request)
    assert isinstance(result, types.ServerResult)
    assert result.root.isError
    assert "Unknown tool" in result.root.content[0].text


@pytest.mark.asyncio
async def test_call_tool_missing_arguments():
    """Test calling tool with missing required arguments."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="search-charts",
            arguments={},
        ),
    )
    result = await _call_tool_request(request)
    assert isinstance(result, types.ServerResult)
    assert result.root.isError
    assert "validation error" in result.root.content[0].text.lower()


@pytest.mark.asyncio
async def test_call_tool_invalid_arguments():
    """Test calling tool with invalid argument types."""
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(
            name="search-charts",
            arguments={"query": 123},  # Should be string
        ),
    )
    result = await _call_tool_request(request)
    assert isinstance(result, types.ServerResult)
    assert result.root.isError
