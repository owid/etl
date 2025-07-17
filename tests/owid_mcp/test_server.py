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

        assert data["data"][0] == {"code": "VUT", "entity": "Vanuatu", "value": 5.39073010664479, "year": 1961}


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


@pytest.mark.asyncio
async def test_cherry_blossom_search_and_sql():
    """Test searching for cherry blossom indicator and then getting data using run_sql."""
    async with Client(mcp) as client:
        # Search for cherry blossom indicator
        search_result = await client.call_tool(
            "search_indicator", {"query": "Day of the year with peak cherry blossom"}
        )
        assert search_result is not None
        assert search_result.structured_content is not None
        assert "result" in search_result.structured_content

        indicators = search_result.structured_content["result"]
        assert len(indicators) > 0

        # Get the first result
        indicator = indicators[0]
        assert "title" in indicator
        assert "metadata" in indicator
        assert "cherry" in indicator["title"].lower() or "blossom" in indicator["title"].lower()

        # Check that we have the catalog metadata
        metadata = indicator["metadata"]
        assert "sql_template" in metadata
        assert "parquet_url" in metadata
        assert "column" in metadata

        # Extract SQL template and modify it for a specific query
        sql_template = metadata["sql_template"]
        parquet_url = metadata["parquet_url"]
        column = metadata["column"]

        # Create a specific SQL query to get Japan data
        sql_query = (
            f"SELECT country, year, {column} FROM '{parquet_url}' WHERE country = 'Japan' ORDER BY year DESC LIMIT 10"
        )

        # Use run_sql to execute the query
        sql_result = await client.call_tool("run_sql", {"query": sql_query})
        assert sql_result is not None
        assert sql_result.structured_content is not None

        result_data = sql_result.structured_content
        assert "columns" in result_data
        assert "rows" in result_data
        assert "source" in result_data

        # Check that we got some data back
        assert len(result_data["columns"]) > 0
        assert len(result_data["rows"]) > 0

        # Check that the columns include what we expect
        columns = result_data["columns"]
        assert "country" in columns
        assert "year" in columns
        assert column in columns

        # Check that we got Japan data
        rows = result_data["rows"]
        for row in rows:
            country_idx = columns.index("country")
            assert row[country_idx] == "Japan"


@pytest.mark.asyncio
async def test_run_sql_basic():
    """Test basic run_sql functionality with a simple query."""
    async with Client(mcp) as client:
        # Test a simple query to get some variables
        sql_query = "SELECT id, name FROM variables WHERE name LIKE '%population%'"

        result = await client.call_tool("run_sql", {"query": sql_query, "max_rows": 5})
        assert result is not None
        assert result.structured_content is not None

        result_data = result.structured_content
        assert "columns" in result_data
        assert "rows" in result_data
        assert "source" in result_data

        # Check basic structure
        assert result_data["columns"] == ["id", "name"]
        assert len(result_data["rows"]) <= 5
        assert len(result_data["rows"]) > 0
