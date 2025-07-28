import base64

import pytest
from fastmcp import Client

from mcp.types import TextResourceContents
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

        if isinstance(content, TextResourceContents):
            data = json.loads(content.text)
        else:
            data = json.loads(base64.b64decode(content.blob).decode("utf-8"))
        assert "metadata" in data
        assert "data" in data
        assert isinstance(data["data"], list)

        assert data["data"][0] == {"entity": "Vanuatu", "value": 5.39, "year": 1961}


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

        if isinstance(content, TextResourceContents):
            data = json.loads(content.text)
        else:
            data = json.loads(base64.b64decode(content.blob).decode("utf-8"))
        assert "metadata" in data
        assert "data" in data
        assert isinstance(data["data"], list)

        # Check that all data points are for USA
        for row in data["data"]:
            assert row["entity"].lower() == "united states"


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
        if isinstance(content, TextResourceContents):
            assert content.text is not None
        else:
            assert content.blob is not None


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

        # Extract parquet URL and column info
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


@pytest.mark.asyncio
async def test_search_deep_research():
    """Test the deep research search tool functionality."""
    async with Client(mcp) as client:
        # Test searching with the deep research search tool
        result = await client.call_tool("search", {"query": "population density", "limit": 3})
        assert result is not None
        assert result.structured_content is not None
        assert "result" in result.structured_content

        search_results = result.structured_content["result"]
        assert isinstance(search_results, list)

        # Check that we get results in the correct format
        if len(search_results) > 0:
            search_result = search_results[0]
            assert "id" in search_result
            assert "title" in search_result
            assert "text" in search_result
            assert "url" in search_result

            # Check that the URL format is correct
            assert search_result["url"].startswith("https://ourworldindata.org/charts?variable=")

            # Check that the id is a string
            assert isinstance(search_result["id"], str)
            assert search_result["id"].isdigit()


@pytest.mark.asyncio
async def test_fetch_deep_research():
    """Test the deep research fetch tool functionality."""
    async with Client(mcp) as client:
        # First search for an indicator to get an ID
        search_result = await client.call_tool("search", {"query": "population", "limit": 1})
        assert search_result is not None
        assert search_result.structured_content is not None
        search_results = search_result.structured_content["result"]
        assert len(search_results) > 0

        # Get the first indicator ID
        indicator_id = search_results[0]["id"]

        # Test fetching with the deep research fetch tool
        fetch_result = await client.call_tool("fetch", {"id": indicator_id})
        assert fetch_result is not None
        assert fetch_result.structured_content is not None

        # Check that we get results in the correct format
        data = fetch_result.structured_content
        assert "id" in data
        assert "title" in data
        assert "text" in data
        assert "url" in data
        assert "metadata" in data

        # Check that the id matches what we requested
        assert data["id"] == indicator_id

        # Check that the text is CSV format
        csv_text = data["text"]
        assert isinstance(csv_text, str)
        assert csv_text.startswith("entity,year,value")

        # Check that we have CSV data rows
        lines = csv_text.split("\n")
        assert len(lines) > 1  # Should have header + data rows

        # Check URL format
        assert data["url"].startswith("https://ourworldindata.org/charts?variable=")

        # Check metadata structure
        metadata = data["metadata"]
        assert "row_count" in metadata
        assert isinstance(metadata["row_count"], int)


@pytest.mark.asyncio
async def test_search_and_fetch_workflow():
    """Test the complete search -> fetch workflow for deep research."""
    async with Client(mcp) as client:
        # Step 1: Search for indicators
        search_result = await client.call_tool("search", {"query": "GDP", "limit": 2})
        assert search_result is not None
        assert search_result.structured_content is not None
        search_results = search_result.structured_content["result"]
        assert len(search_results) >= 1

        # Step 2: Get details from search results
        first_result = search_results[0]
        indicator_id = first_result["id"]
        search_title = first_result["title"]

        # Step 3: Fetch full data for the first result
        fetch_result = await client.call_tool("fetch", {"id": indicator_id})
        assert fetch_result is not None
        assert fetch_result.structured_content is not None

        # Step 4: Verify consistency between search and fetch
        fetch_data = fetch_result.structured_content
        assert fetch_data["id"] == indicator_id
        assert fetch_data["title"] == search_title

        # Step 5: Verify CSV data structure
        csv_text = fetch_data["text"]
        lines = csv_text.split("\n")
        header = lines[0]
        assert header == "entity,year,value"

        # Check that we have actual data rows (not just header)
        data_lines = [line for line in lines[1:] if line.strip()]
        assert len(data_lines) > 0

        # Verify CSV format of data rows
        if data_lines:
            # Check first data row has proper CSV structure
            first_row = data_lines[0]
            parts = first_row.split(",")
            assert len(parts) == 3  # entity, year, value

            # Entity should be quoted, year and value should be numeric-ish
            assert parts[0].startswith('"') and parts[0].endswith('"')
            assert parts[1].strip().isdigit()  # year
            # Value might be float, so just check it's not empty
            assert parts[2].strip()  # value
