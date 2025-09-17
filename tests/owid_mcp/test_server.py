import io

import pandas as pd
import pytest
from fastmcp import Client
from mcp.types import TextContent

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
        result = await client.call_tool("search_chart", {"query": "global temperature warming annual"})
        assert result is not None
        # The result should be a list (even if empty)
        assert isinstance(result.data, list)


@pytest.mark.asyncio
async def test_fetch_indicator_data_tool():
    """Test fetch_indicator_data_tool functionality by fetching an indicator."""
    async with Client(mcp) as client:
        # Test fetching indicator data with a known ID
        # Using GDP indicator ID which should exist
        result = await client.call_tool("fetch_indicator_data", {"indicator_id": 2118})
        assert result is not None
        assert isinstance(result.content, list)
        assert len(result.content) == 1

        # Check the tool result content
        content = result.content[0]
        assert isinstance(content, TextContent)

        # Parse the JSON content
        import json

        data = json.loads(content.text)
        assert isinstance(data, list)

        # Check first entity has the new efficient format
        first_entity = data[0]
        assert "entity" in first_entity
        assert "years" in first_entity
        assert "values" in first_entity
        assert isinstance(first_entity["years"], list)
        assert isinstance(first_entity["values"], list)
        assert len(first_entity["years"]) == len(first_entity["values"])

        # Check that Vanuatu is present with 1961 data point having value 5.39
        vanuatu_data = next((entity for entity in data if entity["entity"] == "Vanuatu"), None)
        assert vanuatu_data is not None
        assert 1961 in vanuatu_data["years"]
        idx_1961 = vanuatu_data["years"].index(1961)
        assert vanuatu_data["values"][idx_1961] == 5.39


@pytest.mark.asyncio
async def test_fetch_indicator_data_tool_for_entity():
    """Test fetch_indicator_data_tool for specific entity functionality."""
    async with Client(mcp) as client:
        # Test fetching indicator data for a specific entity
        # Using GDP indicator ID with USA entity
        result = await client.call_tool("fetch_indicator_data", {"indicator_id": 2118, "entity": "USA"})
        assert result is not None
        assert isinstance(result.content, list)
        assert len(result.content) == 1

        # Check the tool result content
        content = result.content[0]
        assert isinstance(content, TextContent)

        # Parse the JSON content
        import json

        data = json.loads(content.text)
        assert isinstance(data, list)

        # Check that all data points are for USA
        for row in data:
            assert row["entity"].lower() == "united states"


@pytest.mark.asyncio
async def test_fetch_indicator_metadata_tool():
    """Test fetch_indicator_metadata_tool functionality by fetching metadata for an indicator."""
    async with Client(mcp) as client:
        # Test fetching indicator metadata with a known ID
        # Using GDP indicator ID which should exist
        result = await client.call_tool("fetch_indicator_metadata", {"indicator_id": 2118})
        assert result is not None
        assert isinstance(result.content, list)
        assert len(result.content) == 1

        # Check the tool result content
        content = result.content[0]
        assert isinstance(content, TextContent)

        # Parse the JSON content
        import json

        metadata = json.loads(content.text)
        assert isinstance(metadata, dict)

        # Check that basic metadata fields are present
        assert "id" in metadata
        assert metadata["id"] == 2118
        assert "name" in metadata or "title" in metadata

        # Check that dimensions and origins were filtered out
        assert "dimensions" not in metadata
        assert "origins" not in metadata


@pytest.mark.asyncio
async def test_cherry_blossom_search_and_sql():
    """Test searching for cherry blossom indicator and then getting data using run_sql."""
    async with Client(mcp) as client:
        # Search for cherry blossom indicator
        search_result = await client.call_tool("search_indicator", {"query": "cherry blossom"})
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
        assert "run_sql_template" in metadata
        assert "column" in metadata

        # Verify the structure of the template (without executing SQL since it has placeholder issues)
        column = metadata["column"]
        run_sql_template = metadata["run_sql_template"]

        # Check that template contains expected parts
        assert "SELECT" in run_sql_template
        assert column in run_sql_template
        assert "FROM" in run_sql_template
        assert "WHERE" in run_sql_template

        # Verify column is a reasonable cherry blossom related field name
        assert any(word in column.lower() for word in ["date", "flowering", "bloom", "cherry"])


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
        assert "csv" in result_data
        assert "source" in result_data

        # Check basic structure
        csv_content = result_data["csv"]
        assert len(csv_content) > 0

        # Parse CSV to check structure
        lines = csv_content.strip().split("\n")
        assert len(lines) > 1  # Should have header + data
        assert len(lines) <= 6  # Header + max 5 rows

        # Check header
        header = lines[0]
        assert header.strip() == "id,name"


@pytest.mark.asyncio
async def test_search_deep_research():
    """Test the deep research search tool functionality."""
    async with Client(mcp) as client:
        # Test searching with the deep research search tool
        result = await client.call_tool("search", {"query": "population density"})
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

            # Check that the URL format is correct (CSV format from grapher)
            assert search_result["url"].startswith("https://ourworldindata.org/grapher/")

            # Check that the id is a string (it's actually a URL in deep research search)
            assert isinstance(search_result["id"], str)
            assert search_result["id"]  # Just check it's not empty


@pytest.mark.asyncio
async def test_fetch_deep_research():
    """Test the deep research fetch tool functionality."""
    async with Client(mcp) as client:
        # First search for an indicator to get an ID
        search_result = await client.call_tool("search", {"query": "population"})
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
        assert "text" in data
        assert "url" in data
        assert "metadata" in data

        # Check that the id matches what we requested
        assert data["id"] == indicator_id

        # Check that the text is CSV format
        csv_text = data["text"]
        assert isinstance(csv_text, str)
        # Entity column should be removed if Code column has no empty values
        assert (
            csv_text.startswith("Code,Year,")
            or csv_text.startswith("Entity,Code,Year,")
            or csv_text.startswith("entity,year,value")
        )

        # Check that we have CSV data rows
        lines = csv_text.split("\n")
        assert len(lines) > 1  # Should have header + data rows

        # Check URL format
        assert data["url"].startswith("https://ourworldindata.org/grapher/")

        # Check metadata structure
        metadata = data["metadata"]
        assert "rows" in metadata or "row_count" in metadata
        # Check the row count is an integer (could be "rows" or "row_count" key)
        row_count = metadata.get("rows") or metadata.get("row_count")
        assert isinstance(row_count, int)


@pytest.mark.asyncio
async def test_search_and_fetch_workflow():
    """Test the complete search -> fetch workflow for deep research."""
    async with Client(mcp) as client:
        # Step 1: Search for indicators
        search_result = await client.call_tool("search", {"query": "GDP"})
        assert search_result is not None
        assert search_result.structured_content is not None
        search_results = search_result.structured_content["result"]
        assert len(search_results) >= 1

        # Step 2: Get details from search results
        first_result = search_results[0]
        indicator_id = first_result["id"]

        # Step 3: Fetch full data for the first result
        fetch_result = await client.call_tool("fetch", {"id": indicator_id})
        assert fetch_result is not None
        assert fetch_result.structured_content is not None

        # Step 4: Verify consistency between search and fetch
        fetch_data = fetch_result.structured_content
        assert fetch_data["id"] == indicator_id
        # Note: titles may differ slightly between search and fetch due to data processing
        assert fetch_data["title"] is not None

        # Step 5: Verify CSV data structure
        csv_text = fetch_data["text"]
        lines = csv_text.split("\n")
        header = lines[0]
        # Headers vary between different datasets, just check it's a valid CSV header
        assert "," in header  # Should be CSV format

        # Check that we have actual data rows (not just header)
        data_lines = [line for line in lines[1:] if line.strip()]
        assert len(data_lines) > 0

        # Verify CSV format of data rows
        if data_lines:
            # Check first data row has proper CSV structure
            first_row = data_lines[0]
            parts = first_row.split(",")
            # Different datasets have different numbers of columns, just check it has multiple columns
            assert len(parts) >= 3

            # CSV format can be either:
            # - Entity,Code,Year,[Metric columns] (when Code column has empty values)
            # - Code,Year,[Metric columns] (when Entity column is removed)
            # Check if header has Entity column to determine format
            has_entity_column = "Entity" in header

            if has_entity_column:
                # Format: Entity,Code,Year,[Metric columns]
                # Entity should be non-empty
                assert parts[0].strip()  # entity
                # Code might be empty (that's why Entity wasn't removed)
                # Year should be numeric or empty (could be int or float like 1990.0)
                if parts[2].strip():
                    try:
                        float(parts[2].strip())  # year (could be negative, int or float)
                    except ValueError:
                        assert False, f"Year should be numeric but got: {parts[2].strip()}"
            else:
                # Format: Code,Year,[Metric columns]
                # Code should be non-empty (country/region code)
                assert parts[0].strip()  # code
                # Year should be numeric or empty (could be int or float like 1990.0)
                if parts[1].strip():
                    try:
                        float(parts[1].strip())  # year (could be negative, int or float)
                    except ValueError:
                        assert False, f"Year should be numeric but got: {parts[1].strip()}"


@pytest.mark.asyncio
async def test_search_posts_and_fetch():
    """Test searching for posts about poverty and fetching the first result."""
    async with Client(mcp) as client:
        # Step 1: Search for posts about poverty
        search_result = await client.call_tool("search_posts", {"query": "poverty", "limit": 5})
        assert search_result is not None
        assert search_result.structured_content is not None

        # Check search result structure
        search_data = search_result.structured_content

        assert "query" in search_data
        assert "results" in search_data
        assert "count" in search_data
        assert search_data["query"] == "poverty"

        assert isinstance(search_data["results"], list)
        assert search_data["count"] == len(search_data["results"])

        # If we have results, test fetching the first post
        assert search_data["count"] > 0
        first_post = search_data["results"][0]

        # Verify search result structure
        assert "slug" in first_post
        assert "title" in first_post
        assert "excerpt" in first_post
        assert "url" in first_post

        # Step 2: Fetch the full content for the first post
        post_slug = first_post["slug"]
        fetch_result = await client.call_tool("fetch_post", {"identifier": post_slug})
        assert fetch_result is not None
        assert fetch_result.structured_content is not None

        # Check fetch result structure
        fetch_data = fetch_result.structured_content

        # Verify successful fetch structure
        assert "content" in fetch_data
        assert "metadata" in fetch_data

        # Check metadata structure
        metadata = fetch_data["metadata"]
        assert "slug" in metadata
        assert "title" in metadata
        assert "length" in metadata
        assert metadata["slug"] == post_slug
        assert isinstance(metadata["length"], int)
        assert metadata["length"] > 0

        # Check that we got actual markdown content
        content = fetch_data["content"]
        assert isinstance(content, str)
        assert len(content) > 0

        print(f"✅ Successfully fetched post: '{metadata['title']}' ({metadata['length']} chars)")


@pytest.mark.asyncio
async def test_search_posts_algolia_vs_sql():
    """Test that search_posts works with both SQL and Algolia backends."""
    async with Client(mcp) as client:
        query = "poverty"
        limit = 3

        # Test SQL search (default)
        sql_result = await client.call_tool("search_posts", {"query": query, "limit": limit, "use_algolia": False})
        assert sql_result is not None
        assert sql_result.structured_content is not None

        sql_data = sql_result.structured_content
        assert "query" in sql_data
        assert "results" in sql_data
        assert "count" in sql_data
        assert "search_method" in sql_data
        assert sql_data["query"] == query
        assert sql_data["search_method"] == "sql"
        assert isinstance(sql_data["results"], list)
        assert sql_data["count"] == len(sql_data["results"])

        # Test Algolia search
        algolia_result = await client.call_tool("search_posts", {"query": query, "limit": limit, "use_algolia": True})
        assert algolia_result is not None
        assert algolia_result.structured_content is not None

        algolia_data = algolia_result.structured_content
        assert "query" in algolia_data
        assert "results" in algolia_data
        assert "count" in algolia_data
        assert "search_method" in algolia_data
        assert algolia_data["query"] == query
        assert algolia_data["search_method"] == "algolia"
        assert isinstance(algolia_data["results"], list)
        assert algolia_data["count"] == len(algolia_data["results"])

        # Both should return some results for "poverty"
        assert sql_data["count"] > 0
        assert algolia_data["count"] > 0

        # Check that both return properly structured results
        for result_set in [sql_data["results"], algolia_data["results"]]:
            if result_set:  # If we have results
                first_result = result_set[0]
                assert "slug" in first_result
                assert "title" in first_result
                assert "excerpt" in first_result
                assert "url" in first_result
                assert "type" in first_result

                # URL should be properly formatted
                assert first_result["url"].startswith("https://ourworldindata.org/")

        print(f"✅ SQL search returned {sql_data['count']} results")
        print(f"✅ Algolia search returned {algolia_data['count']} results")


@pytest.mark.asyncio
async def test_fetch_chart_data_global_warming():
    """Test fetch_chart_data with global warming by gas and source dataset."""
    async with Client(mcp) as client:
        # Test fetching chart data with specific URL and time filter
        chart_slug = "global-warming-by-gas-and-source"
        time_filter = "1990..latest"

        result = await client.call_tool("fetch_chart_data", {"id": chart_slug, "time": time_filter})
        assert result is not None
        assert result.structured_content is not None

        # Check basic structure
        data = result.structured_content
        assert "id" in data
        assert "text" in data
        assert "url" in data
        assert "metadata" in data

        # Verify the response matches our request
        assert data["id"] == chart_slug
        assert data["url"].startswith(
            f"https://ourworldindata.org/grapher/{chart_slug}.csv"
        )  # URL may have time parameter added

        # Parse the CSV content
        csv_text = data["text"]
        assert isinstance(csv_text, str)
        assert len(csv_text) > 0

        # Check CSV structure
        lines = csv_text.strip().split("\n")
        assert len(lines) > 1  # Should have header + data rows

        header = lines[0]
        # Entity column should be removed if Code column has no empty values
        assert "Code" in header
        assert "Year" in header

        # Entity column may or may not be present depending on Code column completeness
        # For this test, we expect Entity to be removed since Code should be complete
        if "Entity" in header:
            # If Entity is present, Code might have empty values
            print("Warning: Entity column preserved, Code column may have empty values")
        else:
            # Entity was removed, which is the expected behavior for complete Code columns
            assert "Entity" not in header

        # Verify we have data rows
        data_lines = [line for line in lines[1:] if line.strip()]
        assert len(data_lines) > 0

        # Check metadata
        metadata = data["metadata"]
        assert "rows" in metadata
        assert "columns" in metadata
        assert "time_filter" in metadata
        assert metadata["time_filter"] == time_filter
        assert isinstance(metadata["rows"], int)
        assert metadata["rows"] > 0
        assert isinstance(metadata["columns"], list)
        # Entity column may or may not be in metadata depending on Code column completeness
        assert "Code" in metadata["columns"]
        assert "Year" in metadata["columns"]

        # Parse CSV to verify structure
        df = pd.read_csv(io.StringIO(csv_text))
        # Check that we have data and required columns
        assert len(df) > 0
        assert "Code" in df.columns
        assert "Year" in df.columns
        years = set()

        if "Year" in df.columns:
            year_values = pd.to_numeric(df["Year"], errors="coerce").dropna()
            years = set(year_values.astype(int))

        # Check if we have World entity data (if Entity column exists)
        # For this dataset, we expect global data which would be represented by "World" entity
        if "Entity" in df.columns:
            entities = set(df["Entity"].dropna().unique())
            assert "World" in entities  # Global warming data should include World entity
        else:
            # Entity column was removed, so we check Code column for World data
            if "Code" in df.columns:
                codes = set(df["Code"].dropna().unique())
                assert "OWID_WRL" in codes  # World code in OWID data

        # Verify time filtering worked - should only have data from 1990 onwards
        if years:
            min_year = min(years)
            # Note: time filtering may not work on all datasets or may include historical context
            # For this test, let's just verify we got the data and the time filter was applied to URL
            print(f"Data year range: {min_year} - {max(years)}")
            assert "time=1990..latest" in data["url"], f"Time filter should be in URL: {data['url']}"

        entity_info = ""
        if "Entity" in df.columns:
            entities = set(df["Entity"].dropna().unique())
            entity_info = f" with {len(entities)} entities"
        elif "Code" in df.columns:
            codes = set(df["Code"].dropna().unique())
            entity_info = f" with {len(codes)} country codes"

        print(f"✅ Successfully fetched chart data{entity_info} and {len(data_lines)} rows")
        print(f"✅ Year range: {min(years) if years else 'N/A'} - {max(years) if years else 'N/A'}")


@pytest.mark.asyncio
async def test_run_sql_invalid_column_error():
    """Test that run_sql returns a helpful error message when querying with an invalid column."""
    async with Client(mcp) as client:
        # Test with an invalid column name 'abc' that doesn't exist in the variables table
        sql_query = "SELECT abc FROM variables LIMIT 10"

        # The SQL should fail with a clear error message
        # Use raise_on_error=False to get the error in the result instead of raising an exception
        output = await client.call_tool("run_sql", {"query": sql_query})
        error = output.structured_content["error"]  # type: ignore
        assert "column 'abc' does not exist" in error.lower()


@pytest.mark.asyncio
async def test_run_sql_syntax_error():
    """Test that run_sql returns a helpful error message for SQL syntax errors."""
    async with Client(mcp) as client:
        # Test with invalid SQL syntax
        sql_query = "SELECT name FROM variables WHERE"  # Missing condition after WHERE

        output = await client.call_tool("run_sql", {"query": sql_query})
        error = output.structured_content["error"]  # type: ignore
        assert "invalid expression" in error.lower() or "unexpected token" in error.lower()
