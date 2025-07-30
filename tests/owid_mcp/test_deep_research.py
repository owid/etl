import pytest
from fastmcp import Client

from owid_mcp.server import mcp


@pytest.mark.asyncio
async def test_fetch_chart_with_specific_id():
    """Test the fetch tool with the specific chart ID from the error."""
    async with Client(mcp) as client:
        # Test fetching with the specific chart ID that was failing
        chart_url = "https://ourworldindata.org/grapher/population-density.csv?tab=line&csvType=filtered&country=FRA"

        result = await client.call_tool("fetch", {"id": chart_url})

        assert result is not None
        assert result.structured_content is not None

        # Check that we get results in the correct format
        data = result.structured_content
        assert "id" in data
        assert "title" in data
        assert "text" in data
        assert "url" in data
        assert "metadata" in data

        # Check that the id matches what we requested
        assert data["id"] == chart_url

        # Check that the text contains CSV data
        csv_text = data["text"]
        assert isinstance(csv_text, str)
        assert len(csv_text) > 0

        # Should contain CSV headers
        lines = csv_text.split("\n")
        assert len(lines) > 1  # Should have header + data rows

        # Check that the URL matches
        assert data["url"] == chart_url

        # Check metadata structure
        metadata = data["metadata"]
        assert "mime" in metadata
        assert "size_bytes" in metadata
        assert "rows" in metadata
        assert "columns" in metadata
        assert metadata["mime"] == "text/csv"
        assert isinstance(metadata["size_bytes"], int)
        assert isinstance(metadata["rows"], int)
        assert isinstance(metadata["columns"], list)


@pytest.mark.asyncio
async def test_fetch_chart_data_directly():
    """Test calling the charts.fetch_chart_data function directly."""
    from owid_mcp import charts

    chart_url = "https://ourworldindata.org/grapher/population-density.csv?tab=line&csvType=filtered&country=FRA"

    # This should test the actual function that's failing
    result = await charts._fetch_chart_data_internal(chart_url)

    assert result is not None
    assert hasattr(result, "id")
    assert hasattr(result, "title")
    assert hasattr(result, "text")
    assert hasattr(result, "url")
    assert hasattr(result, "metadata")

    assert result.id == chart_url
    assert isinstance(result.text, str)
    assert len(result.text) > 0


@pytest.mark.asyncio
async def test_search_and_fetch_workflow_population_density():
    """Test the complete search -> fetch workflow for population density."""
    async with Client(mcp) as client:
        # Step 1: Search for population density charts
        search_result = await client.call_tool("search", {"query": "population density france"})
        assert search_result is not None
        assert search_result.structured_content is not None

        search_results = search_result.structured_content["result"]
        assert len(search_results) >= 1

        # Step 2: Get the first result
        first_result = search_results[0]
        chart_url = first_result["id"]

        # Step 3: Fetch the chart data
        fetch_result = await client.call_tool("fetch", {"id": chart_url})
        assert fetch_result is not None
        assert fetch_result.structured_content is not None

        # Step 4: Verify the data structure
        fetch_data = fetch_result.structured_content
        assert fetch_data["id"] == chart_url

        # Check CSV structure
        csv_text = fetch_data["text"]
        lines = csv_text.split("\n")
        assert len(lines) > 1

        # Should have proper CSV format
        header = lines[0]
        assert "Code" in header or "Year" in header  # Should have basic CSV structure
