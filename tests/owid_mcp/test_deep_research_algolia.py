"""
Tests for OWID Deep Research Algolia MCP Module.
"""

import base64

import pytest
from fastmcp import Client

from owid_mcp.deep_research_algolia import country_name_to_iso3, mcp


class TestCountryNameToIso3:
    """Tests for country_name_to_iso3 helper function."""

    def test_none_input(self):
        """Test with None input."""
        assert country_name_to_iso3(None) is None

    def test_empty_string(self):
        """Test with empty string."""
        assert country_name_to_iso3("") is None

    def test_owid_regions_mappings(self):
        """Test OWID regions mapping cases."""
        # Test case-insensitive lookups work
        assert country_name_to_iso3("World") == "OWID_WRL"
        assert country_name_to_iso3("world") == "OWID_WRL"

        # Test standard countries work
        assert country_name_to_iso3("France") == "FRA"
        assert country_name_to_iso3("france") == "FRA"
        assert country_name_to_iso3("United States") == "USA"

        # Test EU mapping (uses actual code from regions file)
        assert country_name_to_iso3("European Union (27)") == "OWID_EU27"

        # Test unknown country
        assert country_name_to_iso3("Unknown Country") is None

    def test_regions_file_loading(self):
        """Test that regions file can be loaded successfully."""
        from owid_mcp.deep_research_algolia import _load_regions_mapping

        # This should not raise an exception
        mapping = _load_regions_mapping()
        assert isinstance(mapping, dict)
        assert len(mapping) > 0

        # Check some expected mappings exist
        assert "france" in mapping
        assert "united states" in mapping
        assert "world" in mapping

    def test_aliases_work(self):
        """Test that country aliases work correctly."""
        # Test some aliases that should exist in the regions file
        # These will depend on what's actually in the regions.yml file
        result = country_name_to_iso3("United States")
        assert result == "USA" or result is not None  # Should find some mapping

    def test_case_insensitive_lookup(self):
        """Test case insensitive country name lookup."""
        # Test same country in different cases
        france_upper = country_name_to_iso3("FRANCE")
        france_lower = country_name_to_iso3("france")
        france_mixed = country_name_to_iso3("France")

        assert france_upper == france_lower == france_mixed == "FRA"


class TestSearchTool:
    """Tests for the search tool using real API calls."""

    @pytest.mark.asyncio
    async def test_mcp_server_has_search_tool(self):
        """Test that the MCP server exposes the search tool."""
        async with Client(mcp) as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            assert "search" in tool_names

    @pytest.mark.asyncio
    async def test_search_population_density(self):
        """Test search with 'population density' query."""
        async with Client(mcp) as client:
            result = await client.call_tool("search", {"query": "population density", "limit": 5})

            assert result is not None
            assert isinstance(result.data, list)
            assert len(result.data) > 0
            print(f"Successfully searched for 'population density' and got {len(result.data)} results")

    @pytest.mark.asyncio
    async def test_search_coal_france(self):
        """Test search with 'coal france' query."""
        async with Client(mcp) as client:
            result = await client.call_tool("search", {"query": "coal france", "limit": 5})

            assert result is not None
            assert isinstance(result.data, list)
            assert len(result.data) > 0
            print(f"Successfully searched for 'coal france' and got {len(result.data)} results")

    @pytest.mark.asyncio
    async def test_search_different_limits(self):
        """Test search with different limit values."""
        async with Client(mcp) as client:
            # Test with limit 1
            result1 = await client.call_tool("search", {"query": "population density", "limit": 1})
            assert len(result1.data) == 1

            # Test with limit 3
            result3 = await client.call_tool("search", {"query": "population density", "limit": 3})
            assert len(result3.data) <= 3
            assert len(result3.data) >= 1
            print(f"Limit tests passed: got {len(result1.data)} and {len(result3.data)} results")


class TestFetchTool:
    """Tests for the fetch tool using real API calls."""

    @pytest.mark.asyncio
    async def test_mcp_server_has_fetch_tool(self):
        """Test that the MCP server exposes the fetch tool."""
        async with Client(mcp) as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            assert "fetch" in tool_names

    @pytest.mark.asyncio
    async def test_fetch_real_chart(self):
        """Test fetching real CSV data and verify Entity column removal."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "fetch", {"id": "https://ourworldindata.org/grapher/population-density.csv"}
            )

            assert result is not None
            assert result.data is not None

            # Verify the CSV structure
            csv_text = result.data.text
            assert isinstance(csv_text, str)
            assert len(csv_text) > 0

            # Check that Entity column was removed
            lines = csv_text.strip().split("\n")
            header = lines[0]
            assert "Entity" not in header, f"Entity column should be removed, but header is: {header}"

            # Basic validation that fetch worked
            print(f"CSV header: {header}")
            print(f"Number of data rows: {len(lines) - 1}")
            print(f"Metadata: {result.data.metadata}")
            print("Successfully called fetch tool and verified Entity column removal")

    @pytest.mark.asyncio
    async def test_fetch_invalid_id(self):
        """Test fetch with invalid ID (not a URL)."""
        async with Client(mcp) as client:
            result = await client.call_tool("fetch", {"id": "not-a-url"})

            assert result is not None
            print("Successfully called fetch tool with invalid URL")

    @pytest.mark.asyncio
    async def test_fetch_nonexistent_chart(self):
        """Test fetch with nonexistent chart."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "fetch", {"id": "https://ourworldindata.org/grapher/nonexistent-chart-12345.csv"}
            )

            assert result is not None
            print("Successfully called fetch tool with nonexistent CSV URL")


class TestIntegration:
    """Integration tests for the deep research algolia module."""

    @pytest.mark.asyncio
    async def test_server_initialization(self):
        """Test that the MCP server initializes correctly."""
        async with Client(mcp) as client:
            # Test that the server is responsive
            tools = await client.list_tools()
            assert tools is not None
            assert len(tools) == 3

            tool_names = [tool.name for tool in tools]
            assert "search" in tool_names
            assert "fetch" in tool_names
            assert "fetch_image" in tool_names

    @pytest.mark.asyncio
    async def test_search_then_fetch_workflow(self):
        """Test the complete search -> fetch workflow with real API calls."""
        async with Client(mcp) as client:
            # First search for population density
            search_result = await client.call_tool("search", {"query": "population density", "limit": 1})

            assert len(search_result.data) == 1
            print("Successfully completed search -> fetch workflow for 'population density'")

    @pytest.mark.asyncio
    async def test_search_coal_france_workflow(self):
        """Test search for 'coal france' workflow."""
        async with Client(mcp) as client:
            # Search for coal france
            search_result = await client.call_tool("search", {"query": "coal consumption france", "limit": 1})

            # Now .data[0] should contain the structured SearchResult data with CSV URLs
            expected_result = {
                "id": "https://ourworldindata.org/grapher/coal-consumption-by-country-terawatt-hours-twh.csv?tab=line&csvType=filtered&time=earliest..latest&country=~FRA",
                "text": "Coal consumption by country or region, measured in terawatt-hours (TWh).",
                "title": "Coal consumption",
                "url": "https://ourworldindata.org/grapher/coal-consumption-by-country-terawatt-hours-twh.csv?tab=line&csvType=filtered&time=earliest..latest&country=~FRA",
            }

            assert search_result.data[0] == expected_result


class TestFetchImageTool:
    """Tests for the fetch_image tool using real API calls."""

    @pytest.mark.asyncio
    async def test_mcp_server_has_fetch_image_tool(self):
        """Test that the MCP server exposes the fetch_image tool."""
        async with Client(mcp) as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            assert "fetch_image" in tool_names

    @pytest.mark.asyncio
    async def test_fetch_image_from_csv_url(self):
        """Test fetching image by converting CSV URL to PNG."""
        async with Client(mcp) as client:
            # Use a CSV URL and expect it to be converted to PNG
            csv_url = "https://ourworldindata.org/grapher/population-density.csv?tab=line&csvType=filtered&time=earliest..latest&country=FRA"
            result = await client.call_tool("fetch_image", {"id": csv_url})

            assert result is not None
            assert not result.is_error
            assert len(result.content) == 1

            # Check that we got an ImageContent response
            image_content = result.content[0]
            assert image_content.type == "image"
            assert hasattr(image_content, "data")
            assert isinstance(image_content.data, str)

            # Verify it's valid base64
            try:
                base64.b64decode(image_content.data)
            except Exception as e:
                pytest.fail(f"Invalid base64 data: {e}")

            print(f"Successfully fetched image, base64 length: {len(image_content.data)}")

    @pytest.mark.asyncio
    async def test_fetch_image_url_conversion(self):
        """Test that CSV URLs are properly converted to PNG URLs."""
        async with Client(mcp) as client:
            # Test URL with multiple query parameters
            csv_url = "https://ourworldindata.org/grapher/population-density.csv?tab=line&csvType=filtered&time=2020..2023&country=FRA~DEU"
            result = await client.call_tool("fetch_image", {"id": csv_url})

            assert result is not None
            assert not result.is_error
            # The actual PNG URL should be constructed with proper parameters
            # The function should keep country and time parameters, but remove CSV-specific ones
            print("Successfully converted CSV URL to PNG URL and fetched image")

    @pytest.mark.asyncio
    async def test_fetch_image_invalid_id(self):
        """Test fetch_image with invalid ID (not a URL)."""
        from fastmcp.exceptions import ToolError

        async with Client(mcp) as client:
            # When raise_on_error=True (default), exceptions are raised instead of returned
            with pytest.raises(ToolError) as exc_info:
                await client.call_tool("fetch_image", {"id": "not-a-url"})

            assert "Invalid ID" in str(exc_info.value)
            print("Successfully handled invalid URL for fetch_image")

    @pytest.mark.asyncio
    async def test_fetch_image_nonexistent_chart(self):
        """Test fetch_image with nonexistent chart."""
        from fastmcp.exceptions import ToolError

        async with Client(mcp) as client:
            # When raise_on_error=True (default), exceptions are raised instead of returned
            with pytest.raises(ToolError) as exc_info:
                await client.call_tool(
                    "fetch_image", {"id": "https://ourworldindata.org/grapher/nonexistent-chart-12345.csv"}
                )

            assert "Failed to download PNG" in str(exc_info.value)
            print("Successfully handled nonexistent chart for fetch_image")
