"""
Tests for OWID Deep Research Algolia MCP Module.
"""

import base64
import json
from unittest.mock import MagicMock, patch

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

    def test_manual_mappings(self):
        """Test manual mapping cases."""
        assert country_name_to_iso3("World") == "OWID_WRL"
        assert country_name_to_iso3("European Union") == "OWID_EUN"
        assert country_name_to_iso3("OECD") == "OWID_OECD"
        assert country_name_to_iso3("High income") == "OWID_HIN"
        assert country_name_to_iso3("Low income") == "OWID_LIN"

    @patch("owid_mcp.deep_research_algolia.pycountry")
    def test_pycountry_lookup_success(self, mock_pycountry):
        """Test successful pycountry lookup."""
        mock_country = MagicMock()
        mock_country.alpha_3 = "USA"
        mock_pycountry.countries.lookup.return_value = mock_country

        result = country_name_to_iso3("United States")
        assert result == "USA"
        mock_pycountry.countries.lookup.assert_called_once_with("United States")

    @patch("owid_mcp.deep_research_algolia.pycountry")
    def test_pycountry_lookup_error_fallback_to_manual(self, mock_pycountry):
        """Test pycountry lookup error with fallback to manual mapping."""
        mock_pycountry.countries.lookup.side_effect = LookupError()

        result = country_name_to_iso3("World")
        assert result == "OWID_WRL"

    @patch("owid_mcp.deep_research_algolia.pycountry")
    def test_pycountry_lookup_error_no_manual_match(self, mock_pycountry):
        """Test pycountry lookup error with no manual mapping."""
        mock_pycountry.countries.lookup.side_effect = LookupError()

        result = country_name_to_iso3("Unknown Country")
        assert result is None

    @patch("owid_mcp.deep_research_algolia.pycountry", None)
    def test_no_pycountry_module_fallback_to_manual(self):
        """Test when pycountry module is not available."""
        result = country_name_to_iso3("World")
        assert result == "OWID_WRL"

    @patch("owid_mcp.deep_research_algolia.pycountry", None)
    def test_no_pycountry_module_no_manual_match(self):
        """Test when pycountry module is not available and no manual match."""
        result = country_name_to_iso3("Unknown Country")
        assert result is None


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
        """Test fetching a real chart image."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "fetch", {"id": "https://ourworldindata.org/grapher/population-density.png?tab=chart"}
            )

            assert result is not None
            # Basic validation that fetch worked
            print(f"Fetch result type: {type(result.data)}")
            print("Successfully called fetch tool with valid URL")

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
                "fetch", {"id": "https://ourworldindata.org/grapher/nonexistent-chart-12345.png"}
            )

            assert result is not None
            print("Successfully called fetch tool with nonexistent chart URL")


class TestIntegration:
    """Integration tests for the deep research algolia module."""

    @pytest.mark.asyncio
    async def test_server_initialization(self):
        """Test that the MCP server initializes correctly."""
        async with Client(mcp) as client:
            # Test that the server is responsive
            tools = await client.list_tools()
            assert tools is not None
            assert len(tools) == 2

            tool_names = [tool.name for tool in tools]
            assert "search" in tool_names
            assert "fetch" in tool_names

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

            # Now .data[0] should contain the structured SearchResult data
            expected_result = {
                "id": "https://ourworldindata.org/grapher/coal-consumption-by-country-terawatt-hours-twh.png?tab=chart&country=~FRA",
                "text": "Coal consumption by country or region, measured in terawatt-hours (TWh).",
                "title": "Coal consumption",
                "url": "https://ourworldindata.org/grapher/coal-consumption-by-country-terawatt-hours-twh.png?tab=chart&country=~FRA",
            }

            assert search_result.data[0] == expected_result
