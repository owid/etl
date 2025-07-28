#!/usr/bin/env python3
"""Test script to make a request to the running MCP server."""

import asyncio
from fastmcp.client import Client
from owid_mcp.deep_research_algolia import mcp

async def test_search():
    """Test the search functionality with a population density query."""
    async with Client(mcp) as client:
        # Search for population density of France
        print("🔍 Searching for: population density France")
        search_result = await client.call_tool("search", {"query": "population density France"})
        
        # For debugging, let's see what we actually got
        print(f"Search result type: {type(search_result)}")
        print(f"Search result: {search_result}")
        
        # The result might be directly available or in a content field
        if hasattr(search_result, 'content') and len(search_result.content) > 0:
            content = search_result.content[0]
            if hasattr(content, 'text'):
                import json
                # Try to parse as JSON if it's a string
                try:
                    search_results = json.loads(content.text)
                    print(f"✅ Found {len(search_results)} results:")
                    for i, result in enumerate(search_results, 1):
                        print(f"  {i}. {result.get('title', 'No title')}")
                        print(f"     URL: {result.get('url', 'No URL')}")
                        print(f"     Text: {result.get('text', 'No text')[:100]}...")
                        print()
                except json.JSONDecodeError:
                    print("Raw response:")
                    print(content.text)
            else:
                print("No text content found")
        else:
            print("No content found in search result")
        
        # Skip fetch for now, just show the search worked
        return

if __name__ == "__main__":
    asyncio.run(test_search())