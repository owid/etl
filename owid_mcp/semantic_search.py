"""Semantic search functionality for OWID indicators via API."""

from typing import Any, Dict, List

import httpx

from owid_mcp.config import ETL_API_URL, HTTP_TIMEOUT


async def semantic_search_indicators(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search for indicators using semantic similarity via ETL API.

    Args:
        query: Search query text
        limit: Maximum number of results to return

    Returns:
        List of indicator results with similarity scores
    """
    # Make HTTP request to the ETL API
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await client.post(f"{ETL_API_URL}/indicators/search", json={"query": query, "limit": limit})
        response.raise_for_status()
        data = response.json()

    # Convert API response to expected MCP format
    results = []
    for result in data["results"]:
        results.append(
            {
                "title": result["title"],
                "indicator_id": result["indicator_id"],
                "snippet": result["snippet"],
                "score": result["score"],
                "metadata": result["metadata"],
            }
        )

    return results
