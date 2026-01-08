from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from api_search.semantic_search import get_model_info, is_ready, search_indicators

from .schemas import (
    INDICATOR_SEARCH_EXAMPLES,
    SemanticSearchResponse,
    SemanticSearchResult,
)

log = structlog.get_logger()

v1 = APIRouter()


@v1.get("/health")
def health() -> dict:
    return {"status": "ok"}


@v1.get(
    "/indicators",
    response_model=SemanticSearchResponse,
    responses={
        200: {
            "description": "Successful response with search results",
            "content": {"application/json": {"examples": INDICATOR_SEARCH_EXAMPLES}},
        }
    },
)
async def search_indicators_semantic(
    query: str = Query(..., description="Search query", examples=["gdp", "population"]),
    limit: int = Query(10, description="Limit the number of results", le=100),
    min_popularity: Optional[float] = Query(
        None, description="Minimum popularity score (0-1) to filter results", ge=0, le=1
    ),
) -> SemanticSearchResponse:
    """
    Search for indicators using semantic similarity.

    This endpoint performs semantic search on OWID indicators using preloaded embeddings
    and returns the most relevant results.
    """
    # Check if model is ready
    if not is_ready():
        raise HTTPException(
            status_code=503,
            detail="Semantic search model is still initializing. Please try again in a few seconds.",
            headers={"Retry-After": "5"},
        )

    # Cap limit at 100 to match API maximum
    limit = min(limit, 100)

    # Perform semantic search using preloaded model
    # Request more results if filtering by popularity, since many will be filtered out
    fetch_limit = limit * 10 if min_popularity is not None else limit
    raw_results = search_indicators(query, fetch_limit)

    # Filter by minimum popularity if specified
    if min_popularity is not None:
        raw_results = [r for r in raw_results if r["popularity"] is not None and r["popularity"] >= min_popularity]

    # Convert results to API schema format and apply limit
    results = [SemanticSearchResult(**result) for result in raw_results[:limit]]

    return SemanticSearchResponse(results=results, query=query, total_results=len(results))


@v1.get("/indicators/info", include_in_schema=False)
async def get_semantic_search_info():
    """Get information about the semantic search model status."""
    return get_model_info()
