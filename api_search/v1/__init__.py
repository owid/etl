import structlog
from fastapi import APIRouter

from api_search.semantic_search import get_model_info, search_indicators

from .schemas import (
    SemanticSearchRequest,
    SemanticSearchResponse,
    SemanticSearchResult,
)

log = structlog.get_logger()

v1 = APIRouter()


@v1.get("/health")
def health() -> dict:
    return {"status": "ok"}


@v1.get("/indicators", response_model=SemanticSearchResponse)
async def search_indicators_semantic(query: str, limit: int = 10) -> SemanticSearchResponse:
    """
    Search for indicators using semantic similarity.

    This endpoint performs semantic search on OWID indicators using preloaded embeddings
    and returns the most relevant results.
    """
    # Perform semantic search using preloaded model
    raw_results = search_indicators(query, limit)

    # Convert results to API schema format
    results = []
    for result in raw_results:
        results.append(
            SemanticSearchResult(
                title=result["title"],
                indicator_id=result["indicator_id"],
                snippet=result["snippet"],
                score=result["score"],
                metadata=result["metadata"],
                catalog_path=result["catalog_path"],
                n_charts=result["n_charts"],
                description=result["description"],
            )
        )

    return SemanticSearchResponse(results=results, query=query, total_results=len(results))


@v1.get("/indicators/info", include_in_schema=False)
async def get_semantic_search_info():
    """Get information about the semantic search model status."""
    return get_model_info()
