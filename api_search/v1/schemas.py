from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class SemanticSearchRequest(BaseModel):
    """JSON schema for semantic search request."""

    query: str
    limit: int = 10

    class Config:
        extra = "forbid"


class SemanticSearchResult(BaseModel):
    """JSON schema for individual semantic search result."""

    title: str
    indicator_id: int
    snippet: str
    score: float
    metadata: Dict[str, Any]
    # Additional fields for wizard app
    catalog_path: Optional[str] = None
    n_charts: int = 0
    description: Optional[str] = None

    class Config:
        extra = "forbid"


class SemanticSearchResponse(BaseModel):
    """JSON schema for semantic search response."""

    results: List[SemanticSearchResult]
    query: str
    total_results: int

    class Config:
        extra = "forbid"
