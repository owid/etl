"""Semantic search functionality for OWID indicators - API version."""

import asyncio
import threading
from typing import Any, Dict, List, Optional

from apps.wizard.app_pages.indicator_search.data import Indicator, _get_data_indicators_from_db
from apps.wizard.utils.embeddings import EmbeddingsModel, get_model


def build_catalog_info(catalog_path: str) -> Dict[str, Any]:
    """Extract catalog metadata from path."""
    if not catalog_path or catalog_path == "NULL":
        return {}

    # Remove grapher/ prefix if present
    path = catalog_path.replace("grapher/", "")
    parts = path.split("/")

    if len(parts) >= 3:
        return {
            "namespace": parts[0],
            "version": parts[1],
            "dataset": parts[2],
        }

    return {}


# Global variables to store preloaded data and initialization state
_indicators: Optional[List[Indicator]] = None
_embeddings_model: Optional[EmbeddingsModel[Indicator]] = None
_initialization_complete: bool = False
_initialization_error: Optional[str] = None


def _initialize_semantic_search():
    """Initialize indicators and embeddings model. Runs in background thread."""
    global _indicators, _embeddings_model, _initialization_complete, _initialization_error

    try:
        # Fetch all data indicators.
        _indicators = _get_data_indicators_from_db()

        # Get embedding model.
        model = get_model()
        _embeddings_model = EmbeddingsModel(model)
        _embeddings_model.fit(_indicators)
        
        _initialization_complete = True
    except Exception as e:
        _initialization_error = str(e)
        _initialization_complete = True


def initialize_semantic_search_async():
    """Start semantic search initialization in background thread."""
    thread = threading.Thread(target=_initialize_semantic_search, daemon=True)
    thread.start()


def search_indicators(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Search indicators using the preloaded model."""
    if not _initialization_complete:
        raise RuntimeError("Semantic search model is still initializing. Please wait and try again.")
    
    if _initialization_error:
        raise RuntimeError(f"Semantic search initialization failed: {_initialization_error}")
    
    if _embeddings_model is None:
        raise RuntimeError("Semantic search model not initialized")

    # Perform semantic search
    sorted_indicators = _embeddings_model.get_sorted_documents_by_similarity(query)

    # Format results
    results = []
    for indicator in sorted_indicators[:limit]:
        meta = build_catalog_info(indicator.catalogPath)
        meta["chart_count"] = indicator.n_charts

        results.append(
            {
                "title": indicator.name,
                "indicator_id": indicator.variableId,
                "snippet": (indicator.description or "")[:160],
                "score": float(indicator.similarity or 0.0),
                "metadata": meta,
            }
        )

    return results


def get_model_info() -> Dict[str, Any]:
    """Get information about the loaded model and indicators."""
    return {
        "indicators_loaded": len(_indicators) if _indicators else 0,
        "model_loaded": _embeddings_model is not None,
        "initialization_complete": _initialization_complete,
        "initialization_error": _initialization_error,
        "ready": _initialization_complete and _embeddings_model is not None and _indicators is not None,
    }
