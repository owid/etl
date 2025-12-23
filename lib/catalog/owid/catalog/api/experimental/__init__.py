"""Experimental features for owid-catalog API.

This module contains experimental features that may change or be removed in future versions.
Use with caution in production code.

Features are organized into experimental_* methods on existing classes and standalone
functions in this namespace. Experimental features that prove stable will be graduated
to the main API by removing the experimental_ prefix.

Data Discovery and Retrieval:

    - show(): Browse available data without downloading (discovery/exploration)
    - get(): Direct download by catalog path (precise retrieval)

The API separates discovery from download for better cost awareness:

- Use show() to explore and find what you need (lightweight, no downloads)
- Use get() to download specific data by path (explicit, knows what you're getting)

The experimental namespace follows the streamlit convention: experimental features
are clearly marked and easy to find, allowing rapid iteration while maintaining
API stability for core features.

Example:
    ```python
    from owid.catalog.api.experimental import show, get

    # Browse available data (no download)
    show("population")  # Shows matching paths

    # Download specific data by path
    table = get("garden/un/2024-07-12/un_wpp/population")

    # Or browse indicators
    show("gdp", kind="indicator")
    ind_table = get("garden/un/2024-07-12/un_wpp/population#population")
    ```
"""

# TODOs:
# - cache: cached_client() for reusing Client with caching
# - bulk: bulk download functions for multiple paths at once

# Version marker for experimental features
__version__ = "0.1.0-experimental"

# Import experimental functions
from .quick import get, show

# Commented out until implemented:
# from .cache import cached_client
# from . import bulk

# Exported functions
__all__ = [
    "show",
    "get",
    # "cached_client",
    # "bulk",
]
