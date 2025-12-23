"""Experimental features for owid-catalog API.

This module contains experimental features that may change or be removed in future versions.
Use with caution in production code.

Features are organized into experimental_* methods on existing classes and standalone
functions in this namespace. Experimental features that prove stable will be graduated
to the main API by removing the experimental_ prefix.

Quick Access Functions:
    quick() - Smart search and download with fuzzy matching
    get() - Direct download by catalog path

Caching:
    cached_client() - Create a client with automatic caching

Bulk Operations:
    bulk.download() - Parallel download of multiple tables

The experimental namespace follows the streamlit convention: experimental features
are clearly marked and easy to find, allowing rapid iteration while maintaining
API stability for core features.

Examples:
    >>> from owid.catalog.api.experimental import quick
    >>> table = quick("population")  # Search and download with fuzzy matching

    >>> from owid.catalog.api.experimental import cached_client
    >>> client = cached_client(ttl="7d")  # Enable caching with 7-day TTL

    >>> from owid.catalog.api.experimental import bulk
    >>> tables = bulk.download(["un/un_wpp/population", "worldbank/wdi/gdp"])
"""

# Version marker for experimental features
__version__ = "0.1.0-experimental"

# Import experimental functions
from .quick import get, quick

# Commented out until implemented:
# from .cache import cached_client
# from . import bulk

# Exported functions
__all__ = [
    "quick",
    "get",
    # "cached_client",
    # "bulk",
]
