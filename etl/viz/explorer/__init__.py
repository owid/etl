"""This submodule contains utils specific to Explorers.

Explorers are a particular type of ETL collections.

NOTE: Shouldn't import from collection.model or collection.core.
"""

from etl.viz.explorer.core import Explorer
from etl.viz.explorer.legacy import ExplorerLegacy, create_explorer_legacy

__all__ = [
    "Explorer",
    "ExplorerLegacy",
    "create_explorer_legacy",
]
