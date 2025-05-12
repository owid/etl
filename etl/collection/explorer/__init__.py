"""This submodule contains utils specific to Explorers.

Explorers are a particular type of ETL collections."""

from etl.collection.explorer.core import Explorer, combine_config_dimensions, expand_config
from etl.collection.explorer.legacy import ExplorerLegacy, create_explorer_legacy

__all__ = [
    "Explorer",
    "expand_config",
    "combine_config_dimensions",
    "ExplorerLegacy",
    "create_explorer_legacy",
]
