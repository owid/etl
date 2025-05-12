"""Core functions and classes.

It can import from any submodules (collection.model, collection.explorer, etc.)
"""

from etl.collection.core.collection_set import CollectionSet
from etl.collection.core.combine import combine_collections, combine_config_dimensions
from etl.collection.core.create import create_collection
from etl.collection.core.expand import expand_config

__all__ = [
    "combine_collections",
    "create_collection",
    "CollectionSet",
    "expand_config",
    "combine_config_dimensions",
]
