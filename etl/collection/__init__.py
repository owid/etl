"""This module is a package for collections of indicators. This includes:

- Explorers
- MDIMs
"""

from etl.collection.core.collection_set import CollectionSet
from etl.collection.core.combine import combine_collections, combine_config_dimensions
from etl.collection.core.create import create_collection
from etl.collection.core.expand import expand_config
from etl.collection.model.core import Collection
from etl.collection.utils import drop_dimension_keeping_single_value

__all__ = [
    "combine_collections",
    "create_collection",
    "expand_config",
    "combine_config_dimensions",
    "drop_dimension_keeping_single_value",
    "CollectionSet",
    "Collection",
]
