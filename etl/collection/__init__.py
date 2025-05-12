"""This module is a package for collections of indicators. This includes:

- Explorers
- MDIMs
"""

from etl.collection.combine import combine_collections
from etl.collection.multidim import create_collection

__all__ = [
    "combine_collections",
    "create_collection",
]
