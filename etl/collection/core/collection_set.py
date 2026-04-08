"""A CollectionSet is a collection of Collection objects.

Works similarly to a Dataset (from owid.catalog), which is a collection of Tables.
"""

from pathlib import Path
from typing import Dict

from etl.collection.model.core import Collection


class CollectionSet:
    def __init__(self, path: Path):
        self.path = path
        self.collections = self._build_dictionary()

    def _build_dictionary(self) -> Dict[str, Path]:
        dix = {}
        paths = self.path.glob(r"*.config.json")
        for p in paths:
            name = p.name.replace(".config.json", "")
            dix[name] = p
        return dix

    def read(self, name: str) -> Collection:
        # Check if collection exists
        if name not in self.collections:
            raise ValueError(
                f"Collection name not available. Available options are {self.names}. If this does not make sense to you, try running the necessary steps to re-export files to {self.path}"
            )

        # Read MDIM
        path = self.collections[name]
        try:
            c = Collection.load(str(path))
        except TypeError as e:
            # This is a workaround for the TypeError that occurs when loading the config file.
            raise TypeError(
                f"Error loading Collection config file. Please check the file format and ensure it is valid JSON. Suggestion: Re-run export step generating {name}. Error: {e}"
            )

        # Get and set catalog path
        return c

    @property
    def names(self):
        return list(sorted(self.collections.keys()))
