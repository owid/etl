#
#  tables.py
#

from os.path import join, dirname, splitext
import json
from typing import Any, Optional, List, Dict
from collections import defaultdict

import pandas as pd

from . import variables
from .properties import metadata_property
from .meta import VariableMeta, TableMeta

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


class Table(pd.DataFrame):
    # metdata about the entire table
    metadata: TableMeta

    # metadata about individual columns
    # NOTE: the name _fields is also on the Variable class, pandas will propagate this to
    #       any slices, which is how they get access to their metadata
    _fields: Dict[str, VariableMeta]

    # propagate all these fields on every slice or copy
    _metadata = ["metadata", "_fields"]

    # slicing and copying creates tables
    @property
    def _constructor(self) -> type:
        return Table

    @property
    def _constructor_sliced(self) -> Any:
        return variables.Variable

    def __init__(
        self, *args: Any, metadata: Optional[TableMeta] = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        # empty table metadata by default
        self.metadata = metadata or TableMeta()

        # all columns have empty metadata by default
        assert not hasattr(self, "_fields")
        self._fields = defaultdict(VariableMeta)

    @property
    def primary_key(self) -> List[str]:
        return [n for n in self.index.names if n]

    def to_feather(self, path: Any, **kwargs: Any) -> None:
        """
        Save this table as a feather file plus accompanying JSON metadata file.
        If the table is stored at "mytable.feather", the metadata will be at
        "mytable.meta.json".
        """
        if not isinstance(path, str) or not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        primary_key = self.primary_key

        # feather can't store the index
        df = pd.DataFrame(self)
        if primary_key:
            df = df.reset_index()

        df.to_feather(path, **kwargs)

        # write metadata
        metadata_filename = splitext(path)[0] + ".meta.json"
        with open(metadata_filename, "w") as ostream:
            metadata = self.metadata.to_dict()  # type: ignore
            metadata["primary_key"] = primary_key
            metadata["fields"] = {k: v.to_dict() for k, v in self._fields.items()}
            json.dump(metadata, ostream, indent=2)

    @classmethod
    def read_feather(cls, path: str) -> "Table":
        """
        Read the table from feather plus accompanying JSON sidecar.
        """
        if not path.endswith(".feather"):
            raise ValueError(f'filename must end in ".feather": {path}')

        # load the data
        df = Table(pd.read_feather(path))

        # load the metadata
        metadata_filename = splitext(path)[0] + ".meta.json"
        with open(metadata_filename, "r") as istream:
            metadata = json.load(istream)

        primary_key = metadata.pop("primary_key") if "primary_key" in metadata else []
        fields = metadata.pop("fields") if "fields" in metadata else {}

        df.metadata = TableMeta(**metadata)
        df._fields = defaultdict(
            VariableMeta, {k: VariableMeta.from_dict(v) for k, v in fields.items()}
        )

        if primary_key:
            df.set_index(primary_key, inplace=True)

        return df

    def __setitem__(self, key: Any, value: Any) -> Any:
        super().__setitem__(key, value)

        # propagate metadata when we add a series to a table
        if isinstance(key, str):
            if isinstance(value, variables.Variable):
                self._fields[key] = value.metadata
            else:
                self._fields[key] = VariableMeta()

    def equals_table(self, rhs: "Table") -> bool:
        return (
            isinstance(rhs, Table)
            and self.metadata == rhs.metadata
            and self.to_dict() == rhs.to_dict()
        )


# dynamically add all metadata properties to the class
for k in TableMeta.__dataclass_fields__:  # type: ignore
    if hasattr(Table, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Table, k, metadata_property(k))
