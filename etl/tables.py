#
#  tables.py
#

from dataclasses import dataclass
from os.path import join, dirname, splitext
import json
from typing import Any, Dict, Optional, List

import pandas as pd
from dataclasses_json import dataclass_json

from . import variables

SCHEMA = json.load(open(join(dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


@dataclass_json
@dataclass
class TableMeta:
    name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None


class Table(pd.DataFrame):
    metadata: TableMeta
    _fields: Dict[str, variables.VariableMeta]

    # propagate all these fields on every slice or copy
    _metadata = METADATA_FIELDS + ["_fields"]

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
        self._fields = {col: variables.VariableMeta() for col in self.columns}

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
            primary_key = (
                metadata.pop("primary_key") if "primary_key" in metadata else []
            )
            for k, v in metadata.items():
                setattr(df, k, v)

        if primary_key:
            df.set_index(primary_key, inplace=True)

        return df


def _proxied_property(k: str) -> property:
    def getter(self) -> Any:  # type: ignore
        return getattr(self.metadata, k)

    def setter(self, v: Any) -> None:  # type: ignore
        return setattr(self.metadata, k, v)

    return property(getter, setter)


# dynamically add all metadata properties to the class
for k in TableMeta.__dataclass_fields__:  # type: ignore
    if hasattr(Table, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Table, k, _proxied_property(k))
