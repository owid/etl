#
#  variables.py
#

from os import path
import json
from typing import Any, Dict, Optional
from dataclasses import dataclass

import pandas as pd

from . import tables

SCHEMA = json.load(open(path.join(path.dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


@dataclass
class VariableMeta:
    title: Optional[str] = None
    description: Optional[str] = None


class Variable(pd.Series):
    _name: Optional[str] = None
    _fields: Dict[str, VariableMeta]

    def __init__(
        self,
        *args: Any,
        _fields: Optional[Dict[str, VariableMeta]] = None,
        **kwargs: Any,
    ) -> None:
        self._fields = _fields or {}

        super().__init__(*args, **kwargs)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        # move metadata when you rename a field
        if self._name and self._name in self._fields:
            self._fields[name] = self._fields.pop(self._name)

        # make sure there is always a placeholder metadata object
        if name not in self._fields:
            self._fields[name] = VariableMeta()

        self._name = name

    @property
    def checked_name(self) -> str:
        if not self.name:
            raise ValueError("variable must be named to have metadata")

        return self.name

    _metadata = ["_fields"]

    @property
    def _constructor(self) -> type:
        return Variable

    @property
    def _constructor_expanddim(self) -> type:
        return tables.Table


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:  # type: ignore
    setattr(
        Variable,
        k,
        property(
            lambda self: getattr(self._fields[self.checked_name], k),
            lambda self, v: setattr(
                self._fields[self.checked_name],
                k,
                v,
            ),
        ),
    )
