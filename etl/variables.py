#
#  variables.py
#

from os import path
import json
from typing import Any, Dict, Optional

import pandas as pd

from .properties import metadata_property
from .meta import VariableMeta

SCHEMA = json.load(open(path.join(path.dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])


class Variable(pd.Series):
    _name: Optional[str] = None
    _fields: Dict[str, VariableMeta]

    def __init__(
        self,
        data: Any = None,
        index: Any = None,
        _fields: Optional[Dict[str, VariableMeta]] = None,
        **kwargs: Any,
    ) -> None:
        self._fields = _fields or {}

        # silence warning
        if not data and not kwargs.get("dtype"):
            kwargs["dtype"] = "object"

        super().__init__(data=data, index=index, **kwargs)

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

    # which fields should pandas propagate on slicing, etc?
    _metadata = ["_fields"]

    @property
    def _constructor(self) -> type:
        return Variable

    @property
    def _constructor_expanddim(self) -> type:
        # XXX lazy circular import
        from . import tables

        return tables.Table

    @property
    def metadata(self) -> VariableMeta:
        return self._fields[self.checked_name]


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:  # type: ignore
    if hasattr(Variable, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Variable, k, metadata_property(k))
