#
#  variables.py
#
from __future__ import annotations

import copy
import json
import os
from collections import defaultdict
from collections.abc import Callable
from typing import Any, Literal, cast, overload

import pandas as pd
import structlog
from pandas._typing import Scalar
from pandas.core.series import Series

from . import processing_log as pl
from . import warnings
from .meta import (
    PROCESSING_LEVELS,
    PROCESSING_LEVELS_ORDER,
    License,
    Origin,
    ProcessingLog,
    Source,
    VariableMeta,
    VariablePresentationMeta,
)
from .properties import metadata_property

log = structlog.get_logger()

SCHEMA = json.load(open(os.path.join(os.path.dirname(__file__), "schemas", "table.json")))
METADATA_FIELDS = list(SCHEMA["properties"])

# Defined operations.
OPERATION = Literal[
    "+",
    "-",
    "*",
    "/",
    "**",
    "//",
    "%",
    "fillna",
    "dropna",
    "load",
    "create",
    "save",
    "merge",
    "rename",
    "melt",
    "pivot",
    "concat",
    "sort",
    "pct_change",
]

# NOTE: The following issue seems to not be happening anymore. Consider deleting instances of UNNAMED_VARIABLE.
# When creating a new variable, might we need to pass a temporary name. For example, when doing tb["a"] + tb["b"]:
#  * If variable.name is None, a ValueError is raised.
#  * If variable.name = self.checked_name then the metadata of the first variable summed ("a") is modified.
#  * If variable.name is always a random string (that does not coincide with an existing variable) then
#    when replacing a variable (e.g. tb["a"] += 1) the original variable loses its metadata.
# For these reasons, we ensure that variable.name is always filled, even with a temporary name.
# In fact, if the new variable becomes a column in a table, its name gets overwritten by the column name (which is a
# nice feature). For example, when doing tb["c"] = tb["a"] + tb["b"], the variable name of "c" will be "c", even if we
# passed a temporary variable name. Therefore, this temporary name may be irrelevant in practice.
UNNAMED_VARIABLE = "**TEMPORARY UNNAMED VARIABLE**"


class Variable(pd.Series):
    _name: str | None = None
    _fields: dict[str, VariableMeta]

    def __init__(
        self,
        data: Any = None,
        index: Any = None,
        name: str | None = None,
        _fields: dict[str, VariableMeta] | None = None,
        metadata: VariableMeta | None = None,
        **kwargs: Any,
    ) -> None:
        if metadata:
            assert not _fields, "cannot pass both metadata and _fields"
            assert name or self.name, "cannot pass metadata without a name"
            _fields = {(name or self.name): metadata}  # type: ignore

        self._fields = _fields or defaultdict(VariableMeta)

        # silence warning
        if data is None and not kwargs.get("dtype"):
            kwargs["dtype"] = "object"

        # DeprecationWarning: Passing a SingleBlockManager to Variable is deprecated and will raise in a future version. Use public APIs instead.
        with warnings.ignore_warnings([DeprecationWarning]):
            super().__init__(data=data, index=index, name=name, **kwargs)

    @property
    def m(self) -> VariableMeta:
        """Metadata alias to save typing."""
        return self.metadata

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        # None name does not modify _fields, it is usually triggered on pandas operations
        if name is not None:
            # move metadata when you rename a field
            if self._name and self._name in self._fields:
                self._fields[name] = self._fields.pop(self._name)

            # make sure there is always a placeholder metadata object
            if name not in self._fields:
                self._fields[name] = VariableMeta()

        self._name = name

    @property
    def checked_name(self) -> str:
        if self.name is None:
            raise ValueError("variable must be named to have metadata")

        return self.name

    # which fields should pandas propagate on slicing, etc?
    _metadata = ["_fields", "_name"]

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
        vm = self._fields[self.checked_name]

        # pass this as a hidden attribute to the metadata object for display only
        vm._name = self.name  # type: ignore

        return vm

    @metadata.setter
    def metadata(self, meta: VariableMeta) -> None:
        self._fields[self.checked_name] = meta

    def astype(self, *args: Any, **kwargs: Any) -> Variable:
        # To fix: https://github.com/owid/owid-catalog-py/issues/12
        v = super().astype(*args, **kwargs)
        v.name = self.name
        return cast(Variable, v)

    def _repr_html_(self):
        html = str(self)
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-size: 1.5em; font-family: sans-serif; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">variable</p>
             <pre>{}</pre>
        """.format(self.name, html)

    def __add__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__add__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="+", name=variable_name)
        return variable

    def __iadd__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__add__(other)

    def __sub__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__sub__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="-", name=variable_name)
        return variable

    def __isub__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__sub__(other)

    def __mul__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__mul__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="*", name=variable_name)
        return variable

    def __imul__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__mul__(other)

    def __truediv__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        if is_nullable_series(self) or is_nullable_series(other):
            # 0/0 should return pd.NA, not np.nan
            zero_div_zero = (other == 0) & (self == 0)
            if zero_div_zero.any():
                other = other.replace({0: pd.NA})  # type: ignore

        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__truediv__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="/", name=variable_name)
        return variable

    def __itruediv__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__truediv__(other)

    def __floordiv__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__floordiv__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="//", name=variable_name)
        return variable

    def __ifloordiv__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__floordiv__(other)

    def __mod__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__mod__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="%", name=variable_name)
        return variable

    def __imod__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__mod__(other)

    def __pow__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__pow__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="**", name=variable_name)
        return variable

    def __ipow__(self, other: Scalar | Series | Variable) -> Variable:  # type: ignore
        return self.__pow__(other)

    def fillna(self, value=None, *args, **kwargs) -> Variable:
        # NOTE: Argument "inplace" will modify the original variable's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            warnings.warn(
                "Avoid using fillna(inplace=True), which may not handle metadata as expected.", warnings.MetadataWarning
            )
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().fillna(value, *args, **kwargs), name=variable_name)
        variable._fields = copy.deepcopy(self._fields)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self, value], operation="fillna", name=variable_name
        )
        return variable

    def dropna(self, *args, **kwargs) -> Variable:
        # NOTE: Argument "inplace" will modify the original variable's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            warnings.warn(
                "Avoid using dropna(inplace=True), which may not handle metadata as expected.", warnings.MetadataWarning
            )
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().dropna(*args, **kwargs), name=variable_name)
        variable._fields = copy.deepcopy(self._fields)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self], operation="dropna", name=variable_name
        )
        return variable

    def add(self, other: Scalar | Series | Variable, *args, **kwargs) -> Variable:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__add__(other=other)

    def sub(self, other: Scalar | Series | Variable, *args, **kwargs) -> Variable:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__sub__(other=other)

    def mul(self, other: Scalar | Series | Variable, *args, **kwargs) -> Variable:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__mul__(other=other)

    def truediv(self, other: Scalar | Series | Variable, *args, **kwargs) -> Variable:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__truediv__(other=other)

    def div(self, other: Scalar | Series | Variable, *args, **kwargs) -> Variable:  # type: ignore
        return self.truediv(other=other, *args, **kwargs)

    def pct_change(self, *args, **kwargs) -> Variable:
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().pct_change(*args, **kwargs), name=variable_name)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self], operation="pct_change", name=variable_name
        )
        return variable

    def set_categories(self, *args, **kwargs) -> Variable:
        return Variable(self.cat.set_categories(*args, **kwargs), name=self.name, metadata=self.metadata.copy())

    def update_log(
        self,
        operation: str,
        parents: list[Any] | None = None,
        variable: str | None = None,
        comment: str | None = None,
    ) -> Variable:
        if variable is None:
            # If a variable name is not specified, take it from the variable, or otherwise use UNNAMED_VARIABLE.
            variable = self.name or UNNAMED_VARIABLE

        if parents is None:
            # If parents are not specified, take the variable itself as the only parent.
            parents = [self]

        # Add new entry to the variable's processing log.
        self.metadata.processing_log.add_entry(
            variable=variable,
            parents=parents,
            operation=operation,
            comment=comment,
        )
        return self

    def rolling(self, *args, **kwargs) -> VariableRolling:
        """Rolling operation that preserves metadata."""
        return VariableRolling(super().rolling(*args, **kwargs), self.metadata.copy(), self.name)  # type: ignore

    def copy_metadata(self, from_variable: Variable, inplace: bool = False) -> Variable | None:
        return copy_metadata(to_variable=self, from_variable=from_variable, inplace=inplace)  # type: ignore

    def copy(self, deep: bool = True) -> Variable:
        new_var = super().copy(deep=deep)
        if deep:
            field_names = [n for n in self.index.names + [self.name] if n is not None]
            new_var._fields = defaultdict(VariableMeta, {k: self._fields[k].copy(deep=deep) for k in field_names})
        return new_var


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:
    if hasattr(Variable, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Variable, k, metadata_property(k))


class VariableRolling:
    # fixes type hints
    __annotations__ = {}

    def __init__(self, rolling: pd.core.window.rolling.Rolling, metadata: VariableMeta, name: str | None = None):
        self.rolling = rolling
        self.metadata = metadata
        self.name = name

    def __getattr__(self, name: str) -> Callable[..., Variable]:
        def func(*args, **kwargs):
            """Apply function and return variable with proper metadata."""
            x = getattr(self.rolling, name)(*args, **kwargs)
            return Variable(x, name=self.name, metadata=self.metadata)

        self.__annotations__[name] = Callable[..., Variable]
        return func


def _hash_dict(d):
    return hash(json.dumps(d, sort_keys=True))


def _get_metadata_value_from_variables_if_all_identical(
    variables: list[Variable],
    field: str,
    warn_if_different: bool = False,
    operation: OPERATION | None = None,
) -> Any | None:
    if (operation == "/") and (getattr(variables[0].metadata, field) is None):
        # When dividing a variable by another, it only makes sense to keep the metadata values of the first variable.
        # For example, if we have energy (without description) and population (with a description), when calculating
        # energy per capita, the result shouldn't have the description of population. It should have no description.
        # Therefore, if the first variable has no metadata value, return None.
        return None

    # Get unique values from list, ignoring Nones.
    if field == "dimensions":
        # TODO: we could make a special object from dimensions and make it hashable
        unique_values = [
            variable.metadata.dimensions for variable in variables if variable.metadata.dimensions is not None
        ]
        unique_hashes = {_hash_dict(dims) for dims in unique_values}
        if len(unique_hashes) == 1:
            unique_values = unique_values[:1]
    else:
        unique_values = {
            getattr(variable.metadata, field) for variable in variables if getattr(variable.metadata, field) is not None
        }

    if len(unique_values) == 1:
        combined_value = unique_values.pop()
    else:
        combined_value = None
        if (len(unique_values) > 1) and (operation not in ["/", "*"]) and warn_if_different:
            # There is no need to warn if units are different when doing a multiplication or a division.
            # In most cases, units will be different, and that is fine, as long as the resulting variable has no units.
            # Note that the same reasoning can be applied to other operations, so we may need to generalize this logic.
            warnings.warn(
                f"Different values of '{field}' detected among variables: {unique_values}",
                warnings.DifferentValuesWarning,
            )

    return combined_value


def get_unique_sources_from_variables(variables: list[Variable]) -> list[Source]:
    # Make a list of all sources of all variables.
    sources = []
    for variable in variables:
        sources += [s for s in variable.metadata.sources if s not in sources]
    return sources


def get_unique_origins_from_variables(variables: list[Variable]) -> list[Origin]:
    # Make a list of all origins of all variables.
    origins = []
    for variable in variables:
        # Get unique array of tuples of origin fields (respecting the order).
        origins += [o for o in variable.metadata.origins if o not in origins]
    return origins


def get_unique_licenses_from_variables(variables: list[Variable]) -> list[License]:
    # Make a list of all licenses of all variables.
    licenses = []
    for variable in variables:
        licenses += [license for license in variable.metadata.licenses if license not in licenses]
    return licenses


def get_unique_description_key_points_from_variables(variables: list[Variable]) -> list[str]:
    # Make a list of all description key points of all variables.
    description_key_points = []
    for variable in variables:
        description_key_points += [k for k in variable.metadata.description_key if k not in description_key_points]
    return description_key_points


def combine_variables_processing_logs(variables: list[Variable]) -> ProcessingLog:
    # Make a list with all entries in the processing log of all variables.
    processing_log = sum(
        [
            variable.metadata.processing_log if variable.metadata.processing_log is not None else []
            for variable in variables
        ],
        [],
    )

    return ProcessingLog(processing_log)


def _get_dict_from_list_if_all_identical(list_of_objects: list[dict[str, Any] | None]) -> dict[str, Any] | None:
    # The argument list_of_objects can contain dictionaries or None, or be empty.
    # If a list contains one dictionary (possibly repeated multiple times with identical content), return that
    # dictionary. Otherwise, if not all dictionaries are identical, return None.

    # List all dictionaries, ignoring Nones.
    defined_dicts = [d for d in list_of_objects if d is not None]

    if not defined_dicts:
        # If there are no dictionaries, return None.
        return None

    # Take the first dictionary as a reference.
    reference_dict = defined_dicts[0]

    # Return a copy of the first dictionary if all dictionaries are identical, otherwise return None.
    return reference_dict.copy() if all(d == reference_dict for d in defined_dicts) else None


def combine_variables_display(
    variables: list[Variable], operation: OPERATION, _field_name="display"
) -> dict[str, Any] | None:
    # Gather displays from all variables that are defined.
    list_of_displays = [getattr(variable.metadata, _field_name) for variable in variables]
    if operation == "/" and list_of_displays[0] is None:
        # When dividing a variable by another, it only makes sense to keep the display values of the first variable.
        # Therefore, if the first variables doesn't have a display, the resulting variable should have no display.
        return None
    else:
        return _get_dict_from_list_if_all_identical(list_of_objects=list_of_displays)


def combine_variables_presentation(variables: list[Variable], operation: OPERATION) -> VariablePresentationMeta | None:
    # Apply the same logic as for displays.
    return combine_variables_display(variables=variables, operation=operation, _field_name="presentation")  # type: ignore


def combine_variables_processing_level(variables: list[Variable]) -> PROCESSING_LEVELS | None:
    # Gather processing levels from all variables that are defined.
    processing_levels = [
        variable.metadata.processing_level for variable in variables if variable.metadata.processing_level is not None
    ]

    if len(processing_levels) == 0:
        # If there are no processing levels, return None.
        return None

    # Ensure that all processing levels are known.
    unknown_processing_levels = {level for level in processing_levels} - set(PROCESSING_LEVELS_ORDER)
    assert len(unknown_processing_levels) == 0, f"Unknown processing levels: {unknown_processing_levels}"

    # If any of the variables has a processing level, take the highest level.
    maximum_level = max([PROCESSING_LEVELS_ORDER[level] for level in processing_levels])

    # Return the maximum level as a string.
    combined_processing_level = {value: key for key, value in PROCESSING_LEVELS_ORDER.items()}[maximum_level]

    return cast(PROCESSING_LEVELS, combined_processing_level)


def combine_variables_sort(variables: list[Variable]) -> list[str]:
    # Return sort if all variables have the same sort, otherwise return empty list.
    sorts = [variable.metadata.sort for variable in variables if variable.metadata.sort]
    if not sorts:
        return []
    else:
        return sorts[0] if all(sort == sorts[0] for sort in sorts) else []


def combine_variables_metadata(
    variables: list[Any], operation: OPERATION, name: str = UNNAMED_VARIABLE
) -> VariableMeta:
    # Initialise an empty metadata.
    metadata = VariableMeta()

    # Skip other objects passed in variables that may not contain metadata (e.g. a scalar),
    # and skip unnamed variables that cannot have metadata
    variables_only = [v for v in variables if hasattr(v, "name") and v.name and hasattr(v, "metadata")]

    # Combine each metadata field using the logic of the specified operation.
    metadata.title = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="title", operation=operation
    )
    metadata.description = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="description", operation=operation
    )
    metadata.description_short = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="description_short", operation=operation
    )
    metadata.description_key = get_unique_description_key_points_from_variables(variables=variables_only)
    # TODO: Combine description_processing: If not identical, append one after another.
    metadata.description_from_producer = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="description_from_producer", operation=operation
    )
    metadata.unit = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="unit", operation=operation, warn_if_different=True
    )
    metadata.short_unit = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="short_unit", operation=operation, warn_if_different=True
    )
    metadata.sources = get_unique_sources_from_variables(variables=variables_only)
    metadata.origins = get_unique_origins_from_variables(variables=variables_only)
    metadata.licenses = get_unique_licenses_from_variables(variables=variables_only)
    metadata.display = combine_variables_display(variables=variables_only, operation=operation)
    metadata.presentation = combine_variables_presentation(variables=variables_only, operation=operation)
    metadata.processing_level = combine_variables_processing_level(variables=variables_only)

    metadata.type = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="type", operation=operation, warn_if_different=True
    )
    metadata.sort = combine_variables_sort(variables=variables_only)
    metadata.license = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="license", operation=operation, warn_if_different=True
    )
    metadata.dimensions = _get_metadata_value_from_variables_if_all_identical(
        variables=variables_only, field="dimensions", operation=operation, warn_if_different=True
    )

    if pl.enabled():
        metadata.processing_log = combine_variables_processing_logs(variables=variables_only)
        if operation:
            metadata.processing_log.add_entry(
                variable=name,
                parents=variables,
                operation=operation,
            )

    return metadata


@overload
def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: Literal[False] = False) -> Variable: ...


@overload
def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: Literal[True] = True) -> None: ...


def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: bool = False) -> Variable | None:
    if inplace:
        to_variable.metadata = from_variable.metadata.copy()
    else:
        new_variable = to_variable.copy()
        new_variable.metadata = from_variable.metadata.copy()
        return new_variable


def is_nullable_series(s: Any) -> bool:
    """Check if a series has a nullable pandas dtype."""
    if not hasattr(s, "dtype"):
        return False

    nullable_types = {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "boolean",
    }
    return str(s.dtype) in nullable_types
