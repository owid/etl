#
#  variables.py
#

import copy
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Literal, Optional, Union, cast, overload

import pandas as pd
import structlog
from pandas._typing import Scalar
from pandas.core.series import Series

from .meta import (
    PROCESSING_LEVELS,
    PROCESSING_LEVELS_ORDER,
    License,
    Origin,
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

# Environment variable such that, if True, the processing log will be updated, if False, the log will always be empty.
# If not defined, assume False.
PROCESSING_LOG = bool(os.getenv("PROCESSING_LOG", False))

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
    _name: Optional[str] = None
    _fields: Dict[str, VariableMeta]

    def __init__(
        self,
        data: Any = None,
        index: Any = None,
        _fields: Optional[Dict[str, VariableMeta]] = None,
        **kwargs: Any,
    ) -> None:
        self._fields = _fields or defaultdict(VariableMeta)

        # silence warning
        if data is None and not kwargs.get("dtype"):
            kwargs["dtype"] = "object"

        super().__init__(data=data, index=index, **kwargs)

    @property
    def name(self) -> Optional[str]:
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
        if not self.name:
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

    def astype(self, *args: Any, **kwargs: Any) -> "Variable":
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
        """.format(
            self.name, html
        )

    def __add__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__add__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="+", name=variable_name)
        return variable

    def __iadd__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__add__(other)

    def __sub__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__sub__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="-", name=variable_name)
        return variable

    def __isub__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__sub__(other)

    def __mul__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__mul__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="*", name=variable_name)
        return variable

    def __imul__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__mul__(other)

    def __truediv__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__truediv__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="/", name=variable_name)
        return variable

    def __itruediv__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__truediv__(other)

    def __floordiv__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__floordiv__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="//", name=variable_name)
        return variable

    def __ifloordiv__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__floordiv__(other)

    def __mod__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__mod__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="%", name=variable_name)
        return variable

    def __imod__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__mod__(other)

    def __pow__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().__pow__(other), name=variable_name)
        variable.metadata = combine_variables_metadata(variables=[self, other], operation="**", name=variable_name)
        return variable

    def __ipow__(self, other: Union[Scalar, Series, "Variable"]) -> "Variable":
        return self.__pow__(other)

    def fillna(self, value=None, *args, **kwargs) -> "Variable":
        # NOTE: Argument "inplace" will modify the original variable's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            log.warning("Avoid using fillna(inplace=True), which may not handle metadata as expected.")
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().fillna(value, *args, **kwargs), name=variable_name)
        variable._fields = copy.deepcopy(self._fields)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self, value], operation="fillna", name=variable_name
        )
        return variable

    def dropna(self, *args, **kwargs) -> "Variable":
        # NOTE: Argument "inplace" will modify the original variable's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            log.warning("Avoid using dropna(inplace=True), which may not handle metadata as expected.")
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().dropna(*args, **kwargs), name=variable_name)
        variable._fields = copy.deepcopy(self._fields)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self], operation="dropna", name=variable_name
        )
        return variable

    def add(self, other: Union[Scalar, Series, "Variable"], *args, **kwargs) -> "Variable":
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__add__(other=other)

    def sub(self, other: Union[Scalar, Series, "Variable"], *args, **kwargs) -> "Variable":
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__sub__(other=other)

    def mul(self, other: Union[Scalar, Series, "Variable"], *args, **kwargs) -> "Variable":
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__mul__(other=other)

    def truediv(self, other: Union[Scalar, Series, "Variable"], *args, **kwargs) -> "Variable":
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__truediv__(other=other)

    def div(self, other: Union[Scalar, Series, "Variable"], *args, **kwargs) -> "Variable":
        return self.truediv(other=other, *args, **kwargs)

    def pct_change(self, *args, **kwargs) -> "Variable":
        variable_name = self.name or UNNAMED_VARIABLE
        variable = Variable(super().pct_change(*args, **kwargs), name=variable_name)
        variable._fields[variable_name] = combine_variables_metadata(
            variables=[self], operation="pct_change", name=variable_name
        )
        return variable

    def update_log(
        self,
        parents: List[Any],
        operation: str,
        variable_name: Optional[str] = None,
        comment: Optional[str] = None,
        inplace: bool = False,
    ) -> Optional["Variable"]:
        return update_log(
            variable=self,
            parents=parents,
            operation=operation,
            variable_name=variable_name,
            comment=comment,
            inplace=inplace,  # type: ignore
        )

    def amend_log(
        self,
        variable_name: Optional[str] = None,
        parents: Optional[List[Any]] = None,
        operation: Optional[str] = None,
        comment: Optional[str] = None,
        entry_num: int = -1,
        inplace: bool = False,
    ) -> Optional["Variable"]:
        return amend_log(
            variable=self,
            variable_name=variable_name,
            parents=parents,
            operation=operation,
            comment=comment,
            entry_num=entry_num,
            inplace=inplace,  # type: ignore
        )

    def copy_metadata(self, from_variable: "Variable", inplace: bool = False) -> Optional["Variable"]:
        return copy_metadata(to_variable=self, from_variable=from_variable, inplace=inplace)  # type: ignore

    def copy(self, deep: bool = True) -> "Variable":
        new_var = super().copy(deep=deep)
        if deep:
            new_var._fields = defaultdict(
                VariableMeta, {k: var_meta.copy(deep=deep) for k, var_meta in self._fields.items()}
            )
        return new_var


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:
    if hasattr(Variable, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Variable, k, metadata_property(k))


def _get_metadata_value_from_variables_if_all_identical(
    variables: List[Variable], field: str, warn_if_different: bool = False
) -> Optional[Any]:
    # Get unique values from list, ignoring Nones.
    unique_values = set(
        [getattr(variable.metadata, field) for variable in variables if getattr(variable.metadata, field) is not None]
    )
    if len(unique_values) == 1:
        combined_value = unique_values.pop()
    else:
        combined_value = None
        if (len(unique_values) > 1) and warn_if_different:
            log.warning(f"Different values of '{field}' detected among variables: {unique_values}")

    return combined_value


def combine_variables_unit(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(
        variables=variables, field="unit", warn_if_different=True
    )


def combine_variables_short_unit(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(
        variables=variables, field="short_unit", warn_if_different=True
    )


def combine_variables_title(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(variables=variables, field="title")


def combine_variables_description(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(variables=variables, field="description")


def combine_variables_description_short(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(variables=variables, field="description_short")


def combine_variables_description_from_producer(variables: List[Variable]) -> Optional[str]:
    return _get_metadata_value_from_variables_if_all_identical(variables=variables, field="description_from_producer")


def get_unique_sources_from_variables(variables: List[Variable]) -> List[Source]:
    # Make a list of all sources of all variables.
    sources = sum([variable.metadata.sources for variable in variables], [])

    return pd.unique(sources).tolist()


def get_unique_origins_from_variables(variables: List[Variable]) -> List[Origin]:
    # Make a list of all origins of all variables.
    origins = sum([variable.metadata.origins for variable in variables], [])

    # Get unique array of tuples of origin fields (respecting the order).
    return pd.unique(origins).tolist()


def get_unique_licenses_from_variables(variables: List[Variable]) -> List[License]:
    # Make a list of all licenses of all variables.
    licenses = sum([variable.metadata.licenses for variable in variables], [])

    return pd.unique(licenses).tolist()


def combine_variables_processing_logs(variables: List[Variable]) -> List[Dict[str, Any]]:
    # Make a list with all entries in the processing log of all variables.
    processing_log = sum(
        [
            variable.metadata.processing_log if variable.metadata.processing_log is not None else []
            for variable in variables
        ],
        [],
    )

    return processing_log


def _get_dict_from_list_if_all_identical(list_of_objects: List[Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
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

    # Return the first dictionary if all dictionaries are identical, otherwise return None.
    return reference_dict if all(d == reference_dict for d in defined_dicts) else None


def combine_variables_display(variables: List[Variable]) -> Optional[Dict[str, Any]]:
    return _get_dict_from_list_if_all_identical(list_of_objects=[variable.metadata.display for variable in variables])


def combine_variables_presentation(variables: List[Variable]) -> Optional[VariablePresentationMeta]:
    return _get_dict_from_list_if_all_identical(
        list_of_objects=[variable.metadata.presentation for variable in variables]  # type: ignore
    )


def combine_variables_processing_level(variables: List[Variable]) -> Optional[PROCESSING_LEVELS]:
    # Gather processing levels from all variables that are defined.
    processing_levels = [
        variable.metadata.processing_level for variable in variables if variable.metadata.processing_level is not None
    ]

    if len(processing_levels) == 0:
        # If there are no processing levels, return None.
        return None

    # Ensure that all processing levels are known.
    unknown_processing_levels = set([level for level in processing_levels]) - set(PROCESSING_LEVELS_ORDER)
    assert len(unknown_processing_levels) == 0, f"Unknown processing levels: {unknown_processing_levels}"

    # If any of the variables has a processing level, take the highest level.
    maximum_level = max([PROCESSING_LEVELS_ORDER[level] for level in processing_levels])

    # Return the maximum level as a string.
    combined_processing_level = {value: key for key, value in PROCESSING_LEVELS_ORDER.items()}[maximum_level]

    return cast(PROCESSING_LEVELS, combined_processing_level)


def combine_variables_metadata(
    variables: List[Any], operation: OPERATION, name: str = UNNAMED_VARIABLE
) -> VariableMeta:
    # Initialise an empty metadata.
    metadata = VariableMeta()

    # Skip other objects passed in variables that may not contain metadata (e.g. a scalar),
    # and skip unnamed variables that cannot have metadata
    variables_only = [v for v in variables if hasattr(v, "name") and v.name and hasattr(v, "metadata")]

    # Combine each metadata field using the logic of the specified operation.
    metadata.title = combine_variables_title(variables=variables_only)
    metadata.description = combine_variables_description(variables=variables_only)
    metadata.description_short = combine_variables_description_short(variables=variables_only)
    metadata.description_from_producer = combine_variables_description_from_producer(variables=variables_only)
    metadata.unit = combine_variables_unit(variables=variables_only)
    metadata.short_unit = combine_variables_short_unit(variables=variables_only)
    metadata.sources = get_unique_sources_from_variables(variables=variables_only)
    metadata.origins = get_unique_origins_from_variables(variables=variables_only)
    metadata.licenses = get_unique_licenses_from_variables(variables=variables_only)
    metadata.processing_log = combine_variables_processing_logs(variables=variables_only)
    metadata.display = combine_variables_display(variables=variables_only)
    metadata.presentation = combine_variables_presentation(variables=variables_only)
    metadata.processing_level = combine_variables_processing_level(variables=variables_only)

    # List names of variables and scalars (or other objects passed in variables).
    variables_and_scalars_names = [
        variable.name if hasattr(variable, "name") else str(variable) for variable in variables
    ]
    metadata.processing_log = add_entry_to_processing_log(
        processing_log=metadata.processing_log,
        variable_name=name,
        parents=variables_and_scalars_names,
        operation=operation,
    )

    return metadata


def add_entry_to_processing_log(
    processing_log: List[Any],
    variable_name: str,
    parents: List[Any],
    operation: str,
    comment: Optional[str] = None,
) -> List[Any]:
    if not PROCESSING_LOG:
        # Avoid any processing and simply return the same input processing log.
        return processing_log

    # Consider using a deepcopy if any of the operations in this function alter mutable objects in processing_log.
    processing_log_updated = copy.deepcopy(processing_log)

    # TODO: Parents currently can be anything. Here we should ensure that they are strings. For example, we could
    # extract the name of the parent if it is a variable.

    # Define new log entry.
    log_new_entry = {"variable": variable_name, "parents": parents, "operation": operation}
    if comment is not None:
        log_new_entry["comment"] = comment

    # Add new entry to log.
    processing_log_updated += [log_new_entry]

    return processing_log_updated


@overload
def update_log(
    variable: Variable,
    parents: List[Any],
    operation: str,
    variable_name: Optional[str] = None,
    comment: Optional[str] = None,
    inplace: Literal[True] = True,
) -> None:
    ...


@overload
def update_log(
    variable: Variable,
    parents: List[Any],
    operation: str,
    variable_name: Optional[str] = None,
    comment: Optional[str] = None,
    inplace: Literal[False] = False,
) -> Variable:
    ...


def update_log(
    variable: Variable,
    parents: List[Any],
    operation: str,
    variable_name: Optional[str] = None,
    comment: Optional[str] = None,
    inplace: bool = False,
) -> Optional[Variable]:
    if not inplace:
        variable = copy.deepcopy(variable)

    if variable_name is None:
        # If a variable name is not specified, take it from the variable, or otherwise use UNNAMED_VARIABLE.
        variable_name = variable.name or UNNAMED_VARIABLE

    # Add new entry to the variable's processing log.
    variable.metadata.processing_log = add_entry_to_processing_log(
        processing_log=variable.metadata.processing_log,
        variable_name=variable_name,
        parents=parents,
        operation=operation,
        comment=comment,
    )

    if not inplace:
        return variable


def amend_entry_in_processing_log(
    processing_log: List[Dict[str, Any]],
    parents: Optional[List[Any]],
    operation: Optional[str],
    variable_name: Optional[str] = None,
    comment: Optional[str] = None,
    entry_num: Optional[int] = -1,
) -> List[Any]:
    if not PROCESSING_LOG:
        # Avoid any processing and simply return the same input processing log.
        return processing_log

    # Consider using a deepcopy if any of the operations in this function alter mutable objects in processing_log.
    processing_log_updated = copy.deepcopy(processing_log)

    fields = {"variable": variable_name, "parents": parents, "operation": operation, "comment": comment}
    for field, value in fields.items():
        if value:
            processing_log_updated[entry_num][field] = value  # type: ignore

    return processing_log_updated


@overload
def amend_log(
    variable: Variable,
    variable_name: Optional[str] = None,
    parents: Optional[List[Any]] = None,
    operation: Optional[str] = None,
    comment: Optional[str] = None,
    entry_num: int = -1,
    inplace: Literal[True] = True,
) -> None:
    ...


@overload
def amend_log(
    variable: Variable,
    variable_name: Optional[str] = None,
    parents: Optional[List[Any]] = None,
    operation: Optional[str] = None,
    comment: Optional[str] = None,
    entry_num: int = -1,
    inplace: Literal[False] = False,
) -> Variable:
    ...


def amend_log(
    variable: Variable,
    variable_name: Optional[str] = None,
    parents: Optional[List[Any]] = None,
    operation: Optional[str] = None,
    comment: Optional[str] = None,
    entry_num: int = -1,
    inplace: bool = False,
) -> Optional[Variable]:
    if not inplace:
        variable = variable.copy()

    variable.metadata.processing_log = amend_entry_in_processing_log(
        processing_log=variable.metadata.processing_log,
        parents=parents,
        operation=operation,
        variable_name=variable_name,
        comment=comment,
        entry_num=entry_num,
    )

    if not inplace:
        return variable


def update_variable_name(variable: Variable, name: str) -> None:
    """Update the name of an unnamed variable, as well as its processing log, to have a new name.

    Say you have a table tb with columns "a" and "b".
    If you create a new variable "c" as
    > variable_c = tb["a"] + tb["b"]
    the new variable will have UNNAMED_VARIABLE as name.
    Also, in the processing log, the variable will be cited as UNNAMED_VARIABLE.
    To change the variable name to something more meaningful (e.g. "c"), the current function can be used,
    > update_variable_name(variable=variable_c, name="c")
    This function will update the variable name (in place) and will replace all instances of UNNAMED_VARIABLE in the
    processing log to the new name.

    This function is already used when a variable is added to a table column, so that
    > tb["c"] = tb["a"] + tb["b"]
    will create a new variable with name "c" (which, in the processing log, will be referred to as "c").

    Parameters
    ----------
    variable : Variable
        Variable whose name is given by UNNAMED_VARIABLE.
    name : str
        New name to assign to the variable.
    """
    if hasattr(variable.metadata, "processing_log") and variable.metadata.processing_log is not None:
        variable.metadata.processing_log = json.loads(
            json.dumps(variable.metadata.processing_log).replace("**TEMPORARY UNNAMED VARIABLE**", name)
        )
    variable.name = name


@overload
def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: Literal[False] = False) -> Variable:
    ...


@overload
def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: Literal[True] = True) -> None:
    ...


def copy_metadata(from_variable: Variable, to_variable: Variable, inplace: bool = False) -> Optional[Variable]:
    if inplace:
        to_variable.metadata = from_variable.metadata.copy()
    else:
        new_variable = to_variable.copy()
        new_variable.metadata = from_variable.metadata.copy()
        return new_variable
