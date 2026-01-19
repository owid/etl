#
#  indicators.py
#
#  Core indicator/variable classes (Indicator was formerly called Variable)
#
from __future__ import annotations

import copy
import json
import os
from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import pandas as pd
import structlog
from pandas._typing import Scalar
from pandas.core.series import Series

if TYPE_CHECKING:
    from owid.catalog.core.tables import Table

from owid.catalog.core import warnings
from owid.catalog.core.meta import (
    PROCESSING_LEVELS,
    PROCESSING_LEVELS_ORDER,
    License,
    Origin,
    Source,
    VariableMeta,
    VariablePresentationMeta,
)
from owid.catalog.core.processing_log import ProcessingLog, enabled
from owid.catalog.core.properties import metadata_property

log = structlog.get_logger()

SCHEMA = json.load(open(os.path.join(os.path.dirname(__file__), "..", "schemas", "table.json")))
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

# NOTE: The following issue seems to not be happening anymore. Consider deleting instances of UNNAMED_INDICATOR.
# When creating a new indicator, might we need to pass a temporary name. For example, when doing tb["a"] + tb["b"]:
#  * If indicator.name is None, a ValueError is raised.
#  * If indicator.name = self.checked_name then the metadata of the first indicator summed ("a") is modified.
#  * If indicator.name is always a random string (that does not coincide with an existing indicator) then
#    when replacing a indicator (e.g. tb["a"] += 1) the original indicator loses its metadata.
# For these reasons, we ensure that indicator.name is always filled, even with a temporary name.
# In fact, if the new indicator becomes a column in a table, its name gets overwritten by the column name (which is a
# nice feature). For example, when doing tb["c"] = tb["a"] + tb["b"], the indicator name of "c" will be "c", even if we
# passed a temporary indicator name. Therefore, this temporary name may be irrelevant in practice.
# Keep the original string for backwards compatibility in processing logs
UNNAMED_INDICATOR = "**TEMPORARY UNNAMED VARIABLE**"
# Backwards-compatible alias
UNNAMED_VARIABLE = UNNAMED_INDICATOR


class Indicator(pd.Series):
    """Enhanced pandas Series with indicator-level metadata support.

    Indicator is a pandas Series subclass that stores rich metadata about individual
    indicators. It serves as the column type in Table objects and automatically
    propagates metadata through operations.

    Note:
        This class was formerly called `Variable`. The old name is still available
        as an alias for backwards compatibility.

    Key features:

    - Automatic metadata propagation through arithmetic operations
    - Processing log tracking for data provenance
    - Integration with OWID catalog metadata system
    - Support for rich metadata including sources, origins, licenses

    Attributes:
        _name: Internal name storage for metadata mapping.
        _fields: Dictionary mapping indicator names to their VariableMeta objects.
        metadata: Indicator-level metadata accessible via `.metadata` or `.m` property.

    Example:
        Create an indicator with metadata:

        ```python
        from owid.catalog import Indicator, VariableMeta

        ind = Indicator(
            [1, 2, 3],
            name="gdp",
            metadata=VariableMeta(
                title="GDP",
                unit="trillion USD",
                description="Gross Domestic Product"
            )
        )
        ```

        Access metadata using shortcuts:

        ```python
        print(ind.metadata.title)  # Full property access
        print(ind.m.title)         # Shorthand alias
        print(ind.title)           # Direct property access
        ```

        Metadata propagates through operations:

        ```python
        gdp_per_capita = ind / population
        # Result combines metadata from both indicators
        ```
    """

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
        """Initialize an Indicator with data and metadata.

        Args:
            data: Array-like data for the indicator (list, numpy array, pandas Series, etc.).
            index: Index labels for the data. If None, uses default integer index.
            name: Name of the indicator. Required if metadata is provided.
            _fields: Internal metadata dictionary. Don't use directly - use `metadata` parameter instead.
            metadata: VariableMeta object with indicator-level metadata (title, unit, sources, etc.).
            **kwargs: Additional arguments passed to `pandas.Series.__init__`.

        Raises:
            AssertionError: If both `metadata` and `_fields` are provided, or if `metadata`
                is provided without a `name`.

        Example:
            Create a simple indicator:

            ```python
            ind = Indicator([1, 2, 3], name="population")
            ```

            Create with metadata:

            ```python
            meta = VariableMeta(
                title="Population",
                unit="people",
                description="Total population"
            )
            ind = Indicator([1e6, 2e6, 3e6], name="population", metadata=meta)
            ```
        """
        if metadata:
            assert not _fields, "cannot pass both metadata and _fields"
            assert name or self.name, "cannot pass metadata without a name"
            _fields = {(name or self.name): metadata}  # type: ignore

        self._fields = _fields or defaultdict(VariableMeta)

        # silence warning
        if data is None and not kwargs.get("dtype"):
            kwargs["dtype"] = "object"

        # DeprecationWarning: Passing a SingleBlockManager to Indicator is deprecated and will raise in a future version. Use public APIs instead.
        with warnings.ignore_warnings([DeprecationWarning]):
            super().__init__(data=data, index=index, name=name, **kwargs)

    @property
    def m(self) -> VariableMeta:
        """Metadata alias for shorter access.

        Provides convenient shorthand access to indicator metadata.

        Returns:
            The indicator's VariableMeta object.

        Example:
            ```python
            # These are equivalent:
            ind.metadata.title
            ind.m.title
            ind.title  # Direct property access
            ```
        """
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
            raise ValueError("indicator must be named to have metadata")

        return self.name

    # which fields should pandas propagate on slicing, etc?
    _metadata = ["_fields", "_name"]

    @property
    def _constructor(self) -> type:
        return Indicator

    @property
    def _constructor_expanddim(self) -> type:
        # XXX lazy circular import
        from owid.catalog import tables

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

    def astype(self, *args: Any, **kwargs: Any) -> Indicator:
        # To fix: https://github.com/owid/owid-catalog-py/issues/12
        v = super().astype(*args, **kwargs)
        v.name = self.name
        return cast(Indicator, v)

    def _repr_html_(self):
        html = str(self)
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-size: 1.5em; font-family: sans-serif; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">indicator</p>
             <pre>{}</pre>
        """.format(self.name, html)

    def __add__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__add__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="+", name=indicator_name)
        return indicator

    def __iadd__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__add__(other)

    def __sub__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__sub__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="-", name=indicator_name)
        return indicator

    def __isub__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__sub__(other)

    def __mul__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__mul__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="*", name=indicator_name)
        return indicator

    def __imul__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__mul__(other)

    def __truediv__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        if is_nullable_series(self) or is_nullable_series(other):
            # 0/0 should return pd.NA, not np.nan
            zero_div_zero = (other == 0) & (self == 0)
            if zero_div_zero.any():
                other = other.replace({0: pd.NA})  # type: ignore

        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__truediv__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="/", name=indicator_name)
        return indicator

    def __itruediv__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__truediv__(other)

    def __floordiv__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__floordiv__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="//", name=indicator_name)
        return indicator

    def __ifloordiv__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__floordiv__(other)

    def __mod__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__mod__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="%", name=indicator_name)
        return indicator

    def __imod__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__mod__(other)

    def __pow__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().__pow__(other), name=indicator_name)
        indicator.metadata = combine_indicators_metadata(indicators=[self, other], operation="**", name=indicator_name)
        return indicator

    def __ipow__(self, other: Scalar | Series | Indicator) -> Indicator:  # type: ignore
        return self.__pow__(other)

    def fillna(self, value=None, *args, **kwargs) -> Indicator:
        # NOTE: Argument "inplace" will modify the original indicator's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            warnings.warn(
                "Avoid using fillna(inplace=True), which may not handle metadata as expected.", warnings.MetadataWarning
            )
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().fillna(value, *args, **kwargs), name=indicator_name)
        indicator._fields = copy.deepcopy(self._fields)
        indicator._fields[indicator_name] = combine_indicators_metadata(
            indicators=[self, value], operation="fillna", name=indicator_name
        )
        return indicator

    def dropna(self, *args, **kwargs) -> Indicator:
        # NOTE: Argument "inplace" will modify the original indicator's data, but not its metadata.
        #  But we should not use "inplace" anyway.
        if "inplace" in kwargs and kwargs["inplace"] is True:
            warnings.warn(
                "Avoid using dropna(inplace=True), which may not handle metadata as expected.", warnings.MetadataWarning
            )
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().dropna(*args, **kwargs), name=indicator_name)
        indicator._fields = copy.deepcopy(self._fields)
        indicator._fields[indicator_name] = combine_indicators_metadata(
            indicators=[self], operation="dropna", name=indicator_name
        )
        return indicator

    def add(self, other: Scalar | Series | Indicator, *args, **kwargs) -> Indicator:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__add__(other=other)

    def sub(self, other: Scalar | Series | Indicator, *args, **kwargs) -> Indicator:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__sub__(other=other)

    def mul(self, other: Scalar | Series | Indicator, *args, **kwargs) -> Indicator:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__mul__(other=other)

    def truediv(self, other: Scalar | Series | Indicator, *args, **kwargs) -> Indicator:  # type: ignore
        if args or kwargs:
            raise NotImplementedError("This feature may exist in pandas, but not in owid.catalog.")
        return self.__truediv__(other=other)

    def div(self, other: Scalar | Series | Indicator, *args, **kwargs) -> Indicator:  # type: ignore
        return self.truediv(other=other, *args, **kwargs)

    def pct_change(self, *args, **kwargs) -> Indicator:
        indicator_name = self.name or UNNAMED_INDICATOR
        indicator = Indicator(super().pct_change(*args, **kwargs), name=indicator_name)
        indicator._fields[indicator_name] = combine_indicators_metadata(
            indicators=[self], operation="pct_change", name=indicator_name
        )
        return indicator

    def set_categories(self, *args, **kwargs) -> Indicator:
        return Indicator(self.cat.set_categories(*args, **kwargs), name=self.name, metadata=self.metadata.copy())

    def update_log(
        self,
        operation: str,
        parents: list[Any] | None = None,
        variable: str | None = None,
        comment: str | None = None,
    ) -> Indicator:
        """Add an entry to the indicator's processing log.

        Records data transformation operations for data provenance tracking.

        Args:
            operation: Name of the operation performed (e.g., "merge", "aggregate").
            parents: List of parent indicators that contributed to this operation.
                If None, uses the indicator itself as the only parent.
            variable: Name of the variable for the log entry. If None, uses the
                indicator's name or UNNAMED_INDICATOR.
            comment: Optional comment describing the operation in detail.

        Returns:
            The indicator itself (for method chaining).

        Example:
            ```python
            # Log a custom transformation
            ind.update_log(
                operation="normalize",
                comment="Normalized to 2015 baseline"
            )

            # Log a merge operation
            result.update_log(
                operation="merge",
                parents=[ind1, ind2],
                comment="Combined GDP and population data"
            )
            ```
        """
        if variable is None:
            # If a variable name is not specified, take it from the indicator, or otherwise use UNNAMED_INDICATOR.
            variable = self.name or UNNAMED_INDICATOR

        if parents is None:
            # If parents are not specified, take the indicator itself as the only parent.
            parents = [self]

        # Add new entry to the indicator's processing log.
        self.metadata.processing_log.add_entry(
            variable=variable,
            parents=parents,
            operation=operation,
            comment=comment,
        )
        return self

    def rolling(self, *args, **kwargs) -> IndicatorRolling:
        """Create a rolling window operation that preserves metadata.

        This method wraps pandas rolling operations while maintaining the indicator's metadata.

        Args:
            *args: Arguments passed to `pandas.Series.rolling`.
            **kwargs: Keyword arguments passed to `pandas.Series.rolling`.

        Returns:
            IndicatorRolling object that applies operations while preserving metadata.

        Example:
            Calculate 7-day rolling average
            ```python
            rolling_avg = ind.rolling(window=7).mean()
            ```

            The result retains the original indicator's metadata
            ```python
            assert rolling_avg.metadata.title == ind.metadata.title
            ```
        """
        return IndicatorRolling(super().rolling(*args, **kwargs), self.metadata.copy(), self.name)  # type: ignore

    def to_frame(self, name: str | None = None) -> Table:
        """Convert Indicator to a Table (single-column table)."""
        # The parent to_frame() already returns a Table via _constructor_expanddim
        # This override just provides proper type hints
        # Don't pass name=None explicitly, as that would make pandas use None as column name
        if name is None:
            return super().to_frame()  # type: ignore[return-value]
        else:
            return super().to_frame(name=name)  # type: ignore[return-value]

    def copy_metadata(self, from_variable: Indicator, inplace: bool = False) -> Indicator | None:
        """Copy metadata from another indicator.

        Args:
            from_variable: Source indicator to copy metadata from.
            inplace: If True, modifies the current indicator. If False, returns a new indicator.

        Returns:
            New indicator with copied metadata if `inplace=False`, otherwise None.

        Example:
            Create new indicator with copied metadata
            ```python
            new_ind = ind1.copy_metadata(from_variable=ind2)
            ```

            Copy metadata in-place
            ```python
            ind1.copy_metadata(from_variable=ind2, inplace=True)
            ```
        """
        return copy_metadata(to_variable=self, from_variable=from_variable, inplace=inplace)  # type: ignore

    def copy(self, deep: bool = True) -> Indicator:
        new_var = super().copy(deep=deep)
        if deep:
            field_names = [n for n in self.index.names + [self.name] if n is not None]
            new_var._fields = defaultdict(VariableMeta, {k: self._fields[k].copy(deep=deep) for k in field_names})
        return new_var


# Backwards-compatible alias
Variable = Indicator


# dynamically add all metadata properties to the class
for k in VariableMeta.__dataclass_fields__:
    if hasattr(Indicator, k):
        raise Exception(f'metadata field "{k}" would overwrite a Pandas built-in')

    setattr(Indicator, k, metadata_property(k))


class IndicatorRolling:
    """Wrapper for pandas rolling window operations that preserves Indicator metadata.

    This class intercepts rolling window operations (mean, sum, std, etc.) and ensures
    that the resulting Indicator retains the original metadata.

    Note:
        This class was formerly called `VariableRolling`.

    Attributes:
        rolling: The underlying pandas Rolling object.
        metadata: Indicator metadata to preserve through operations.
        name: Indicator name to preserve through operations.

    Example:
        Create a rolling average
        ```python
        rolling_avg = ind.rolling(window=7).mean()
        ```

        Metadata is preserved
        ```python
        assert rolling_avg.metadata == ind.metadata
        assert rolling_avg.name == ind.name
        ```

    Note:
        You typically don't instantiate this class directly. Use `Indicator.rolling()` instead.
    """

    # fixes type hints
    __annotations__ = {}

    def __init__(self, rolling: pd.core.window.rolling.Rolling, metadata: VariableMeta, name: str | None = None):
        """Initialize an IndicatorRolling wrapper.

        Args:
            rolling: The pandas Rolling object to wrap.
            metadata: Metadata to preserve through operations.
            name: Indicator name to preserve through operations.
        """
        self.rolling = rolling
        self.metadata = metadata
        self.name = name

    def __getattr__(self, name: str) -> Callable[..., Indicator]:
        """Dynamically wrap rolling methods to return Indicators with metadata.

        Args:
            name: Name of the rolling method (e.g., "mean", "sum", "std").

        Returns:
            A function that applies the rolling operation and returns an Indicator
            with preserved metadata.
        """

        def func(*args, **kwargs):
            """Apply function and return indicator with proper metadata."""
            x = getattr(self.rolling, name)(*args, **kwargs)
            return Indicator(x, name=self.name, metadata=self.metadata)

        self.__annotations__[name] = Callable[..., Indicator]
        return func


def _hash_dict(d):
    return hash(json.dumps(d, sort_keys=True))


def _get_metadata_value_from_indicators_if_all_identical(
    indicators: list[Indicator],
    field: str,
    warn_if_different: bool = False,
    operation: OPERATION | None = None,
) -> Any | None:
    if (operation == "/") and (getattr(indicators[0].metadata, field) is None):
        # When dividing an indicator by another, it only makes sense to keep the metadata values of the first indicator.
        # For example, if we have energy (without description) and population (with a description), when calculating
        # energy per capita, the result shouldn't have the description of population. It should have no description.
        # Therefore, if the first indicator has no metadata value, return None.
        return None

    # Get unique values from list, ignoring Nones.
    if field == "dimensions":
        # TODO: we could make a special object from dimensions and make it hashable
        unique_values = [
            indicator.metadata.dimensions for indicator in indicators if indicator.metadata.dimensions is not None
        ]
        unique_hashes = {_hash_dict(dims) for dims in unique_values}
        if len(unique_hashes) == 1:
            unique_values = unique_values[:1]
    else:
        unique_values = {
            getattr(indicator.metadata, field)
            for indicator in indicators
            if getattr(indicator.metadata, field) is not None
        }

    if len(unique_values) == 1:
        combined_value = unique_values.pop()
    else:
        combined_value = None
        if (len(unique_values) > 1) and (operation not in ["/", "*"]) and warn_if_different:
            # There is no need to warn if units are different when doing a multiplication or a division.
            # In most cases, units will be different, and that is fine, as long as the resulting indicator has no units.
            # Note that the same reasoning can be applied to other operations, so we may need to generalize this logic.
            warnings.warn(
                f"Different values of '{field}' detected among indicators: {unique_values}",
                warnings.DifferentValuesWarning,
            )

    return combined_value


# Backwards-compatible alias
_get_metadata_value_from_variables_if_all_identical = _get_metadata_value_from_indicators_if_all_identical


def get_unique_sources_from_indicators(indicators: list[Indicator]) -> list[Source]:
    """Get unique sources from a list of indicators.

    Collects all unique Source objects from the metadata of multiple indicators,
    preserving order of first occurrence.

    Args:
        indicators: List of Indicator objects to extract sources from.

    Returns:
        List of unique Source objects in order of first appearance.

    Example:
        ```python
        sources = get_unique_sources_from_indicators([ind1, ind2, ind3])
        print(f"Combined {len(sources)} unique sources")
        ```
    """
    # Make a list of all sources of all indicators.
    sources = []
    for indicator in indicators:
        sources += [s for s in indicator.metadata.sources if s not in sources]
    return sources


def get_unique_origins_from_indicators(indicators: list[Indicator]) -> list[Origin]:
    """Get unique origins from a list of indicators.

    Collects all unique Origin objects from the metadata of multiple indicators,
    preserving order of first occurrence.

    Args:
        indicators: List of Indicator objects to extract origins from.

    Returns:
        List of unique Origin objects in order of first appearance.

    Example:
        ```python
        origins = get_unique_origins_from_indicators([ind1, ind2, ind3])
        for origin in origins:
            print(f"Producer: {origin.producer}")
        ```
    """
    # Make a list of all origins of all indicators.
    origins = []
    for indicator in indicators:
        # Get unique array of tuples of origin fields (respecting the order).
        origins += [o for o in indicator.metadata.origins if o not in origins]
    return origins


def get_unique_licenses_from_indicators(indicators: list[Indicator]) -> list[License]:
    """Get unique licenses from a list of indicators.

    Collects all unique License objects from the metadata of multiple indicators,
    preserving order of first occurrence.

    Args:
        indicators: List of Indicator objects to extract licenses from.

    Returns:
        List of unique License objects in order of first appearance.

    Example:
        ```python
        licenses = get_unique_licenses_from_indicators([ind1, ind2, ind3])
        print(f"Data uses {len(licenses)} different licenses")
        ```
    """
    # Make a list of all licenses of all indicators.
    licenses = []
    for indicator in indicators:
        licenses += [license for license in indicator.metadata.licenses if license not in licenses]
    return licenses


def get_unique_description_key_points_from_indicators(indicators: list[Indicator]) -> list[str]:
    """Get unique description key points from a list of indicators.

    Collects all unique key points from the description_key field of multiple indicators,
    preserving order of first occurrence.

    Args:
        indicators: List of Indicator objects to extract description key points from.

    Returns:
        List of unique description key points in order of first appearance.

    Example:
        ```python
        key_points = get_unique_description_key_points_from_indicators([ind1, ind2])
        for point in key_points:
            print(f"- {point}")
        ```
    """
    # Make a list of all description key points of all indicators.
    description_key_points = []
    for indicator in indicators:
        description_key_points += [k for k in indicator.metadata.description_key if k not in description_key_points]
    return description_key_points


# Backwards-compatible alias
get_unique_description_key_points_from_variables = get_unique_description_key_points_from_indicators


def combine_indicators_processing_logs(
    indicators: list[Indicator] | None = None,
    *,
    variables: list[Indicator] | None = None,
) -> ProcessingLog:
    """Combine processing logs from multiple indicators.

    Merges all processing log entries from the provided indicators into a single
    ProcessingLog object, maintaining chronological order.

    Args:
        indicators: List of Indicator objects whose processing logs should be combined.
        variables: Deprecated alias for indicators parameter (for backwards compatibility).

    Returns:
        ProcessingLog object containing all entries from all indicators.

    Example:
        ```python
        combined_log = combine_indicators_processing_logs([ind1, ind2, ind3])
        print(f"Combined log has {len(combined_log)} entries")
        ```
    """
    # Support both parameter names for backwards compatibility
    if indicators is None and variables is not None:
        indicators = variables
    elif indicators is None:
        indicators = []

    # Make a list with all entries in the processing log of all indicators.
    processing_log = sum(
        [
            indicator.metadata.processing_log if indicator.metadata.processing_log is not None else []
            for indicator in indicators
        ],
        [],
    )

    return ProcessingLog(processing_log)  # type: ignore


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


def combine_indicators_display(
    indicators: list[Indicator], operation: OPERATION | None, _field_name="display"
) -> dict[str, Any] | None:
    # Gather displays from all indicators that are defined.
    list_of_displays = [getattr(indicator.metadata, _field_name) for indicator in indicators]
    if operation == "/" and list_of_displays and list_of_displays[0] is None:
        # When dividing an indicator by another, it only makes sense to keep the display values of the first indicator.
        # Therefore, if the first indicators doesn't have a display, the resulting indicator should have no display.
        return None
    else:
        return _get_dict_from_list_if_all_identical(list_of_objects=list_of_displays)


# Backwards-compatible alias
combine_variables_display = combine_indicators_display


def combine_indicators_presentation(
    indicators: list[Indicator], operation: OPERATION | None
) -> VariablePresentationMeta | None:
    # Apply the same logic as for displays.
    return combine_indicators_display(indicators=indicators, operation=operation, _field_name="presentation")  # type: ignore


# Backwards-compatible alias
combine_variables_presentation = combine_indicators_presentation


def combine_indicators_processing_level(indicators: list[Indicator]) -> PROCESSING_LEVELS | None:
    # Gather processing levels from all indicators that are defined.
    processing_levels = [
        indicator.metadata.processing_level
        for indicator in indicators
        if indicator.metadata.processing_level is not None
    ]

    if len(processing_levels) == 0:
        # If there are no processing levels, return None.
        return None

    # Ensure that all processing levels are known.
    unknown_processing_levels = {level for level in processing_levels} - set(PROCESSING_LEVELS_ORDER)
    assert len(unknown_processing_levels) == 0, f"Unknown processing levels: {unknown_processing_levels}"

    # If any of the indicators has a processing level, take the highest level.
    maximum_level = max([PROCESSING_LEVELS_ORDER[level] for level in processing_levels])

    # Return the maximum level as a string.
    combined_processing_level = {value: key for key, value in PROCESSING_LEVELS_ORDER.items()}[maximum_level]

    return cast(PROCESSING_LEVELS, combined_processing_level)


# Backwards-compatible alias
combine_variables_processing_level = combine_indicators_processing_level


def combine_indicators_sort(indicators: list[Indicator]) -> list[str]:
    # Return sort if all indicators have the same sort, otherwise return empty list.
    sorts = [indicator.metadata.sort for indicator in indicators if indicator.metadata.sort]
    if not sorts:
        return []
    else:
        return sorts[0] if all(sort == sorts[0] for sort in sorts) else []


def combine_indicators_metadata(
    indicators: list[Any] | None = None,
    operation: OPERATION | None = None,
    name: str = UNNAMED_INDICATOR,
    *,
    variables: list[Any] | None = None,
) -> VariableMeta:
    """Combine metadata from multiple indicators based on an operation.

    This function intelligently merges metadata from multiple indicators when they are
    combined through operations like addition, division, etc. The logic varies by field:

    - If all indicators have identical values for a field, that value is preserved
    - For lists (sources, origins, licenses), all unique values are combined
    - For some operations (e.g., division), only the first indicator's metadata is kept
    - Processing logs are merged and a new entry is added for the operation

    Args:
        indicators: List of indicators (or other objects) to combine metadata from.
            Non-Indicator objects are automatically filtered out.
        operation: Type of operation being performed ("+", "-", "*", "/", etc.).
            Affects how metadata fields are combined.
        name: Name for the resulting indicator. Defaults to UNNAMED_INDICATOR.
        variables: Deprecated alias for indicators parameter (for backwards compatibility).

    Returns:
        Combined VariableMeta object with merged metadata from all indicators.

    Example:
        Metadata from addition
        ```python
        result_meta = combine_indicators_metadata(
            indicators=[ind1, ind2],
            operation="+",
            name="sum"
        )
        ```

        Metadata from division (keeps first indicator's metadata)
        ```python
        ratio_meta = combine_indicators_metadata(
            indicators=[numerator, denominator],
            operation="/",
            name="ratio"
        )
        ```

    Note:
        This function is typically called automatically by Indicator arithmetic operations.
        You rarely need to call it directly.
    """
    # Support both parameter names for backwards compatibility
    if indicators is None and variables is not None:
        indicators = variables
    elif indicators is None:
        indicators = []

    # Initialise an empty metadata.
    metadata = VariableMeta()

    # Skip other objects passed in indicators that may not contain metadata (e.g. a scalar),
    # and skip unnamed indicators that cannot have metadata
    indicators_only = [v for v in indicators if hasattr(v, "name") and v.name and hasattr(v, "metadata")]

    # Combine each metadata field using the logic of the specified operation.
    metadata.title = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="title", operation=operation
    )
    metadata.description = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="description", operation=operation
    )
    metadata.description_short = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="description_short", operation=operation
    )
    metadata.description_key = get_unique_description_key_points_from_indicators(indicators=indicators_only)
    # TODO: Combine description_processing: If not identical, append one after another.
    metadata.description_from_producer = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="description_from_producer", operation=operation
    )
    metadata.unit = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="unit", operation=operation, warn_if_different=True
    )
    metadata.short_unit = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="short_unit", operation=operation, warn_if_different=True
    )
    metadata.sources = get_unique_sources_from_indicators(indicators=indicators_only)
    metadata.origins = get_unique_origins_from_indicators(indicators=indicators_only)
    metadata.licenses = get_unique_licenses_from_indicators(indicators=indicators_only)
    metadata.display = combine_indicators_display(indicators=indicators_only, operation=operation)
    metadata.presentation = combine_indicators_presentation(indicators=indicators_only, operation=operation)
    metadata.processing_level = combine_indicators_processing_level(indicators=indicators_only)

    metadata.type = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="type", operation=operation, warn_if_different=True
    )
    metadata.sort = combine_indicators_sort(indicators=indicators_only)
    metadata.license = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="license", operation=operation, warn_if_different=True
    )
    metadata.dimensions = _get_metadata_value_from_indicators_if_all_identical(
        indicators=indicators_only, field="dimensions", operation=operation, warn_if_different=True
    )

    if enabled():
        metadata.processing_log = combine_indicators_processing_logs(indicators=indicators_only)
        if operation:
            metadata.processing_log.add_entry(
                variable=name,
                parents=indicators,
                operation=operation,
            )

    return metadata


@overload
def copy_metadata(from_variable: Indicator, to_variable: Indicator, inplace: Literal[False] = False) -> Indicator: ...


@overload
def copy_metadata(from_variable: Indicator, to_variable: Indicator, inplace: Literal[True] = True) -> None: ...


def copy_metadata(from_variable: Indicator, to_variable: Indicator, inplace: bool = False) -> Indicator | None:
    """Copy metadata from one indicator to another.

    Args:
        from_variable: Source indicator to copy metadata from.
        to_variable: Target indicator to copy metadata to.
        inplace: If True, modifies `to_variable` in place. If False, returns a new indicator.

    Returns:
        New indicator with copied metadata if `inplace=False`, otherwise None.

    Example:
        Create new indicator with copied metadata
        ```python
        new_ind = copy_metadata(from_variable=source, to_variable=target)
        ```

        Copy metadata in-place
        ```python
        copy_metadata(from_variable=source, to_variable=target, inplace=True)
        ```
    """
    if inplace:
        to_variable.metadata = from_variable.metadata.copy()
    else:
        new_variable = to_variable.copy()
        new_variable.metadata = from_variable.metadata.copy()
        return new_variable


def is_nullable_series(s: Any) -> bool:
    """Check if a series has a nullable pandas dtype.

    Determines whether a pandas Series uses one of the nullable integer, float, or
    boolean dtypes (as opposed to traditional numpy dtypes).

    Args:
        s: Any object to check. Typically a pandas Series.

    Returns:
        True if the object has a nullable pandas dtype, False otherwise.

    Example:
        ```python
        import pandas as pd

        # Nullable integer dtype
        s1 = pd.Series([1, 2, None], dtype="Int64")
        assert is_nullable_series(s1) == True

        # Traditional numpy dtype
        s2 = pd.Series([1, 2, 3], dtype="int64")
        assert is_nullable_series(s2) == False

        # Nullable boolean dtype
        s3 = pd.Series([True, False, None], dtype="boolean")
        assert is_nullable_series(s3) == True
        ```

    Note:
        Nullable dtypes (capitalized like `Int64`) differ from numpy dtypes (`int64`)
        in that they can represent missing values using `pd.NA` instead of `np.nan`.
    """
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
