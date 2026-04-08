"""Objects related to pandas dataframes."""

import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
from pandas.api.types import union_categoricals

from owid.datautils.common import ExceptionFromDocstring, warn_on_list_of_entities
from owid.datautils.io.df import to_file as to_file_


# Backwards compatibility
def to_file(*args: Any, **kwargs: Any) -> None:
    """Save a dataframe in any format.

    Will be deprecated. Use owid.datautils.io.df.to_file instead.
    """
    warnings.warn(
        "Call to deprecated class to_file (This function will be removed in the next"
        " minor update, use owid.datautils.io.df_to_file instead.)",
        category=DeprecationWarning,
        stacklevel=2,
    )
    to_file_(*args, **kwargs)


def has_index(df: pd.DataFrame) -> bool:
    """Check if a DataFrame has a meaningful index.

    Determines whether a DataFrame has an actual index set, or just the
    default dummy integer index created by pandas.

    Args:
        df: DataFrame to check for index.

    Returns:
        True if DataFrame has a non-dummy (single or multi) index, False otherwise.

    Example:
        ```python
        import pandas as pd
        from owid.datautils import has_index

        # DataFrame with dummy index
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        print(has_index(df1))  # False

        # DataFrame with actual index
        df2 = df1.set_index("a")
        print(has_index(df2))  # True
        ```
    """
    # Dataframes always have an attribute index.names, which is a frozen list.
    # If the dataframe has no set index (i.e. if it has a dummy index), that list contains only [None].
    # In any other case, the frozen list contains one or more elements different than None.
    df_has_index = True if df.index.names[0] is not None else False

    return df_has_index


class DataFramesHaveDifferentLengths(ExceptionFromDocstring):
    """Dataframes cannot be compared because they have different number of rows."""


class ObjectsAreNotDataframes(ExceptionFromDocstring):
    """Given objects are not dataframes."""


def compare(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    columns: Optional[List[str]] = None,
    absolute_tolerance: float = 1e-8,
    relative_tolerance: float = 1e-8,
) -> pd.DataFrame:
    """Compare two DataFrames element-wise for equality.

    Performs element-by-element comparison of two DataFrames, treating NaN values
    as equal and allowing tolerance for floating-point comparisons.

    Args:
        df1: First DataFrame to compare.
        df2: Second DataFrame to compare.
        columns: List of column names to compare (must exist in both DataFrames).
            If None, all common columns are compared.
        absolute_tolerance: Maximum absolute difference allowed for values to be
            considered equal: `abs(a - b) <= absolute_tolerance`.
        relative_tolerance: Maximum relative difference allowed for values to be
            considered equal: `abs(a - b) / abs(b) <= relative_tolerance`.

    Returns:
        DataFrame of booleans with the same shape as the comparison. Each element
        is True if the corresponding values in df1 and df2 are equal (within tolerance).

    Raises:
        ObjectsAreNotDataframes: If either input is not a DataFrame.
        DataFramesHaveDifferentLengths: If DataFrames have different row counts.

    Example:
        ```python
        import pandas as pd
        from owid.datautils.dataframes import compare

        df1 = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        df2 = pd.DataFrame({"a": [1.0001, 2.0], "b": [3.0, 4.1]})

        result = compare(df1, df2, absolute_tolerance=0.01)
        print(result)
        #       a      b
        # 0  True   True
        # 1  True  False
        ```

    Note:
        DataFrames must have the same number of rows to be compared.
    """
    # Ensure dataframes can be compared.
    if (not isinstance(df1, pd.DataFrame)) or (not isinstance(df2, pd.DataFrame)):
        raise ObjectsAreNotDataframes
    if len(df1) != len(df2):
        raise DataFramesHaveDifferentLengths

    # If columns are not specified, assume common columns.
    if columns is None:
        columns = sorted(set(df1.columns) & set(df2.columns))

    # Compare, column by column, the elements of the two dataframes.
    compared = pd.DataFrame()
    for col in columns:
        if (df1[col].dtype in (object, "category", "string")) or (df2[col].dtype in (object, "category", "string")):
            # Apply a direct comparison for strings or categories
            compared_row = df1[col].values == df2[col].values
        else:
            # For numeric data, consider them equal within certain absolute and relative tolerances.
            compared_row = np.isclose(
                df1[col].values,  # type: ignore
                df2[col].values,  # type: ignore
                atol=absolute_tolerance,
                rtol=relative_tolerance,
            )
        # Treat nans as equal.
        compared_row[pd.isnull(df1[col].values) & pd.isnull(df2[col].values)] = True  # type: ignore
        compared[col] = compared_row

    return compared


def are_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    absolute_tolerance: float = 1e-8,
    relative_tolerance: float = 1e-8,
    verbose: bool = True,
) -> Tuple[bool, pd.DataFrame]:
    """Check if two DataFrames are equal with detailed comparison report.

    Comprehensive equality check that compares structure, dtypes, and values
    with tolerance for floating-point numbers. Treats all NaN values as equal.
    Optionally prints a detailed summary of differences.

    Args:
        df1: First DataFrame to compare.
        df2: Second DataFrame to compare.
        absolute_tolerance: Maximum absolute difference for numeric equality:
            `abs(a - b) <= absolute_tolerance`.
        relative_tolerance: Maximum relative difference for numeric equality:
            `abs(a - b) / abs(b) <= relative_tolerance`.
        verbose: If True, print detailed comparison summary showing all
            differences found.

    Returns:
        Tuple of (equality_flag, comparison_dataframe) where:
            - equality_flag: True if DataFrames are equal within tolerance
            - comparison_dataframe: Boolean DataFrame showing element-wise
              equality. Empty if DataFrames have incompatible shapes.

    Example:
        ```python
        import pandas as pd
        from owid.datautils.dataframes import are_equal

        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        equal, comparison = are_equal(df1, df2, verbose=True)
        # Prints: "Dataframes are identical..."
        # Returns: (True, DataFrame of all True values)

        df3 = pd.DataFrame({"a": [1, 2], "c": [5, 6]})
        equal, comparison = are_equal(df1, df3, verbose=True)
        # Prints differences: missing columns, etc.
        # Returns: (False, DataFrame)
        ```
    """
    # Initialise flag that is True only if both dataframes are equal.
    equal = True
    # Initialise flag that is True if dataframes can be compared cell by cell.
    can_be_compared = True
    # Initialise string of messages, which will optionally be printed.
    summary = ""

    # Check if all columns in df2 are in df1.
    missing_in_df1 = sorted(set(df2.columns) - set(df1.columns))
    if len(missing_in_df1):
        summary += f"\n* {len(missing_in_df1)} columns in df2 missing in df1.\n"
        summary += "\n".join([f"  * {col}" for col in missing_in_df1])
        equal = False

    # Check if all columns in df1 are in df2.
    missing_in_df2 = sorted(set(df1.columns) - set(df2.columns))
    if len(missing_in_df2):
        summary += f"\n* {len(missing_in_df2)} columns in df1 missing in df2.\n"
        summary += "\n".join([f"  * {col}" for col in missing_in_df2])
        equal = False

    # Check if dataframes have the same number of rows.
    if len(df1) != len(df2):
        summary += f"\n* {len(df1)} rows in df1 and {len(df2)} rows in df2."
        equal = False
        can_be_compared = False

    # Check for differences in column names or types.
    common_columns = sorted(set(df1.columns) & set(df2.columns))
    all_columns = sorted(set(df1.columns) | set(df2.columns))
    if common_columns == all_columns:
        if df1.columns.tolist() != df2.columns.tolist():
            summary += "\n* Columns are sorted differently.\n"
            equal = False
        for col in common_columns:
            if df1[col].dtype != df2[col].dtype:
                summary += (
                    f"  * Column {col} is of type {df1[col].dtype} for df1, but type" f" {df2[col].dtype} for df2."
                )
                equal = False
    else:
        summary += f"\n* Only {len(common_columns)} common columns out of" f" {len(all_columns)} distinct columns."
        equal = False

    if not can_be_compared:
        # Dataframes cannot be compared.
        compared = pd.DataFrame()
        equal = False
    else:
        # Check if indexes are equal.
        if (df1.index != df2.index).any():
            summary += "\n* Dataframes have different indexes (consider resetting indexes of" " input dataframes)."
            equal = False

        # Dataframes can be compared cell by cell (two nans on the same cell are considered equal).
        compared = compare(
            df1,
            df2,
            columns=common_columns,
            absolute_tolerance=absolute_tolerance,
            relative_tolerance=relative_tolerance,
        )
        all_values_equal = compared.all().all()  # type: ignore
        if not all_values_equal:
            summary += "\n* Values differ by more than the given absolute and relative" " tolerances."

        # Dataframes are equal only if all previous checks have passed.
        equal = equal & all_values_equal

    if equal:
        summary += (
            "Dataframes are identical (within absolute tolerance of"
            f" {absolute_tolerance} and relative tolerance of {relative_tolerance})."
        )

    if verbose:
        # Optionally print the summary of the comparison.
        print(summary)

    return equal, compared


def _calculate_weighted_mean(
    group_data: pd.DataFrame,
    value_col: str,
    weight_col: str,
    num_allowed_nans: int | None = None,
    frac_allowed_nans: float | None = None,
    min_num_values: int | None = None,
) -> float:
    """Calculate weighted mean for a group, applying NaN handling rules."""
    values = group_data[value_col]
    weights = group_data[weight_col]

    # Apply same NaN handling logic as regular aggregations
    total_count = len(values)
    mask = ~(pd.isna(values) | pd.isna(weights) | (weights == 0))
    valid_values = values[mask]
    valid_weights = weights[mask]
    valid_count = len(valid_values)
    nan_count = total_count - valid_count

    # Apply NaN handling rules to follow the same logic as for the non-weighted aggregations (defined in groupby_agg).
    if (num_allowed_nans is not None) and (nan_count > num_allowed_nans):
        return np.nan
    if (frac_allowed_nans is not None) and (total_count > 0) and (nan_count / total_count > frac_allowed_nans):
        return np.nan
    if (min_num_values is not None) and (valid_count < min_num_values) and (nan_count > 0):
        return np.nan
    if len(valid_values) == 0:
        return np.nan
    return float(np.average(valid_values, weights=valid_weights))


def groupby_agg(
    df: pd.DataFrame,
    groupby_columns: Union[List[str], str],
    aggregations: Optional[Dict[str, Any]] = None,
    num_allowed_nans: Optional[int] = None,
    frac_allowed_nans: Optional[float] = None,
    min_num_values: Optional[int] = None,
) -> pd.DataFrame:
    """Group DataFrame with intelligent NaN handling during aggregation.

    Enhanced version of `pandas.DataFrame.groupby().agg()` that provides control over
    how NaN values are treated during aggregation. By default, pandas ignores NaNs,
    which can produce misleading results (e.g., treating NaNs as zeros in sums).

    This function supports weighted aggregations using the special syntax
    `mean_weighted_by_<column_name>` for any aggregation.

    Behavior:
        - When all NaN parameters are None: behaves like standard pandas groupby
        - When any NaN parameter is set: applies sequential validation rules

        NaN Handling Rules (applied in order):

            1. If `num_allowed_nans` is set: group becomes NaN if it has more NaNs
            2. If `frac_allowed_nans` is set: group becomes NaN if NaN fraction exceeds threshold
            3. If `min_num_values` is set: group becomes NaN if valid values < threshold

    Args:
        df: Source DataFrame to group and aggregate.
        groupby_columns: Column name(s) to group by. Can be a single string or list.
        aggregations: Dictionary mapping column names to aggregation functions.
            If None, applies 'sum' to all columns. Supports weighted means with
            syntax: `{'col': 'mean_weighted_by_weight_col'}`.
        num_allowed_nans: Maximum number of NaN values allowed in a group before
            the aggregate becomes NaN.
        frac_allowed_nans: Maximum fraction of NaN values allowed (0.0-1.0).
            Group becomes NaN if NaN fraction exceeds this threshold.
        min_num_values: Minimum number of non-NaN values required. Group becomes
            NaN if it has fewer valid values (and at least one NaN).

    Returns:
        Grouped and aggregated DataFrame with NaN handling applied.

    Example:
        Basic groupby with NaN control
        ```python
        import pandas as pd
        from owid.datautils.dataframes import groupby_agg

        df = pd.DataFrame({
            "country": ["USA", "USA", "UK", "UK"],
            "year": [2020, 2021, 2020, 2021],
            "value": [100, None, 200, 300]
        })

        # Standard pandas sum treats NaN as 0
        # result = df.groupby("country").sum()  # USA: 100

        # With min_num_values=1, NaN if all values are NaN
        result = groupby_agg(
            df,
            groupby_columns="country",
            aggregations={"value": "sum"},
            min_num_values=1
        )
        # USA: 100 (has 1 valid value), UK: 500 (has 2 valid values)
        ```

        Weighted mean aggregation
        ```python
        df = pd.DataFrame({
            "country": ["USA", "USA", "UK"],
            "value": [10, 20, 30],
            "population": [100, 200, 300]
        })

        result = groupby_agg(
            df,
            groupby_columns="country",
            aggregations={"value": "mean_weighted_by_population"}
        )
        # USA: 16.67 = (10*100 + 20*200)/(100+200)
        ```

    Note:
        Does not support multiple aggregations for the same column
        (e.g., `{'a': ('sum', 'mean')}`).
    """
    if isinstance(groupby_columns, str):
        groupby_columns = [groupby_columns]

    if aggregations is None:
        columns_to_aggregate = [column for column in df.columns if column not in groupby_columns]
        aggregations = {column: "sum" for column in columns_to_aggregate}

    # Default groupby arguments, `observed` makes sure the final dataframe
    # does not explode with NaNs
    groupby_kwargs = {
        "dropna": False,
        "observed": True,
    }

    # Handle weighted aggregations separately if any are present
    weighted_aggregations = {
        k: v for k, v in aggregations.items() if isinstance(v, str) and v.startswith("mean_weighted_by_")
    }

    if weighted_aggregations:
        # Split out regular aggregations to handle them normally
        regular_aggregations = {
            k: v for k, v in aggregations.items() if not (isinstance(v, str) and v.startswith("mean_weighted_by_"))
        }

        # Handle regular aggregations first (if any)
        if regular_aggregations:
            grouped = df.groupby(groupby_columns, **groupby_kwargs).agg(regular_aggregations)  # type: ignore
        else:
            # Create empty DataFrame with proper groupby index for weighted-only case
            grouped = (
                df.groupby(groupby_columns, dropna=False, observed=True)
                .size()
                .to_frame("_temp")
                .drop(columns=["_temp"])
            )

        # Add weighted mean columns
        for col, agg_func in weighted_aggregations.items():
            weight_col = agg_func.replace("mean_weighted_by_", "")
            if weight_col not in df.columns:
                raise ValueError(f"Weight column '{weight_col}' not found in data")

            weighted_results = df.groupby(groupby_columns, dropna=False, observed=True).apply(
                lambda group: _calculate_weighted_mean(
                    group, col, weight_col, num_allowed_nans, frac_allowed_nans, min_num_values
                ),
                include_groups=False,
            )
            grouped[col] = weighted_results  # type: ignore
    else:
        # No weighted aggregations; use standard grouping logic
        grouped = df.groupby(groupby_columns, **groupby_kwargs).agg(aggregations)  # type: ignore

    # Calculate a few necessary parameters related to the number of nans and valid elements.
    if (num_allowed_nans is not None) or (frac_allowed_nans is not None) or (min_num_values is not None):
        # Count the number of missing values in each group.
        num_nans_detected = count_missing_in_groups(df, groupby_columns, **groupby_kwargs)
    if (frac_allowed_nans is not None) or (min_num_values is not None):
        # Count number of total elements in each group (counting both nans and non-nan values).
        num_elements = df.groupby(groupby_columns, **groupby_kwargs).size()  # type: ignore

    # Apply conditions sequentially.
    if num_allowed_nans is not None:
        # Make nan any aggregation where there were too many missing values.
        grouped = grouped[num_nans_detected <= num_allowed_nans]  # type: ignore

    if frac_allowed_nans is not None:
        # Make nan any aggregation where there were too many missing values.
        grouped = grouped[num_nans_detected.divide(num_elements, axis="index") <= frac_allowed_nans]  # type: ignore

    if min_num_values is not None:
        # Make nan any aggregation where there were too few valid (non-nan) values.
        # The number of valid values is the number of elements minus the number of nans. So, a priori, what we need is:
        # grouped = grouped[(-num_nans_detected.subtract(num_elements, axis="index") >= min_num_values)]
        # However, if a group has fewer elements than min_num_values, the condition is not fulfilled, and the aggregate
        # is nan. But that is probably not the desired behavior. Instead, if all elements in a group are valid, the
        # aggregate should exist, even if that number of valid values is smaller than min_num_values.
        # Therefore, we impose that either the number of valid values is >= min_num_values, or that there are no nans
        # (and hence all values are valid).
        grouped = grouped[
            (-num_nans_detected.subtract(num_elements, axis="index") >= min_num_values) | (num_nans_detected == 0)  # type: ignore
        ]

    return cast(pd.DataFrame, grouped)


def count_missing_in_groups(df: pd.DataFrame, groupby_columns: List[str], **kwargs: Any) -> pd.DataFrame:
    """Count the number of missing values in each group.

    This is equivalent but faster than:

    ```python
    num_nans_detected = df.groupby(groupby_columns, **groupby_kwargs).agg(
        lambda x: pd.isnull(x).sum()
    )
    ```

    """
    nan_columns = [c for c in df.columns if c not in groupby_columns]

    num_nans_detected = df[nan_columns].isnull().groupby([df[c] for c in groupby_columns], **kwargs).sum()

    return cast(pd.DataFrame, num_nans_detected)


def multi_merge(dfs: List[pd.DataFrame], on: Union[List[str], str], how: str = "inner") -> pd.DataFrame:
    """Merge multiple DataFrames on common columns.

    Convenience function for merging more than two DataFrames sequentially.
    Equivalent to chaining multiple `pd.merge()` calls.

    Args:
        dfs: List of DataFrames to merge.
        on: Column name(s) to merge on. Must exist in all DataFrames with
            the same name.
        how: Type of merge to perform. Options: 'inner', 'outer', 'left', 'right'.
            Default is 'inner'.

    Returns:
        Merged DataFrame containing all input DataFrames joined on specified columns.

    Example:
        ```python
        import pandas as pd
        from owid.datautils.dataframes import multi_merge

        df1 = pd.DataFrame({"country": ["USA", "UK"], "gdp": [20, 3]})
        df2 = pd.DataFrame({"country": ["USA", "UK"], "pop": [330, 67]})
        df3 = pd.DataFrame({"country": ["USA", "UK"], "area": [9.8, 0.24]})

        result = multi_merge([df1, df2, df3], on="country")
        #   country  gdp  pop  area
        # 0     USA   20  330  9.80
        # 1      UK    3   67  0.24
        ```
    """
    merged = dfs[0].copy()
    for df in dfs[1:]:
        merged = pd.merge(merged, df, how=how, on=on)  # type: ignore

    return merged


def map_series(
    series: pd.Series,
    mapping: Dict[Any, Any],
    make_unmapped_values_nan: bool = False,
    warn_on_missing_mappings: bool = False,
    warn_on_unused_mappings: bool = False,
    show_full_warning: bool = False,
) -> pd.Series:
    """Map Series values with performance optimization and flexible NaN handling.

    Enhanced version of `pandas.Series.map()` that:

    - Preserves unmapped values instead of converting to NaN (optional)
    - Much faster than `Series.replace()` for large DataFrames
    - Supports categorical Series with automatic category management
    - Provides warnings for missing or unused mappings

    Behavior differences from `pandas.Series.map()`:

        - Default: unmapped values keep original values (not NaN)
        - With `make_unmapped_values_nan=True`: same as `Series.map()`

    Args:
        series: Series to map values from.
        mapping: Dictionary mapping old values to new values.
        make_unmapped_values_nan: If True, unmapped values become NaN.
            If False, they retain original values.
        warn_on_missing_mappings: If True, warn about values in Series
            that don't exist in mapping.
        warn_on_unused_mappings: If True, warn about mapping entries
            not used by any value in Series.
        show_full_warning: If True, print full list of missing/unused
            values in warnings.

    Returns:
        Series with mapped values.

    Example:
        Basic mapping
        ```python
        import pandas as pd
        from owid.datautils.dataframes import map_series

        series = pd.Series(["usa", "uk", "france"])
        mapping = {"usa": "United States", "uk": "United Kingdom"}

        # Default: unmapped values preserved
        result = map_series(series, mapping)
        # ["United States", "United Kingdom", "france"]

        # With NaN for unmapped
        result = map_series(series, mapping, make_unmapped_values_nan=True)
        # ["United States", "United Kingdom", NaN]
        ```

        With warnings
        ```python
        result = map_series(
            series,
            mapping,
            warn_on_missing_mappings=True,  # Warns about "france"
            warn_on_unused_mappings=True    # Warns if mapping has unused keys
        )
        ```
    """
    # If given category, only map category names and return category type.
    if series.dtype == "category":
        # Remove unused categories in input series.
        series = series.cat.remove_unused_categories()

        new_categories = map_series(
            pd.Series(series.cat.categories),
            mapping=mapping,
            make_unmapped_values_nan=make_unmapped_values_nan,
            warn_on_missing_mappings=warn_on_missing_mappings,
            warn_on_unused_mappings=warn_on_unused_mappings,
            show_full_warning=show_full_warning,
        )
        category_mapping = dict(zip(series.cat.categories, new_categories))
        return rename_categories(series, category_mapping)

    # Translate values in series following the mapping.
    series_mapped = series.map(mapping)
    if not make_unmapped_values_nan:
        # Rows that had values that were not in the mapping are now nan.
        # Replace those nans with their original values, except if they were actually meant to be mapped to nan.
        # For example, if {"bad_value": np.nan} was part of the mapping, do not replace those nans back to "bad_value".

        # if we are setting values from the original series, ensure we have the same dtype
        try:
            series_mapped = series_mapped.astype(series.dtype, copy=False)
        except ValueError:
            # casting NaNs to integer will fail
            pass

        # Detect values in the mapping that were intended to be mapped to nan.
        values_mapped_to_nan = [
            original_value for original_value, target_value in mapping.items() if pd.isnull(target_value)
        ]

        # Make a mask that is True for new nans that need to be replaced back to their original values.
        missing = series_mapped.isnull() & (~series.isin(values_mapped_to_nan))
        if missing.any():
            # Replace those nans by their original values.
            series_mapped.loc[missing] = series[missing]  # type: ignore[reportCallIssue]

    if warn_on_missing_mappings:
        unmapped = set(series) - set(mapping)
        if len(unmapped) > 0:
            warn_on_list_of_entities(
                unmapped,
                f"{len(unmapped)} missing values in mapping.",
                show_list=show_full_warning,
            )

    if warn_on_unused_mappings:
        unused = set(mapping) - set(series)
        if len(unused) > 0:
            warn_on_list_of_entities(
                unused,
                f"{len(unused)} unused values in mapping.",
                show_list=show_full_warning,
            )

    return series_mapped


def rename_categories(series: pd.Series, mapping: Dict[Any, Any]) -> pd.Series:
    """Alternative to pd.Series.cat.rename_categories which supports non-unique categories.

    We do that by replacing non-unique categories first and then mapping with unique categories.
    Unused categories are removed during the process. It should be as fast as
    pd.Series.cat.rename_categories if there are no non-unique categories.
    """
    if series.dtype != "category":
        raise ValueError("Series must be of type category.")

    series = series.copy()

    new_mapping: Dict[Any, Any] = {}
    for map_from, map_to in mapping.items():
        # Map nulls right away
        if pd.isnull(map_to):
            series[series == map_from] = np.nan

        # Non-unique category, replace it first
        elif map_to in new_mapping.values():
            # Find the category that maps to map_to
            series[series == map_from] = [k for k, v in new_mapping.items() if v == map_to][0]
        else:
            new_mapping[map_from] = map_to

    # NOTE: removing unused categories is necessary because of renaming
    return cast(
        pd.Series,
        series.cat.remove_unused_categories().cat.rename_categories(new_mapping).cat.remove_unused_categories(),
    )


def concatenate(objs: List[pd.DataFrame], **kwargs: Any) -> pd.DataFrame:
    """Concatenate while preserving categorical columns.

    Original source code from https://stackoverflow.com/a/57809778/1275818.
    """
    # Iterate on categorical columns common to all dfs
    for col in set.intersection(*[set(df.select_dtypes(include="category").columns) for df in objs]):
        ignore_order = any([not df[col].cat.ordered for df in objs])
        # Generate the union category across dfs for this column
        uc = union_categoricals([df[col] for df in objs], ignore_order=ignore_order)
        # Change to union category for all dataframes
        for df in objs:
            # df.loc[:, col] = pd.Categorical(df[col].values, categories=uc.categories)
            df[col] = df[col].astype(pd.CategoricalDtype(categories=uc.categories, ordered=uc.ordered))

    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        return pd.concat(objs, **kwargs)


def apply_on_categoricals(cat_series: List[pd.Series], func: Callable[..., str]) -> pd.Series:
    """Apply a function across multiple categorical Series efficiently.

    High-performance operation that applies a function to categorical Series
    without converting to strings first. Uses category codes internally to
    prevent memory explosion and significantly improve speed.

    Args:
        cat_series: List of Series with categorical dtype.
        func: Function that takes N arguments (one per Series) and returns a string.
            Called for each unique combination of category codes.

    Returns:
        New categorical Series with the function applied.

    Example:
        ```python
        import pandas as pd
        from owid.datautils.dataframes import apply_on_categoricals

        # Combine country and region categories
        countries = pd.Series(["USA", "UK", "USA"], dtype="category")
        regions = pd.Series(["Americas", "Europe", "Americas"], dtype="category")

        # Concatenate with separator
        result = apply_on_categoricals(
            [countries, regions],
            lambda c, r: f"{c} ({r})"
        )
        # Result: ["USA (Americas)", "UK (Europe)", "USA (Americas)"]
        # Still categorical dtype, much faster than string operations
        ```

    Note:
        This is significantly faster than converting categories to strings,
        especially for large DataFrames with repeated category values.
    """
    seen = {}
    codes = []
    categories = []
    for cat_codes in zip(*[s.cat.codes for s in cat_series]):
        if cat_codes not in seen:
            # add category
            # -1 is a special code for missing values
            cat_values = [s.cat.categories[code] if code != -1 else np.nan for s, code in zip(cat_series, cat_codes)]
            categories.append(func(*cat_values))
            seen[cat_codes] = len(categories) - 1

        # use existing category
        codes.append(seen[cat_codes])

    return cast(pd.Series, pd.Categorical.from_codes(codes, categories=categories))


def combine_two_overlapping_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    index_columns: Optional[List[str]] = None,
    keep_column_order: bool = False,
) -> pd.DataFrame:
    """Combine two DataFrames with overlapping columns, prioritizing the first.

    Intelligent merge that combines DataFrames with potentially identical columns,
    prioritizing values from df1 but filling its NaN values with data from df2.
    Avoids creating duplicate columns (e.g., "col_x", "col_y") that result from
    standard merges.

    Why not use standard operations:
        - `pd.merge()`: Creates duplicate columns with "_x" and "_y" suffixes
        - `pd.concat()` + `drop_duplicates()`: Would keep NaN values from df1
          instead of filling them with df2 values

    Args:
        df1: First DataFrame (higher priority for values).
        df2: Second DataFrame (used to fill NaN values in df1).
        index_columns: Column names to use as index for alignment (e.g., ["country", "year"]).
            Must exist in both DataFrames as regular columns. If None, uses existing
            DataFrame indices.
        keep_column_order: If True, preserve original column order (df1 columns first,
            then new df2 columns). If False, sort columns alphabetically.

    Returns:
        Combined DataFrame with union of rows and columns, prioritizing df1 values.

    Example:
        ```python
        import pandas as pd
        from owid.datautils.dataframes import combine_two_overlapping_dataframes

        df1 = pd.DataFrame({
            "country": ["USA", "UK"],
            "gdp": [20, None],
            "population": [330, 67]
        })

        df2 = pd.DataFrame({
            "country": ["USA", "UK", "France"],
            "gdp": [21, 3, 2.7],
            "area": [9.8, 0.24, 0.64]
        })

        result = combine_two_overlapping_dataframes(
            df1, df2,
            index_columns=["country"]
        )
        #   country   gdp  population  area
        # 0     USA  20.0         330  9.80  # GDP from df1
        # 1      UK   3.0          67  0.24  # GDP from df2 (was NaN in df1)
        # 2  France   2.7         NaN  0.64  # New row from df2
        ```
    """
    df1 = df1.copy()
    df2 = df2.copy()
    if index_columns is not None:
        # Ensure dataframes have a dummy index.
        if not ((df1.index.names == [None]) and (df2.index.names == [None])):
            warnings.warn("If index_columns is given, dataframes should have a dummy index. Use" " reset_index().")
        # Set index columns.
        df1 = df1.set_index(index_columns)
        df2 = df2.set_index(index_columns)
    else:
        # Ensure dataframes have the same indexes.
        if not (df1.index.names == df2.index.names):
            warnings.warn("Dataframes should have the same indexes.")

    # Align both dataframes on their common indexes.
    # Give priority to df1 on overlapping values.
    combined, df2 = df1.align(df2)

    new_columns = df2.columns.difference(df1.columns)
    for col in new_columns:
        try:
            combined[col] = combined[col].astype(df2[col].dtype, copy=False)
        except ValueError:
            # casting NaNs to integer will fail
            pass

    # Fill missing values in df1 with values from df2.
    combined = combined.fillna(df2)

    if index_columns is not None:
        combined = combined.reset_index()

    if keep_column_order:
        # The previous operations will automatically sort columns alphanumerically.
        # To keep the original order of columns, we need to find that sequence of columns.
        # First, columns of df1, and then all columns in df2 that were not in df1.
        if index_columns is not None:
            columns = index_columns + df1.columns.tolist()
        else:
            columns = df1.columns.tolist()
        columns = columns + [column for column in df2.columns if column not in columns]
        combined = combined[columns]

    # It would be good to have a 'keep_row_order' option, but it's a bit tricky.

    return cast(pd.DataFrame, combined)
