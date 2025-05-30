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
    """Return True if a dataframe has an index, and False if it does not (i.e. if it has a dummy index).

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe whose index will be checked.

    Returns
    -------
    df_has_index : bool
        True if dataframe has a non-dummy (single- or multi-) index.

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
    """Compare two dataframes element by element to see if they are equal.

    It assumes that nans are all identical, and allows for certain absolute and relative tolerances for the comparison
    of floats.

    NOTE: Dataframes must have the same number of rows to be able to compare them.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe.
    df2 : pd.DataFrame
        Second dataframe.
    columns : list or None
        List of columns to compare (they both must exist in both dataframes). If None, common columns will be compared.
    absolute_tolerance : float
        Absolute tolerance to assume in the comparison of each cell in the dataframes. A value a of an element in df1 is
        considered equal to the corresponding element b at the same position in df2, if:
        abs(a - b) <= absolute_tolerance
    relative_tolerance : float
        Relative tolerance to assume in the comparison of each cell in the dataframes. A value a of an element in df1 is
        considered equal to the corresponding element b at the same position in df2, if:
        abs(a - b) / abs(b) <= relative_tolerance

    Returns
    -------
    compared : pd.DataFrame
        Dataframe of booleans, with as many rows as df1 and df2, and as many columns as specified by `columns` argument
        (or as many common columns between df1 and df2, if `columns` is None). The (i, j) element is True if df1 and f2
        have the same value (for the given tolerances) at that same position.

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
    """Check whether two dataframes are equal.

    It assumes that all nans are identical, and compares floats by means of certain absolute and relative tolerances.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe.
    df2 : pd.DataFrame
        Second dataframe.
    absolute_tolerance : float
        Absolute tolerance to assume in the comparison of each cell in the dataframes. A value a of an element in df1 is
        considered equal to the corresponding element b at the same position in df2, if:
        abs(a - b) <= absolute_tolerance
    relative_tolerance : float
        Relative tolerance to assume in the comparison of each cell in the dataframes. A value a of an element in df1 is
        considered equal to the corresponding element b at the same position in df2, if:
        abs(a - b) / abs(b) <= relative_tolerance
    verbose : bool
        True to print a summary of the comparison of the two dataframes.

    Returns
    -------
    are_equal : bool
        True if the two dataframes are equal (given the conditions explained above).
    compared : pd.DataFrame
        Dataframe with the same shape as df1 and df2 (if they have the same shape) that is True on each element where
        both dataframes have equal values. If dataframes have different shapes, compared will be empty.

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


def groupby_agg(
    df: pd.DataFrame,
    groupby_columns: Union[List[str], str],
    aggregations: Optional[Dict[str, Any]] = None,
    num_allowed_nans: Optional[int] = None,
    frac_allowed_nans: Optional[float] = None,
    min_num_values: Optional[int] = None,
) -> pd.DataFrame:
    """Group dataframe by certain columns, and aggregate using a certain method, and decide how to handle nans.

    This function is similar to the usual
    > df.groupby(groupby_columns).agg(aggregations)
    However, pandas by default ignores nans in aggregations. This implies, for example, that
    > df.groupby(groupby_columns).sum()
    will treat nans as zeros, which can be misleading.

    When both num_allowed_nans, frac_allowed_nans, and min_num_values are None, this function behaves like the default
    pandas groupby().agg() (and nans may be treated as zeros).

    Otherwise, if any of those parameters is not None, then the following conditions are applied (one after the other):

    1. If num_allowed_nans is not None, then a group will be nan if the number of nans in that group is larger than
      num_allowed_nans.
      For example, if num_allowed_nans is set to 1, and there are 2 or more nans in a group, the aggregate will be nan.

    2. If frac_allowed_nans is not None, then a group will be nan if the fraction of nans in that group is larger than
      frac_allowed_nans.
      For example, if frac_allowed_nans is set to 0.2, and the fraction of nans in a group 0.201, the aggregate will be
      nan.

    3. If min_num_values is not None, then a group will be nan if the number of non-nan values is smaller than
      min_num_values. Note that, for this condition to be relevant, min_num_values must be >= 1.
      For example, if min_num_values is set to 1, and all values in a group are nan, the aggregate will be nan (instead
      of a spurious zero, as it commonly happens).

    NOTE: This function won't work when using multiple aggregations for the same column (e.g. {'a': ('sum', 'mean')}).

    Parameters
    ----------
    df : pd.DataFrame
        Original dataframe.
    groupby_columns : list or str
        List of columns to group by. It can be given as a string, if it is only one column.
    aggregations : dict or None
        Aggregations to apply to each column in df. If None, 'sum' will be applied to all columns.
    num_allowed_nans : int or None
        Maximum number of nans that are allowed in a group.
    frac_allowed_nans : float or None
        Maximum fraction of nans that are allowed in a group.
    min_num_values : int or None
        Minimum number of non-nan values that a group must have. If fewer values are found, the aggregate will be nan.

    Returns
    -------
    grouped : pd.DataFrame
        Grouped dataframe after applying aggregations.

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

    # Group by and aggregate in the usual way.
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

    Faster version of:

    >>> num_nans_detected = df.groupby(groupby_columns, **groupby_kwargs).agg(
        lambda x: pd.isnull(x).sum()
    )

    """
    nan_columns = [c for c in df.columns if c not in groupby_columns]

    num_nans_detected = df[nan_columns].isnull().groupby([df[c] for c in groupby_columns], **kwargs).sum()

    return cast(pd.DataFrame, num_nans_detected)


def multi_merge(dfs: List[pd.DataFrame], on: Union[List[str], str], how: str = "inner") -> pd.DataFrame:
    """Merge multiple dataframes.

    This is a helper function when merging more than two dataframes on common columns.

    Parameters
    ----------
    dfs : list
        Dataframes to be merged.
    on : list or str
        Column or list of columns on which to merge. These columns must have the same name on all dataframes.
    how : str
        Method to use for merging (with the same options available in pd.merge).

    Returns
    -------
    merged : pd.DataFrame
        Input dataframes merged.

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
    """Map values of a series given a certain mapping.

    This function does almost the same as
    > series.map(mapping)
    However, map() translates values into nan if those values are not in the mapping, whereas this function allows to
    optionally keep the original values.

    This function should do the same as
    > series.replace(mapping)
    However .replace() becomes very slow on big dataframes.

    Parameters
    ----------
    series : pd.Series
        Original series to be mapped.
    mapping : dict
        Mapping.
    make_unmapped_values_nan : bool
        If true, values in the series that are not in the mapping will be translated into nan; otherwise, they will keep
        their original values.
    warn_on_missing_mappings : bool
        True to warn if elements in series are missing in mapping.
    warn_on_unused_mappings : bool
        True to warn if the mapping contains values that are not present in the series. False to ignore.
    show_full_warning : bool
        True to print the entire list of unused mappings (only relevant if warn_on_unused_mappings is True).

    Returns
    -------
    series_mapped : pd.Series
        Mapped series.

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
    """Apply a function on a list of categorical series.

    This is much faster than converting them to strings first and then applying the function and it prevents memory
    explosion. It uses category codes instead of using values directly and it builds the output categorical mapping
    from codes to strings on the fly.

    Parameters
    ----------
    cat_series :
        List of series with category type.
    func :
        Function taking as many arguments as there are categorical series and returning str.
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
    """Combine two dataframes that may have identical columns, prioritizing the first one.

    If dataframes have a dummy index, index_columns have to be specified (and must be a column in both dataframes).
    If dataframes have a single/multi index, index_columns must be left as None.

    Suppose you have two dataframes, df1 and df2, both having columns "col_a" and "col_b", and we want to create a
    combined dataframe with the union of rows and columns, and, on the overlapping elements, prioritize df1 values.
    To do this, you could:
    * Merge the dataframes. But then the result would have columns "col_a_x", "col_a_y", "col_b_x", and "col_b_y".
    * Concatenate them and then drop duplicates (for example keeping the last repetition). This works, but, if df1 has
    nans then we would keep those nans.
    To solve these problems, this function will not create new columns, and will prioritize df1, but filling missing
    values in df1 with data from df2.

    Parameters
    ----------
    df1 : pd.DataFrame
        First dataframe (the one that has priority).
    df2 : pd.DataFrame
        Second dataframe.
    index_columns : list or None
        Columns (that must be present in both dataframes, and not as index columns) that should be treated as index
        (e.g. ["country", "year"]). If None, the single/multi index of the dataframes will be used.
    keep_column_order : bool
        True to keep the column order of the original dataframes (first all columns in df1, then all columns from df2
        that were not already in df1). False to sort columns alphanumerically.

    Returns
    -------
    combined : pd.DataFrame
        Combination of the two dataframes.

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
