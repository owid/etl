import random
from typing import Any, Callable, Generator, Iterable, List, Optional, cast

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from pandas.api.types import is_numeric_dtype  # type: ignore
from pandas.api.types import union_categoricals

# ######## Note - this file will be moved to owid-catalog-py before the branch is merged ##############


def get_list_description_with_max_length(items: List[Any], max_items: int = 20) -> str:
    if len(items) > max_items:
        return (
            f"[{len(items)} items] "
            + f'{", ".join(str(item) for item in items[:int(max_items/2)])} ... {", ".join(str(item) for item in items[-int(max_items/2):])}'
        )
    else:
        return ", ".join(str(item) for item in items)


def yield_list_lines(description: str, items: Iterable[Any]) -> Generator[str, None, None]:
    sublines = [item for item in items]
    if len(sublines) > 1:
        yield f"{description}:"
        for subline in sublines:
            if subline != "":
                yield f"  {subline}"
    elif len(sublines) == 1:
        yield f"{description}: {sublines[0]}"


def get_compact_list_description(
    items_iterable: Iterable[Any], tuple_headers: List[str] = [], max_items: int = 20
) -> Generator[str, None, None]:
    """Returns a compact desription of a list.

    If the list is numeric and monotonic then it gets compacted into a range like 2000-2015. If
    the list contains tuples then the tuples are deconstructed into their components and the
    components are compacted individually.
    """
    items = set(items_iterable)
    if not items:
        yield "[]"
    elif all(isinstance(item, int) for item in items):
        sorted_items = sorted(items)
        if len(items) == 1:
            yield str(sorted_items[0])
        if len(items) == 2:
            yield f"{sorted_items[0]}, {sorted_items[1]}"
        if len(items) > 2:
            if len(items) == sorted_items[-1] - sorted_items[0]:
                yield f"{sorted_items[0]}-{sorted_items[-1]}"
            else:
                yield get_list_description_with_max_length(sorted_items, max_items)
    elif all(isinstance(item, tuple) for item in items):
        transposed = zip(*items)
        lines = [line for item in transposed for line in get_compact_list_description(item)]
        if len(tuple_headers) == len(lines):
            yield from (f"{header}: {line}" for header, line in zip(tuple_headers, lines))
        else:
            yield from lines
    else:
        sorted_items = sorted(items)
        yield get_list_description_with_max_length(sorted_items, max_items)


def yield_formatted_if_not_empty(
    item: Any,
    format_function: Callable[[Any], Generator[str, None, None]],
    fallback_message: str = "",
) -> Generator[str, None, None]:
    """Yield an item formatted with the given function if it is not empty."""
    if item is not None and any(item):
        yield from format_function(item)
    elif fallback_message != "":
        yield fallback_message


def sample_from_dataframe(df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Sample from dataframe with sorted output index and maximum `n`."""
    if "n" in kwargs:
        kwargs["n"] = min(kwargs["n"], len(df))
    return cast(pd.DataFrame, df.sample(**kwargs).sort_index())


def series_equals(
    s1: pd.Series,
    s2: pd.Series,
    absolute_tolerance: float = 1e-08,
    relative_tolerance: float = 1e-05,
) -> pd.Series:
    """Compare two series and return a boolean series with True
    if values are the same or within tolerance."""
    assert all(s1.index == s2.index), "Indices must be the same"

    # Union categories of categorical columns to enable comparison
    if s1.dtype == "category":
        uc = union_categoricals([s1, s2])
        s1 = pd.Series(pd.Categorical(s1.values, categories=uc.categories), index=s1.index)
        s2 = pd.Series(pd.Categorical(s2.values, categories=uc.categories), index=s2.index)

    # Eq above does not take tolerance into account so compare again with tolerance
    # for columns that are numeric. this could probably be sped up with a check on any on
    # the column first but would have to be benchmarked
    if is_numeric_dtype(s1) and is_numeric_dtype(s2):
        # For numeric data, consider them equal within certain absolute and relative tolerances.
        return pd.Series(
            np.isclose(
                s1.astype(float),
                s2.astype(float),
                atol=absolute_tolerance,
                rtol=relative_tolerance,
                equal_nan=True,
            ),
            index=s1.index,
        )
    elif (s1.dtype in (object, "category", "string")) or (s2.dtype in (object, "category", "string")):
        # Apply a direct comparison for strings or categories
        pass
    elif is_datetime64_any_dtype(s1):
        # Apply a direct comparison for datetimes
        pass
    else:
        raise ValueError(f"Unsupported dtype {s1.dtype} for column {s1.name}")

    return s1.eq(s2) | (s1.isnull() & s2.isnull())


def df_equals(df1: pd.DataFrame, df2: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Compare two dataframes and return a boolean dataframe with True
    if values are the same or within tolerance."""
    assert all(df1.columns == df2.columns), "Columns must be the same"
    assert all(df1.index == df2.index), "Indices must be the same"

    equals = pd.DataFrame(False, index=df1.index, columns=df1.columns)
    for col in df1.columns:
        equals[col] = series_equals(df1[col], df2[col], **kwargs)
    return equals


class HighLevelDiff:
    """Class for comparing two dataframes.

    It assumes that all nans are identical, and compares floats by means of certain absolute and relative tolerances.
    Construct this class by passing two dataframes of possibly different shape. Then check the are_structurally_equal
    property to see if the column and row sets of the two dataframes match and/or check the are_equal flag to also
    check for equality of values. The other fields give detailed information on what is different between the two
    dataframes.

    For cases where there is a difference, various member fields on this class give indications of what is different
    (e.g. columns missing in dataframe 1 or 2, index values missing in dataframe 1 or 2, etc.).

    The get_description_lines method fetches a list of strings that compactly describe the differences for humans.

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

    """

    df1: pd.DataFrame
    df2: pd.DataFrame
    columns_missing_in_df1: List[str]
    columns_missing_in_df2: List[str]
    columns_shared: List[str]
    index_columns_missing_in_df1: List[str]
    index_columns_missing_in_df2: List[str]
    index_columns_shared: List[str]
    index_values_missing_in_df1: pd.Index
    index_values_missing_in_df2: pd.Index
    index_values_shared: pd.Index
    duplicate_index_values_in_df1: pd.Series
    duplicate_index_values_in_df2: pd.Series
    value_differences: Optional[pd.DataFrame] = None

    def __init__(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        absolute_tolerance: float = 1e-08,
        relative_tolerance: float = 1e-05,
    ):
        self.df1 = df1
        self.df2 = df2
        self.absolute_tolerance = absolute_tolerance
        self.relative_tolerance = relative_tolerance
        self._diff()

    @property
    def value_differences_count(self) -> int:
        """Get number of cells in the structural overlap of the two dataframes that differ by more than tolerance."""
        if self.value_differences is None:
            return 0
        else:
            return int(self.value_differences.sum().sum())

    @property
    def columns_with_differences(self) -> Any:
        """Get the columns that are different in the two dataframes.

        This will be an array of index values. If the index is a MultiIndex, the index values will be tuples.
        """
        if self.value_differences is None:
            return np.array([])
        return self.value_differences.columns.values

    @property
    def rows_with_differences(self) -> Any:
        """Return the row indices that are different in the two dataframes.

        This will be an array of index values. If the index is a MultiIndex, the index values will be tuples.
        """
        if self.value_differences is None:
            return np.array([])
        return self.value_differences.index.values

    def _diff(self) -> None:
        """Diff the two dataframes.

        This can be a somewhat slow operation
        """
        df1_columns_set = set(self.df1.columns)
        df2_columns_set = set(self.df2.columns)
        self.columns_missing_in_df1 = sorted(df2_columns_set - df1_columns_set)
        self.columns_missing_in_df2 = sorted(df1_columns_set - df2_columns_set)
        self.columns_shared = sorted(df1_columns_set.intersection(df2_columns_set))

        df1_index_names = set(self.df1.index.names)
        df2_index_names = set(self.df2.index.names)
        self.index_columns_missing_in_df1 = sorted(df2_index_names - df1_index_names)
        self.index_columns_missing_in_df2 = sorted(df1_index_names - df2_index_names)
        self.index_columns_shared = sorted(df1_index_names.intersection(df2_index_names))

        self.index_values_missing_in_df1 = self.df2.index.difference(self.df1.index)
        self.index_values_missing_in_df2 = self.df1.index.difference(self.df2.index)
        self.index_values_shared = self.df2.index.intersection(self.df1.index)
        self.duplicate_index_values_in_df1 = self.df1[self.df1.index.duplicated()].index.values
        self.duplicate_index_values_in_df2 = self.df2[self.df2.index.duplicated()].index.values

        # Now we calculate the value differences in the intersection of the two dataframes.
        if self.columns_shared and not self.index_values_shared.empty:
            df1_intersected = self.df1.loc[self.index_values_shared, list(self.columns_shared)]
            df2_intersected = self.df2.loc[self.index_values_shared, list(self.columns_shared)]

            diffs = df_equals(
                df1_intersected,
                df2_intersected,
                absolute_tolerance=self.absolute_tolerance,
                relative_tolerance=self.relative_tolerance,
            )

            # We now have a dataframe with the same shape and indices as df1 and df2, filled with
            # True where the values are the same. We want to use true for different values, so invert
            # element-wise now
            diffs = ~diffs

            if diffs.empty:
                self.value_differences = None
            else:
                # Get a copy of diffs with all rows dropped where all values in a row are False
                # (i.e. where df1 and df2 have identical values for all columns)
                rows_with_diffs = diffs[diffs.any(axis=1)]
                if rows_with_diffs.empty or not rows_with_diffs.any().any():
                    self.value_differences = None
                else:
                    # Now figure out all columns where there is at least one difference
                    columns_with_diffs = diffs.any(axis=0)
                    if not columns_with_diffs.any():
                        self.value_differences = None
                    else:
                        # Here we drop the columns that did not have differences. We are left with a dataframe
                        # with the original indices and only the rows and columns with differences.
                        self.value_differences = rows_with_diffs.loc[:, columns_with_diffs]

    @property
    def are_structurally_equal(self) -> bool:
        """Check if the two dataframes are structurally equal (i.e. same columns, same index values, ...)."""
        return not (
            any(self.columns_missing_in_df1)
            or any(self.columns_missing_in_df2)
            or any(self.index_columns_missing_in_df1)
            or any(self.index_columns_missing_in_df2)
            or any(self.index_values_missing_in_df1)
            or any(self.index_values_missing_in_df2)
            or any(self.duplicate_index_values_in_df1)
            or any(self.duplicate_index_values_in_df2)
        )

    @property
    def are_equal(self) -> bool:
        """Check if the two dataframes are equal, both structurally and cell-wise."""
        return self.are_structurally_equal and self.are_overlapping_values_equal

    @property
    def are_overlapping_values_equal(self) -> bool:
        """Check if the values within the overlapping columns and rows of the two dataframes are equal."""
        return self.value_differences is None

    @property
    def df1_value_differences(self) -> Optional[pd.DataFrame]:
        """Get a sliced version of df1 that contains only the columns and rows that differ from df2.

        Note that this only includes the part of the dataframe that has structural overlap with
        the other dataframe (i.e. extra columns or rows are not included).
        """
        if self.value_differences is None:
            return None
        return cast(
            pd.DataFrame,
            self.df1.loc[self.value_differences.index, self.value_differences.columns],
        )

    @property
    def df2_value_differences(self) -> Optional[pd.DataFrame]:
        """Get a sliced version of df2 that contains only the columns and rows that differ from df2.

        Note that this only includes the part of the dataframe that has structural overlap with
        the other dataframe (i.e. extra columns or rows are not included).
        """
        if self.value_differences is None:
            return None
        return cast(
            pd.DataFrame,
            self.df2.loc[self.value_differences.index, self.value_differences.columns],
        )

    def get_description_lines_for_diff(
        self,
        df1_label: str,
        df2_label: str,
        use_color_tags: bool = False,
        preview_different_dataframe_values: bool = False,
        show_shared: bool = False,
        truncate_lists_longer_than: int = 20,
        preview_samples: int = 20,
    ) -> Generator[str, None, None]:
        """Generate a human readable description of the differences between the two dataframes.

        It is returned as a generator of strings, roughly one line per string yielded
        (dataframe printing is done by pandas as one string and is returned as a single yielded item)
        """
        red, red_end = ("[red]", "[/red]") if use_color_tags else ("", "")
        green, green_end = ("[green]", "[/green]") if use_color_tags else ("", "")
        blue, blue_end = ("[blue]", "[/blue]") if use_color_tags else ("", "")

        if self.are_equal:
            yield (f"{green}{df1_label} is equal to {df2_label}{green_end}")
        else:
            yield (f"{red}{df1_label} is not equal to {df2_label}{red_end}")

            if self.are_structurally_equal:
                yield (f"The structure is {green}identical{green_end}")
            else:
                yield (f"The structure is {red}different{red_end}")

                # The structure below works like this: we have a property that is a list
                # (e.g. self.columns_missing_in_df1) that can be empty or have elements.
                # If the list is empty we don't want to yield any lines. If the list has elements
                # we want to yield a line. Additionally, we also want to truncate lines with many
                # elements if they are too long. We use yield_formatted_if_not_empty on most of the
                # member properties to output the differences if there are any.

                # Structural differences
                if show_shared:
                    yield from yield_formatted_if_not_empty(
                        self.columns_shared,
                        lambda item: yield_list_lines(
                            f"{blue}Shared columns{blue_end}",
                            get_compact_list_description(item, max_items=truncate_lists_longer_than),
                        ),
                        f"{red}No shared columns{red_end}",
                    )
                yield from yield_formatted_if_not_empty(
                    self.columns_missing_in_df1,
                    lambda item: yield_list_lines(
                        f"Columns missing in {df1_label}",
                        get_compact_list_description(item, max_items=truncate_lists_longer_than),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.columns_missing_in_df2,
                    lambda item: yield_list_lines(
                        f"Columns missing in {df2_label}",
                        get_compact_list_description(item, max_items=truncate_lists_longer_than),
                    ),
                )
                if show_shared:
                    yield from yield_formatted_if_not_empty(
                        self.index_columns_shared,
                        lambda item: yield_list_lines(
                            f"{blue}Shared index columns{blue_end}",
                            get_compact_list_description(item, max_items=truncate_lists_longer_than),
                        ),
                        f"{red}No shared index columns{red_end}",
                    )
                yield from yield_formatted_if_not_empty(
                    self.index_columns_missing_in_df1,
                    lambda item: yield_list_lines(
                        f"Index columns missing in {df1_label}",
                        get_compact_list_description(item, max_items=truncate_lists_longer_than),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.index_columns_missing_in_df2,
                    lambda item: yield_list_lines(
                        f"Index columns missing in {df2_label}",
                        get_compact_list_description(item, max_items=truncate_lists_longer_than),
                    ),
                )
                if show_shared:
                    yield from yield_formatted_if_not_empty(
                        self.index_values_shared,
                        lambda item: yield_list_lines(
                            f"{blue}Shared index values{blue_end}",
                            get_compact_list_description(item, max_items=truncate_lists_longer_than),
                        ),
                        f"{red}No shared index values{red_end}",
                    )
                yield from yield_formatted_if_not_empty(
                    self.index_values_missing_in_df1,
                    lambda item: yield_list_lines(
                        f"Index values missing in {df1_label}",
                        get_compact_list_description(
                            item,
                            self.df1.index.names,
                            max_items=truncate_lists_longer_than,
                        ),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.index_values_missing_in_df2,
                    lambda item: yield_list_lines(
                        f"Index values missing in {df2_label}",
                        get_compact_list_description(
                            item,
                            self.df2.index.names,
                            max_items=truncate_lists_longer_than,
                        ),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.duplicate_index_values_in_df1,
                    lambda item: yield_list_lines(
                        f"Duplicate index values in {df1_label}",
                        get_compact_list_description(
                            item,
                            self.df1.index.names,
                            max_items=truncate_lists_longer_than,
                        ),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.duplicate_index_values_in_df2,
                    lambda item: yield_list_lines(
                        f"Duplicate index values in {df2_label}",
                        get_compact_list_description(
                            item,
                            self.df2.index.names,
                            max_items=truncate_lists_longer_than,
                        ),
                    ),
                )

            # Show "coordinates" where there are value differences
            # This is done in compact form, e.g. if you have 10 new years for 200 countries
            # that would be 2000 values but instead we unpack the hierarchical index tuples
            # and show that a (shortened) list for the 200 countries and the 10 new years.
            if self.value_differences is not None:
                yield (
                    f"Values in the shared columns/rows are {red}different{red_end}. "
                    + f"({self.value_differences_count} different cells)"
                )
                yield from yield_formatted_if_not_empty(
                    self.columns_with_differences,
                    lambda item: yield_list_lines(
                        "Columns with diffs",
                        get_compact_list_description(item, max_items=truncate_lists_longer_than),
                    ),
                )
                yield from yield_formatted_if_not_empty(
                    self.rows_with_differences,
                    lambda item: yield_list_lines(
                        "Rows with diffs",
                        get_compact_list_description(
                            item,
                            self.df1.index.names,
                            max_items=truncate_lists_longer_than,
                        ),
                    ),
                )

        # This prints the two dataframes one after the other sliced to
        # only the area where they have differences
        # IDEA: we could show columns side by side for easier comparison
        if preview_different_dataframe_values:
            if self.columns_shared and self.index_values_shared is not None:
                if self.value_differences is not None:
                    if preview_samples < len(self.value_differences.index):
                        extra_msg = f" (showing {preview_samples} samples)"
                    else:
                        extra_msg = ""

                    random_state = random.randint(0, 100000)

                    yield f"Values with differences in {df1_label}{extra_msg}:"
                    yield (
                        str(
                            sample_from_dataframe(
                                self.df1.loc[self.value_differences.index, self.value_differences.columns],
                                n=preview_samples,
                                random_state=random_state,
                            )
                        )
                    )
                    yield f"Values with differences in {df2_label}{extra_msg}:"
                    yield (
                        str(
                            sample_from_dataframe(
                                self.df2.loc[self.value_differences.index, self.value_differences.columns],
                                n=preview_samples,
                                random_state=random_state,
                            )
                        )
                    )
                else:
                    yield "[orange1]Dataframes are structurally different, but are equal within overlapping columns/rows.[/orange1]"
            else:
                yield "The datasets have no overlapping columns/rows."
