"""General data tools."""

import math
import os
import random
import time
from datetime import date, datetime
from functools import wraps
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, TypeVar, Union, cast

import owid.catalog.processing as pr
import pandas as pd
import plotly.express as px
from googleapiclient.errors import HttpError
from owid.catalog import Table
from owid.datautils import dataframes
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import OWID_ENV
from etl.google import CLIENT_SECRET_FILE, GoogleDrive, GoogleSheet

log = get_logger()
TableOrDataFrame = TypeVar("TableOrDataFrame", pd.DataFrame, Table)
DIMENSION_COL_NONE = "temporary"
GSHEET_EXPORT_CONFIG = {
    "MAX_TITLE_LENGTH": 100,
    "MAX_SHEET_NAME_LENGTH": 31,
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1.0,
    "INCLUDED_METADATA_FIELDS": frozenset(
        {"title", "description_short", "description_key", "description_processing", "origins", "unit"}
    ),
    "FIELD_DISPLAY_NAMES": {
        "title": "Title",
        "description_short": "Description",
        "description_key": "What you should know about this data",
        "description_processing": "Processing steps",
        "unit": "Unit",
        "origins": "Data sources",
    },
}
DEFAULT_TEAM_FOLDER_NAME = "ETL GSheet Exports"
OWID_SHARED_FOLDER_ID = os.getenv("OWID_SHARED_FOLDER_ID")


def check_known_columns(df: pd.DataFrame, known_cols: list) -> None:
    """Check that all columns in a dataframe are known and none is missing."""
    unknown_cols = set(df.columns).difference(set(known_cols))
    if len(unknown_cols) > 0:
        raise Exception(f"Unknown column(s) found: {unknown_cols}")

    missing_cols = set(known_cols).difference(set(df.columns))
    if len(missing_cols) > 0:
        raise Exception(f"Previous column(s) missing: {missing_cols}")


def check_values_in_column(df: pd.DataFrame, column_name: str, values_expected: Union[Set[Any], List[Any]]):
    """Check values in a column are as expected.

    It checks both ways:
        - That there are no new and unexpected values (compared to `values_expected`).
        - That all expected values are present in the column (all in `values_expected`).
    """
    if not isinstance(values_expected, set):
        values_expected = set(values_expected)
    ds = df[column_name]
    values_obtained = set(ds)
    if values_unknown := values_obtained.difference(values_expected):
        raise ValueError(f"Values {values_unknown} in column `{column_name}` are new, unsure how to map. Review!")
    if values_missing := values_expected.difference(values_obtained):
        raise ValueError(
            f"Values {values_missing} in column `{column_name}` missing, check if they were removed from source!"
        )


def interpolate_table(
    df: TableOrDataFrame,
    entity_col: str | List[str] | Iterable[str],
    time_col: str,
    time_mode: Literal["full_range", "full_range_entity", "observed", "none"] = "full_range",
    method: str = "linear",
    limit_direction: str = "both",
    limit_area: Optional[str] = None,
) -> TableOrDataFrame:
    """Interpolate missing values in a column linearly.

    df: Table or DataFrame
        Should contain three columns: country, year, and the column to be interpolated.
    entity_col: str
        Name of the column with entity names (typically for countries).
    time_col: str
        Name of the column with years.
    mode: str
        How to complete time series. 'full_range' for complete range, 'full_range_entity' for complete range within an entity, 'reduced' for only time values appearing in the data. Use 'none' to interpolate with existing values.
    """
    SINGLE_ENTITY = isinstance(entity_col, str)
    MULTIPLE_ENTITY = isinstance(entity_col, list)
    assert SINGLE_ENTITY | MULTIPLE_ENTITY, "`entity_col` must be either a string or a list of strings!"
    if SINGLE_ENTITY:
        index = [entity_col, time_col]
    else:
        index = list(entity_col) + [time_col]

    if time_mode != "none":
        # Expand time
        df = expand_time_column(
            df,
            dimension_col=entity_col,
            time_col=time_col,
            method=time_mode,
        )

    # Set index
    df = cast(TableOrDataFrame, df.set_index(index).sort_index())

    # Interpolate
    df = (
        df.groupby(entity_col)
        .transform(lambda x: x.interpolate(method=method, limit_direction=limit_direction, limit_area=limit_area))  # type: ignore
        .reset_index()
    )

    return df


def expand_time_column(
    df: TableOrDataFrame,
    time_col: str,
    dimension_col: Optional[str | Iterable[str]] = None,
    method: Literal["full_range", "full_range_entity", "observed", "none"] = "full_range",
    until_time: Optional[int | datetime] = None,
    since_time: Optional[int | datetime] = None,
    fillna_method: Optional[List[str] | str] = None,
) -> TableOrDataFrame:
    """Add rows to complete the timeseries.

    Parameters
    ----------
    df: Table or pd.DataFrame
        Table or dataframe for which you want to add new time rows. It should have an entity column (or columns) and time column.
    dimension_col: str or List[str]
        Name of the dimension columns. Dimension columns typically include the entity (e.g. country name) and other optional dimensions (e.g. sex, age group, etc.)
    time_col: str
        Name of the time column. Tables should either have a column with the year, or with dates.
    method: "full_range", "full_range_entity" or "observed"
        You can complete the timeseries in various ways, by changing the value of `method`:
            - 'full_range_entity': Add rows for all possible times within the minimum and maximum times in the data for a given entity. That is, the ranges covered for each entity is different.
            - 'full_range': Add rows for all possible entity-time pairs within the minimum and maximum times in the (complete) data. That is, the time range covered by each entity is the same.
            - 'observed': Add rows for all times that appear in the data. Note that some times might be present for an entity, but not for another.
            - 'none': No row is added. Might be useful when you are only interested in using `until_time` and `since_time`.
    until_time: int
        Only year is supported. After expanding the time-series using `method`, extend it until the given time.
    since_time: int
        Only year is supported. After expanding the time-series using `method`, extend it since the given time.
    fillna_method: List[str] or str
        If the table is expanded, new rows with NaN values will appear. You can fill these NaNs with a strategy:
            - None: If none is given, NaNs are left as-is.
            - 'interpolate': Linearly interpolate values.
            - 'ffill': Forward-filling.
            - 'bfill': Backward-filling.
            - 'zero': Replace NaNs with zeroes.
        You can provide a list of strategies, e.g. ['bfill', 'ffill']. This will first apply backward-filling and then forward-filling.

    Notes
    -----

    - This method has been extensively tested in datasets using `year`. If you are using dates, please use with caution and please report any issue.
    - Behaviour of interpolation and filling methods for non-numeric values is unknown. Please pay attention to the output if that's your case.

    Future work:
    ------------
    - Add arguments `since_time_condition_value` and `until_time_condition_value`: only extend those entity-dimensions that start (or end) in a particular year or date. E.g. Extend table until 2023, but only for those countries that made it up to 2015 (this avoid extending historical countries that ended up way before). This could be actually `since_time_condition_values`, i.e. list of conditions!
    """
    # Sanity check
    assert isinstance(time_col, str), "`time_col` must be a string!"

    # TODO: This is temporary hack
    if dimension_col is None:
        dimension_col = DIMENSION_COL_NONE
        df[DIMENSION_COL_NONE] = ""
        df[DIMENSION_COL_NONE] = df[DIMENSION_COL_NONE].astype("string")

    # Determine if we have a single or multiple dimensiosn (will affect how groupbys are done)
    SINGLE_DIMENSION = isinstance(dimension_col, str)
    MULTIPLE_DIMENSION = isinstance(dimension_col, list)
    assert SINGLE_DIMENSION | MULTIPLE_DIMENSION, "`dimension_col` must be either a string or a list of strings!"

    # Sanity check: value for `method` is as expected
    assert method in {"full_range_entity", "full_range", "observed", "none"}, f"Wrong value for `method` {method}!"

    # Save initial states
    ## dataframe column order
    columns_order = list(df.columns)
    ## dtypes
    dtypes = df.dtypes

    # Temporary function to get the upper and lower bounds of the time period
    def _get_complete_date_range(ds):
        date_min = ds.min()
        date_max = ds.max()
        if isinstance(date_max, datetime):
            return pd.date_range(start=date_min, end=date_max)
        if isinstance(date_max, date):
            return pd.date_range(start=date_min, end=date_max).date
        else:
            return range(int(date_min), int(date_max) + 1)

    def _get_iter_and_names(
        df: pd.DataFrame, single_dimension: bool, dimension_col: Iterable[str] | str, date_values: Iterable[Any]
    ):
        if single_dimension:
            # For some countries we have population data only on certain years, e.g. 1900, 1910, etc.
            # Optionally fill missing years linearly.
            entities_in_data = df[dimension_col].unique()
            iterables = [entities_in_data, date_values]
            names = [dimension_col, time_col]
        else:
            iterables = [df[col].unique() for col in dimension_col] + [date_values]
            names = [col for col in dimension_col] + [time_col]

        return iterables, names

    # Define index column (or columns). Useful for groupby and alike operations
    if SINGLE_DIMENSION:
        index = [dimension_col, time_col]
    else:
        index = list(dimension_col) + [time_col]

    # Cover complete time range for each country
    if method == "full_range_entity":

        def _reindex_dates(group):
            complete_date_range = _get_complete_date_range(group[time_col])
            group = (
                group.set_index(time_col).reindex(complete_date_range).reset_index().rename(columns={"index": time_col})
            )
            group[dimension_col] = group[dimension_col].ffill().bfill()  # Fill NaNs in 'country'
            return group

        df = df.groupby(dimension_col).apply(_reindex_dates).reset_index(drop=True).set_index(index)  # type: ignore
        df = cast(TableOrDataFrame, df.reset_index())
    # Either full range or all observations.
    elif method in {"full_range", "observed"}:
        # Get list of times
        if method == "full_range":
            date_values = _get_complete_date_range(df[time_col])
        else:
            date_values = df[time_col].unique()

        iterables, names = _get_iter_and_names(
            df,
            SINGLE_DIMENSION,
            dimension_col,
            date_values,
        )

        # Reindex
        df = (
            df.set_index(index)
            .reindex(pd.MultiIndex.from_product(iterables, names=names))  # type: ignore
            .sort_index()
        )

        df = cast(TableOrDataFrame, df.reset_index())

    #####################################################################
    # Further extend (back, forth, back and forth)
    #####################################################################
    EXTEND_END = (since_time is None) and (until_time is not None)
    EXTEND_START = (since_time is not None) and (until_time is None)
    EXTEND_BOTH = (since_time is not None) and (until_time is not None)
    if EXTEND_END or EXTEND_START or EXTEND_BOTH:
        # Get time bounds
        df_bounds = df.reset_index().groupby(dimension_col)[time_col].agg(["min", "max"]).reset_index()

        # Get dates to add (preliminary)
        start = since_time
        end = until_time
        if EXTEND_END:
            start = df[time_col].min()
        elif EXTEND_START:
            end = df[time_col].max()
        if isinstance(start, datetime):
            date_values = pd.date_range(start=start, end=end)
        else:
            date_values = range(start, end + 1)  # type: ignore

        # Build ranges to add (preliminary)
        iterables, names = _get_iter_and_names(
            df.reset_index(),
            SINGLE_DIMENSION,
            dimension_col,
            date_values,
        )
        df_range = pd.MultiIndex.from_product(iterables, names=names).to_frame(index=False)
        df_range = df_range.merge(df_bounds, on=dimension_col)

        # Filter and get the actual range to extend
        if EXTEND_END:
            df_range = df_range.loc[df_range[time_col] > df_range["max"]]
        elif EXTEND_START:
            df_range = df_range.loc[df_range[time_col] < df_range["min"]]
        else:
            df_range = df_range.loc[(df_range[time_col] < df_range["min"]) | (df_range[time_col] > df_range["max"])]
        df_range = df_range.drop(columns=["min", "max"])

        # Extend the dataframe
        if isinstance(df, Table):
            df = pr.concat([df, Table(df_range)])  #  type: ignore
        elif isinstance(df, pd.DataFrame):
            df = pd.concat([df, df_range])  # type: ignore

    df = df.sort_values(index)

    #####################################################################
    # Fill method
    #####################################################################
    values_column = [col for col in df.columns if col not in index]

    def _fillna(df: Any, method: Any):
        if method == "interpolate":
            df = interpolate_table(
                df,
                dimension_col,
                time_col,
                "none",  # NOTE: DO NOT CHANGE THIS, CAN LEAD TO CIRCULAR LOOP
                limit_area="inside",
            )
        elif method == "bfill":
            df[values_column] = df.groupby(dimension_col)[values_column].bfill()
        elif method == "ffill":
            df[values_column] = df.groupby(dimension_col)[values_column].ffill()
        elif method == "zero":
            df[values_column] = df.groupby(dimension_col)[values_column].fillna(0)
        return df

    DID_INTERPOLATE = False
    if values_column:
        if isinstance(fillna_method, list):
            if "interpolate" in fillna_method:
                DID_INTERPOLATE = True
            for m in fillna_method:
                df = _fillna(df, m)
        else:
            if "interpolate" == fillna_method:
                DID_INTERPOLATE = True
            df = _fillna(df, fillna_method)

    #####################################################################
    # Final touches
    #####################################################################
    # Output dataframe in same order as input
    df = df.loc[:, columns_order]

    if not DID_INTERPOLATE:  # type: ignore
        try:
            df = df.astype(dtypes)
        except pd.errors.IntCastingNaNError:
            pass

    if dimension_col == DIMENSION_COL_NONE:
        df = df.drop(columns=dimension_col)

    return df


def explode_rows_by_time_range(
    tb: Table,
    col_time_start: str,
    col_time_end: str,
    col_time: str,
    cols_scale: Optional[List[str]] = None,
) -> Table:
    """Expand a table to have a row per time unit given a range.

    Example
    -------

        Input:

        | value | time_start | time_end |
        |---------|------------|----------|
        | 1       | 1990       | 1993     |

        Output:

        |  time | value |
        |-------|---------|
        |  1990 |    1    |
        |  1991 |    1    |
        |  1992 |    1    |

    Parameters
    ----------
    tb : Table
        Original table, where each row is describes a period. It should have two columns determining the time period.
    col_time_start: str
        Name of the column that contains the lower-bound time range. Only year is supported for now.
    col_time_end: str
        Name of the column that contains the upper-bound time range. Only year is supported for now.
    col_time: str
        Name of the new column for time. E.g. 'year'.
    cols_scale: List[str]
        If given, column specified by this will be scaled based on the length of the time period. E.g. if the value was '10' over the whole period of 20 years, the new rows per year will have the value '0.5'.

    Returns
    -------
    Table
        Here, each conflict has as many rows as years of activity. Its deaths have been uniformly distributed among the years of activity.
    """
    # For that we scale the number of deaths proportional to the duration of the conflict.
    if cols_scale:
        for col in cols_scale:
            tb[col] = (tb[col] / (tb[col_time_end] - tb[col_time_start] + 1)).copy_metadata(tb[col])

    # Add missing times for each triplet
    TIME_MIN = tb[col_time_start].min()
    TIME_MAX = tb[col_time_end].max()
    tb_all_times = Table(pd.RangeIndex(TIME_MIN, TIME_MAX + 1), columns=[col_time])
    tb = tb.merge(tb_all_times, how="cross")
    # Filter only entries that actually existed
    tb = tb.loc[(tb[col_time] >= tb[col_time_start]) & (tb[col_time] < tb[col_time_end])]

    return tb


def bard(a, b, eps=1e-8):
    """Bounded Adjusted Relative Deviation (BARD) between two values or two series.

    Given a and b (that can be either single real values or series), the BARD is defined as:
    BARD(a, b) = |a - b| / (|a| + |b| + eps)
    where epsilon (or eps) is a small positive constant to avoid large deviations on small numbers.

    Common error metrics, like the absolute percentage error, have some important drawbacks:
    - They are not bounded, so they can be arbitrarily large.
    - They are not symmetric, so they can be misleading when comparing two values.
    - They can become very large (or even undefined) when comparing very small numbers (which are usually irrelevant).

    The BARD is a bounded, symmetric, and well-behaved error metric that can be used to compare two values.
    To have a sense of its meaning, notice that:
    - The BARD is always bounded between 0 and 1, which makes it a convenient metric.
    - BARD(a, b) = 0 if and only if a and b are equal.
    - BARD(a, b) tends to 1 when one of the two values is much larger than the other (and much larger than eps).
    - BARD(a, b) is symmetric, so BARD(a, b) = BARD(b, a).
    - BARD(a, b) is well-behaved for small numbers, as tends to 0 when a and b are small (with respect to eps).
    - When a and b are much larger than eps (the most common case):
        - When BARD(a, b) = 0.5, |a - b| = (|a| + |b|) / 2. In other words, a BARD of 50% means that the absolute deviation is similar to the absolute mean value of a and b.
        - And, when BARD >> 0.5, the absolute deviation is much larger than the absolute mean value of a and b.
        - When BARD ~ 0, the absolute deviation between a and b is much smaller than a (and hence also b).
    - When a and b are of a similar magnitude than eps, we enter a regime where we don't care about big deviations. The additional eps term in the denominator makes the BARD insensitive to large deviations when a and b are small.
        - For example, when |a| + |b| ~ eps, BARD becomes 1/2 of the BARD we would have if eps was 0.
        - Even if a >> b, BARD(a, b) ~ |a| / (|a| + eps). So, if eps >= |a|, this means that BARD <= 1/2 (and a similar thing happens for b >> a). In other words, even when there are big deviations of small numbers, BARD is at most 50%.

    Parameters
    ----------
    a : float or pd.Series
        One of the quantities to compare.
    b : float or pd.Series
        One of the quantities to compare.
    eps : float, optional
        Small constant to avoid big deviations of small numbers (or divisions by zero).

    Returns
    -------
    bard : float or pd.Series
        Bounded Adjusted Relative Deviation (BARD) between a and b, given a small constant eps.

    """
    return abs(a - b) / (abs(a) + abs(b) + eps)


def compare_tables(
    old,
    new,
    columns=None,
    countries=None,
    x="year",
    country_column="country",
    legend="source",
    old_label="old",
    new_label="new",
    skip_empty=True,
    skip_equal=True,
    metric="bard_max",
    bard_eps=1e-8,
    bard_max=0.1,
    absolute_tolerance=1e-8,
    relative_tolerance=1e-8,
    max_num_charts=50,
    only_coincident_rows=False,
) -> None:
    """Plot columns of two tables (usually an "old" and a "new" version) to compare them.

    Parameters
    ----------
    old : _type_
        Old version of the data to be compared.
    new : _type_
        New version of the data to be compared.
    columns : _type_, optional
        Columns to compare. None to compare all columns.
    countries : _type_, optional
        Countries to compare. None to compare all countries.
    x : str, optional
        Name of the column to use as x-axis, by default "year".
    country_column : str, optional
        Name of the country column, by default "country".
    legend : str, optional
        Name of the new column to use as a legend, by default "source".
    old_label : str, optional
        Label for the old data, by default "old".
    new_label : str, optional
        Label for the new data, by default "new".
    skip_empty : bool, optional
        True to skip plots that have no data, by default True.
    skip_equal : bool, optional
        True to skip plots where old and new data are equal (within a certain absolute and relative tolerance), by default True.
    metric : str or callable, optional
        Only relevant if skip_equal is True. Metric to use to compare old and new data. It can be a string with the following options:
        - "are_equal": Check if old and new data are equal (within a certain absolute_tolerance and relative_tolerance).
        - "bard_max": Check if the maximum BARD between old and new data is below a certain threshold (given by bard_max), and assuming an epsilon value of bard_eps. See bard function for more details.

        It can also be a custom function that takes two pandas Series and returns a boolean, which should be True if the two Series are considered equal. For example: metric=lambda a,b: (mean(bard(a, b, eps=0.01))<0.1)
    bard_eps : float, optional
        Only relevant if skip_equal is True and metric is "bard_max". Small constant to avoid big deviations of small numbers (or divisions by zero).
    bard_max : float, optional
        Only relevant if skip_equal is True and metric is "bard_max". Maximum BARD value to consider old and new data as equal.
    absolute_tolerance : float, optional
        Only relevant if skip_equal is True and metric is "are_equal".
        Absolute tolerance when comparing old and new data, by default 1e-8.
    relative_tolerance : float, optional
        Only relevant if skip_equal is True and metric is "are_equal".
        Relative tolerance when comparing old and new data, by default 1e-8.
    only_coincident_rows : bool, optional
        True to only compare rows that are present in both tables (e.g. to ignore points for years that are only in new).
    max_num_charts : int, optional
        Maximum number of charts to show, by default 50. If exceeded, the user will be asked how to proceed.

    """
    # Ensure input data is in a dataframe format.
    df1 = pd.DataFrame(old).copy()
    df2 = pd.DataFrame(new).copy()

    # Add a column that identifies the source of data (i.e. if it is old or new data).
    df1[legend] = old_label
    df2[legend] = new_label

    if countries is None:
        # List all countries in the data.
        countries = sorted(set(df1[country_column]) | set(df2[country_column]))

    if columns is None:
        # List all common columns of both tables and exclude index and color columns.
        columns = sorted((set(df1.columns) & set(df2.columns)) - set([country_column, x, legend]))

    # Put both dataframes together.
    compared = pd.concat([df1, df2], ignore_index=True)

    # Ensure all common columns have the same numeric type.
    for column in columns:
        try:
            compared[column] = compared[column].astype(float)
        except ValueError:
            log.info(f"Skipping column {column}, which can't be converted into float.")
            compared = compared.drop(columns=column, errors="raise")
            columns.remove(column)

    # Initialize a list with all plots.
    figures = []

    # Initialize a switch to stop the loop if the user wants to.
    decision = None

    # Create a chart for each country and for each column.
    for country in tqdm(countries):
        # For convenience, disable the progress bar of the columns.
        for y_column in tqdm(columns, disable=True):
            # Select rows for the current relevant country, and select relevant column.
            filtered = compared[compared[country_column] == country][[x, legend, y_column]]
            # Remove rows with missing values.
            filtered = filtered.dropna(subset=y_column).reset_index(drop=True)
            if skip_empty and (len(filtered) == 0):
                # If there are no data points in the old or new tables for this country-column, skip this column.
                continue

            if only_coincident_rows:
                # Select only years where there is data for both old and new.
                filtered = filtered[filtered.groupby(x)["source"].transform("count") == 2].reset_index(drop=True)

            if skip_equal:
                _old = filtered[filtered[legend] == old_label].reset_index()[y_column]
                _new = filtered[filtered[legend] == new_label].reset_index()[y_column]

                if (len(_old) == 0) and (len(_new) == 0):
                    # If there are no data points in the old or new tables for this country-column, skip this column.
                    continue

                if metric == "are_equal":
                    condition = dataframes.are_equal(
                        df1=_old.to_frame(),
                        df2=_new.to_frame(),
                        verbose=False,
                        absolute_tolerance=absolute_tolerance,
                        relative_tolerance=relative_tolerance,
                    )[0]
                elif metric == "bard_max":
                    condition = max(bard(a=_old, b=_new, eps=bard_eps)) < bard_max
                else:
                    condition = metric(_old, _new)  # type: ignore

                if condition:  # type: ignore
                    # If the old and new tables are equal for this country-column, skip this column.
                    continue

            # Prepare plot.
            fig = px.line(
                filtered,
                x=x,
                y=y_column,
                color=legend,
                markers=True,
                color_discrete_map={old_label: "rgba(256,0,0,0.5)", new_label: "rgba(0,256,0,0.5)"},
                title=f"{country} - {y_column}",
            )
            figures.append(fig)

            # If the number of maximum charts is reached, stop the loop and show them.
            if len(figures) >= max_num_charts:
                decision = input(
                    f"WARNING: There are more than {len(figures)} figures.\n"
                    "* Press enter (or escape in VSCode) to continue loading more (might get slow).\n"
                    f"* Press 'o' to only show the first {max_num_charts} plots.\n"
                    "* Press 'q' to quit (and maybe set a different max_num_charts or filter the data)."
                )
                if decision in ["q", "o"]:
                    # Stop adding figures to the list.
                    break

        if decision in ["q", "o"]:
            # Break the loop over countries.
            break

    if decision != "q":
        # Plot all listed figures.
        for fig in figures:
            fig.show()


def round_to_nearest_power_of_ten(value: Union[int, float], floor: bool = True) -> float:
    """Round a number to its nearest power of ten.

    If `floor`, values are rounded down, e.g. 123 -> 100. Otherwise, they are rounded up, e.g. 123 -> 1000.

    NOTE: For convenience, negative numbers are rounded down in absolute value.
    For example, when `floor` is True, -123 -> -100.

    Parameters
    ----------
    value : Union[int, float]
        Number to round.
    floor : bool, optional
        Whether to round the value down or up.

    Returns
    -------
    float
        Nearest power of ten.
    """
    if value == 0:
        return 0

    if floor:
        rounded_value = 10 ** (math.floor(math.log10(abs(value))))
    else:
        rounded_value = 10 ** (math.ceil(math.log10(abs(value))))

    if value < 0:
        rounded_value = -rounded_value

    return rounded_value


def round_to_sig_figs(value: Union[int, float], sig_figs: int = 1) -> float:
    """Round a number to a fixed amount of significant figures.

    For example, if `sig_figs=1`:
    * 0.123 -> 0.1
    * 0.992 -> 1
    * 12.3 -> 10
    And, if `sig_figs=2`:
    * 0.123 -> 0.12
    * 0.992 -> 0.99
    * 12.3 -> 12

    NOTE: Python will always ignore trailing zeros (even when printing in scientific notation).
    We could have a function that returns a string that respects significant trailing zeros.
    But for now, this is good enough.

    Parameters
    ----------
    value : Union[int, float]
        Number to round.
    sig_figs : int, optional
        Number of significant figures.

    Returns
    -------
    float
        Rounded value.
    """
    return round(value, sig_figs - 1 - math.floor(math.log10(abs(value if value != 0 else 1))))


def round_to_shifted_power_of_ten(
    value: Union[int, float], shifts: Optional[List[int]] = None, floor: bool = True
) -> Union[int, float]:
    """Round a number to its nearest power of ten, shifted by a certain coefficient.

    By default, the coefficients are 1, 2, 3, and 5.

    If `floor` is True, values are rounded down, e.g. 123 -> 100. Otherwise, they are rounded up, e.g. 123 -> 200.

    For example (if `floor` is True):
    0 -> 0
    0.1 -> 0.1
    0.09 -> 0.05
    0.11 -> 0.1
    123 -> 100
    199 -> 100
    201 -> 200
    350 -> 300
    500 -> 500

    NOTE: For convenience, negative numbers are rounded down in absolute value.
    For example, when `floor` is True, -123 -> -100.

    Parameters
    ----------
    value : Union[int, float]
        Number to round.
    shifts : Optional[List[int]]
        Coefficients that determine the shift from the closest power of ten.
    floor : bool, optional
        Whether to round the value down (if True) or up (if False).

    Returns
    -------
    closest_shifted_value : Union[int,float]
        Nearest shifted power of ten.
    """
    if value == 0:
        # Handle special case for zero.
        return 0

    # Define the absolute value.
    value_abs = abs(value)

    if shifts is None:
        shifts = [1, 2, 3, 5]

    # Find the closest power of 10 that is smaller than the value.
    log_value = math.log10(value_abs)
    power_of_10 = 10 ** math.floor(log_value)

    # Generate all possible values shifted by the coefficients given in "shifts".
    _values = [power_of_10 * shift for shift in shifts]

    if floor:
        # Find the largest shifted value that is still smaller than or equal to the given value.
        closest_shifted_value = max([_value for _value in _values if _value <= value_abs], default=power_of_10)
    else:
        # Generate possible shifted values for the next power of 10.
        next_power_of_10 = 10 ** math.ceil(log_value)
        _values += [next_power_of_10 * shift for shift in shifts]
        # Find the smallest shifted value that is still greater than or equal to the given value.
        closest_shifted_value = min([_value for _value in _values if _value >= value_abs], default=next_power_of_10)

    # Due to floating precision errors, the returned number differs from the expected one.
    # Round to 1 significant figure.
    closest_shifted_value = round_to_sig_figs(closest_shifted_value, sig_figs=1)

    # Respect the type of the input value.
    if isinstance(value, int):
        closest_shifted_value = int(closest_shifted_value)

    # Respect the sign of the input value.
    if value < 0:
        closest_shifted_value = -closest_shifted_value

    return closest_shifted_value


def humanize_number(number, sig_figs=2):
    """Create a human-readable string representation of a number.

    NOTE: This function has been designed to handle mostly positive integers, so it may not work as expected in other cases. Feel free to generalize it if new use cases arise.

    Examples
    --------
    >>> humanize_number(3)
    'three'
    >>> humanize_number(12)
    '12'
    >>> humanize_number(1234567, sig_figs=2)
    '1.2 million'
    >>> humanize_number(1234567, sig_figs=3)
    '1.23 million'
    NOTE: By convention, we do not use the word 'thousand'.
    >>> humanize_number(1234)
    '1,200'
    >>> humanize_number(123456, sig_figs=2)
    '120,000'

    Parameters
    ----------
    number : int or float
        The number to be humanized.
    sig_figs : int, optional
        The number of significant figures to use for rounding.

    Returns
    -------
    str
        A human-readable string representation of the number, with appropriate scaling (e.g., million, billion).

    """
    if isinstance(number, int) and (number < 11):
        humanized = {
            0: "zero",
            1: "one",
            2: "two",
            3: "three",
            4: "four",
            5: "five",
            6: "six",
            7: "seven",
            8: "eight",
            9: "nine",
            10: "ten",
        }[number]
    else:
        scale_factors = {
            "quadrillion": 1e15,
            "trillion": 1e12,
            "billion": 1e9,
            "million": 1e6,
        }
        for scale_name, threshold in scale_factors.items():
            if number >= threshold:
                value = round_to_sig_figs(number / threshold, sig_figs)
                break
        else:
            value = round_to_sig_figs(number, sig_figs)
            scale_name = ""

        # Format number with commas.
        value_str = f"{value:,}"

        # Remove trailing zeros.
        if ("." in value_str) and (len(value_str.split(".")[0]) >= sig_figs):
            value_str = value_str.split(".")[0]

        # Combine number and scale.
        humanized = f"{value_str} {scale_name}".strip()

    return humanized


def _format_list_metadata(key: str, value: list) -> str:
    """Format list metadata based on key type."""
    if key in ["description_key", "origins", "licenses"]:
        return "\n".join(str(item) for item in value)
    elif len(str(value)) > 100:
        return f"[List with {len(value)} items] {str(value)[:100]}..."
    else:
        return ", ".join(str(item) for item in value)


def _prepare_dataframe(table: Table) -> pd.DataFrame:
    """Convert table to DataFrame and optimize for Google Sheets."""
    # Reset index only if it's not the default RangeIndex
    if not isinstance(table.index, pd.RangeIndex) or table.index.name is not None:
        df = table.reset_index()
    else:
        df = table.copy()

    if isinstance(df, Table):
        df = pd.DataFrame(df)

    # Optimize data types for Google Sheets
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)

    return df


def retry_on_network_error(max_retries: int = 3, base_delay: float = 1.0):
    """Decorator to retry on network/SSL errors."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if (
                        any(term in str(e) for term in ["SSL", "EOF", "Connection", "Timeout"])
                        and attempt < max_retries - 1
                    ):
                        delay = base_delay * (2**attempt) + random.uniform(0, 1)
                        log.warning(
                            f"Network error in function '{func.__name__}' on attempt {attempt + 1}: {str(e)}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                        continue
                    raise
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _get_variables_to_process(table: Table, metadata_variables: Optional[List[str]]) -> List[str]:
    """Determine which variables to process for metadata export."""
    if metadata_variables is None:
        return list(table.columns)

    variables_to_process = [var for var in metadata_variables if var in table.columns]
    missing_vars = [var for var in metadata_variables if var not in table.columns]
    if missing_vars:
        log.warning(f"Warning: Variables {missing_vars} not found in table")

    return variables_to_process


def _extract_metadata_rows(metadata_obj, included_fields: set) -> List[List[str]]:
    """Extract metadata rows from a metadata object, keeping only specified fields."""
    rows = []

    # Handle both dict and object with __dict__
    items = metadata_obj.items() if isinstance(metadata_obj, dict) else metadata_obj.__dict__.items()

    for key, value in items:
        # Only include specified fields that have non-empty values
        if key not in included_fields or value is None or value == "":
            continue

        # Get display name
        display_name = GSHEET_EXPORT_CONFIG["FIELD_DISPLAY_NAMES"].get(key, key)

        if isinstance(value, list):
            # Handle special case for origins - extract attributes for each
            if key == "origins":
                for origin in value:
                    # Get origin identifier with proper type handling
                    origin_identifier = None

                    # Try attribution_short first
                    if hasattr(origin, "attribution_short") and getattr(origin, "attribution_short", None):
                        origin_identifier = origin.attribution_short
                    elif isinstance(origin, dict) and origin.get("attribution_short"):
                        origin_identifier = origin["attribution_short"]
                    # Fall back to producer
                    elif hasattr(origin, "producer") and getattr(origin, "producer", None):
                        origin_identifier = origin["producer"]
                    elif isinstance(origin, dict) and origin.get("producer"):
                        origin_identifier = origin["producer"]

                    # Create display name with identifier or use base name
                    if origin_identifier:
                        origin_display_name = f"{display_name} ({origin_identifier})"
                    else:
                        origin_display_name = display_name

                    rows.extend(_extract_origin_attributes(origin, origin_display_name))
            else:
                # For other list fields, use the original formatting
                formatted_value = _format_list_metadata(key, value)
                rows.append([display_name, formatted_value])
        elif isinstance(value, dict) and len(str(value)) > 100:
            formatted_value = f"[{type(value).__name__}] {str(value)[:100]}..."
            rows.append([display_name, formatted_value])
        else:
            formatted_value = str(value)
            rows.append([display_name, formatted_value])

    return rows


def _extract_origin_attributes(origin, base_display_name: str) -> List[List[str]]:
    """Extract individual attributes from an Origin object and format them in a single cell."""
    # Define the origin attributes we want to show and their display names
    origin_field_mapping = {
        "producer": "Producer",
        "title": "Title",
        "description": "Description",
        "citation_full": "Citation",
        "attribution_short": "Attribution",
        "url_main": "Main URL",
        "url_download": "Download URL",
        "date_accessed": "Date accessed",
        "date_published": "Date published",
    }

    # Handle both dict and object with __dict__
    items = origin.items() if isinstance(origin, dict) else origin.__dict__.items()

    # Collect all attributes into a single formatted string
    formatted_attributes = []

    for attr_key, attr_value in items:
        if attr_value is None or attr_value == "":
            continue

        if attr_key == "license":
            # Handle license object specially
            if hasattr(attr_value, "name") and attr_value.name:
                formatted_attributes.append(f"License: {attr_value.name}")
            if hasattr(attr_value, "url") and attr_value.url:
                formatted_attributes.append(f"License URL: {attr_value.url}")
        elif attr_key in origin_field_mapping:
            attr_display_name = origin_field_mapping[attr_key]
            formatted_attributes.append(f"{attr_display_name}: {str(attr_value)}")

    # Join all attributes with line breaks
    formatted_value = "\n".join(formatted_attributes)

    # Return a single row with all the origin information
    return [[base_display_name, formatted_value]]


def export_table_to_gsheet(
    table: Table,
    sheet_title: str,
    update_existing: bool = True,
    folder_id: Optional[str] = None,
    role: Literal["reader", "commenter", "writer"] = "reader",
    general_access: Literal["anyone", "domain", "user"] = "anyone",
    include_metadata: bool = True,
    metadata_variables: Optional[List[str]] = None,
) -> tuple[str, str]:
    """Export a Table to Google Sheets with improved error handling and performance.

    Returns
    -------
    tuple[str, str]
        A tuple containing (sheet_url, sheet_id)

    Raises
    ------
    RuntimeError
        If the export fails or if CLIENT_SECRET_FILE is not available in dev environment
    """
    if not CLIENT_SECRET_FILE or not CLIENT_SECRET_FILE.exists() or OWID_ENV.env_local != "dev":
        raise RuntimeError(
            "Google Sheets export is only available in development environment with valid CLIENT_SECRET_FILE"
        )

    try:
        # Sanitize sheet title
        sheet_title = sheet_title.strip()[: GSHEET_EXPORT_CONFIG["MAX_TITLE_LENGTH"]]

        # Convert and validate data
        df = _prepare_dataframe(table)

        # Create or update sheet
        sheet = _update_or_create_sheet(sheet_title, folder_id, df, update_existing)

        # Write main data
        sheet.write_dataframe(df)

        # Add metadata if requested
        if include_metadata:
            _add_metadata_tabs(sheet, table, metadata_variables)

        # Set permissions
        _set_permissions(sheet.sheet_id, role, general_access)

        return sheet.url, sheet.sheet_id

    except Exception as e:
        log.error(f"Failed to export table to Google Sheets: {e}")
        raise RuntimeError(f"Failed to export table to Google Sheets: {e}")


def _update_or_create_sheet(
    sheet_title: str, folder_id: Optional[str], df: pd.DataFrame, update_existing: bool
) -> GoogleSheet:
    """Update existing sheet or create new one."""
    if not update_existing:
        return GoogleSheet.create_sheet(sheet_title, folder_id)

    # Try to find existing sheet in the specified folder
    try:
        drive = GoogleDrive()
        query = f"name='{sheet_title}' and mimeType='application/vnd.google-apps.spreadsheet'"

        # If folder_id is specified, search within that folder
        if folder_id:
            query += f" and '{folder_id}' in parents"

        results = drive.drive_service.files().list(q=query).execute()
        files = results.get("files", [])

        if files:
            sheet = GoogleSheet(files[0]["id"])
            # Clear and write to first sheet
            sheet_metadata = sheet.sheets_service.spreadsheets().get(spreadsheetId=files[0]["id"]).execute()
            first_tab_name = sheet_metadata["sheets"][0]["properties"]["title"]
            sheet.clear_sheet_data(first_tab_name)
            sheet.write_dataframe(df, sheet_name=first_tab_name)
            return sheet
    except HttpError as e:
        log.error(f"Warning: Google Drive API error while updating existing sheet: {e}")
    except Exception as e:
        log.error(f"Critical: Unexpected error while updating existing sheet: {e}")

    # Fallback to creating new sheet in the specified folder
    return GoogleSheet.create_sheet(sheet_title, folder_id)


def _add_metadata_tabs(sheet: GoogleSheet, table: Table, metadata_variables: Optional[List[str]]) -> None:
    """Add metadata tabs to the sheet."""

    """Create metadata DataFrames for each variable in the table."""
    metadata_dfs = {}

    variables_to_process = _get_variables_to_process(table, metadata_variables)

    for column_name in variables_to_process:
        if not (hasattr(table[column_name], "metadata") and table[column_name].metadata):
            continue

        try:
            metadata_rows = _extract_metadata_rows(
                table[column_name].metadata, GSHEET_EXPORT_CONFIG["INCLUDED_METADATA_FIELDS"]
            )

            if metadata_rows:
                # Create DataFrame with proper column headers
                metadata_df = pd.DataFrame(metadata_rows, columns=["Property", "Value"])
                metadata_dfs[f"metadata_{column_name}"] = metadata_df

        except Exception as e:
            log.warning(f"Warning: Could not process metadata for column '{column_name}': {e}")
            continue
    for sheet_name, metadata_df in metadata_dfs.items():
        try:
            sheet.write_dataframe(metadata_df, sheet_name=sheet_name)
        except Exception as e:
            log.warning(f"Note: Could not create metadata sheet '{sheet_name}': {e}")


def _set_permissions(sheet_id: str, role: str, general_access: str) -> None:
    """Set permissions for the sheet."""
    drive = GoogleDrive()
    drive.set_file_permissions(file_id=sheet_id, role=role, general_access=general_access)


def create_or_get_shared_folder(
    folder_name: str = "ETL GSheet Exports",
    parent_folder_id: Optional[str] = None,
) -> Optional[str]:
    """Create or get a shared folder for storing multiple Google Sheets.

    Parameters
    ----------
    folder_name : str
        Name of the folder to create or find
    parent_folder_id : Optional[str], optional
        Parent folder ID, by default None (creates in root)

    Returns
    -------
    Optional[str]
        The folder ID, or None if creation failed
    """
    try:
        drive = GoogleDrive()

        # Search for existing folder
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"

        results = drive.drive_service.files().list(q=query).execute()
        files = results.get("files", [])

        if files:
            folder_id = files[0]["id"]
            log.info(f"Using existing folder: {folder_name} (ID: {folder_id})")
            return folder_id

        # Create new folder
        folder_metadata: Dict[str, Any] = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}

        if parent_folder_id:
            folder_metadata["parents"] = [parent_folder_id]

        folder = drive.drive_service.files().create(body=folder_metadata, fields="id").execute()
        folder_id = folder.get("id")

        if folder_id is None:
            log.error(f"Failed to create folder: {folder_name}")
            return None

        # Set permissions for team access
        drive.set_file_permissions(file_id=folder_id, role="reader", general_access="anyone")

        log.info(f"Created new folder: {folder_name} (ID: {folder_id})")
        log.info(f"Folder URL: https://drive.google.com/drive/folders/{folder_id}")

        return folder_id

    except HttpError as e:
        log.error(f"HTTP error occurred while creating/accessing folder '{folder_name}': {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error occurred while creating/accessing folder '{folder_name}': {e}")
        raise


def get_team_folder_id() -> Optional[str]:
    """Get the team folder ID for OWID ETL exports."""
    # Use the shared folder ID if available
    if OWID_SHARED_FOLDER_ID:
        try:
            # Verify the current user can access this folder
            drive = GoogleDrive()
            drive.drive_service.files().get(fileId=OWID_SHARED_FOLDER_ID).execute()
            return OWID_SHARED_FOLDER_ID
        except Exception as e:
            log.warning(f"Warning: Cannot access shared folder {OWID_SHARED_FOLDER_ID}: {e}")

    return None
