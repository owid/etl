"""General data tools.

Use this module with caution. Functions added here are half-way their final destination: owid-datautils.

When working on a specific project, it is often the case that we may identify functions that can be useful for other projects. These functions
should probably be moved to owid-datautils. However this can be time consuming at the time we are working on the project. Therefore:

- By adding them here we make them available for other projects.
- We have these functions in one place if we ever wanted to move them to owid-datautils.
- Prior to moving them to owid-datautils, we can test and discuss them.

"""

import math
from datetime import datetime
from typing import Any, List, Literal, Optional, Set, TypeVar, Union, cast

import pandas as pd
import plotly.express as px
from owid.catalog import License, Origin, Table
from owid.datautils import dataframes
from tqdm.auto import tqdm

TableOrDataFrame = TypeVar("TableOrDataFrame", pd.DataFrame, Table)


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
    entity_col: str,
    time_col: str,
    time_mode: Literal["full_range", "full_range_entity", "reduced", "none"] = "full_range",
    method: str = "linear",
    limit_direction: str = "both",
) -> TableOrDataFrame:
    """Interpolate missing values in a column linearly.

    df: Table or DataFrame
        Should contain three columns: country, year, and the column to be interpolated.
    entity_col: str
        Name of the column with entity names (typically for countries).
    time_col: str
        Name of the column with years.
    mode: str
        How to compelte time series. 'full_range' for complete range, 'full_range_entity' for complete range within an entity, 'reduced' for only time values appearing in the data. Use 'none' to interpolate with existing values.
    """

    if time_mode != "none":
        # Expand time
        df = expand_time_column(df, entity_col, time_col, time_mode)

    # Set index
    df = cast(TableOrDataFrame, df.set_index([entity_col, time_col]).sort_index())

    # Interpolate
    df = (
        df.groupby(entity_col)
        .transform(lambda x: x.interpolate(method=method, limit_direction=limit_direction))  # type: ignore
        .reset_index()
    )

    return df


def expand_time_column(
    df: TableOrDataFrame,
    entity_col: str,
    time_col: str,
    mode: Literal["full_range", "full_range_entity", "reduced"] = "full_range",
) -> TableOrDataFrame:
    """Add rows to complete the timeseries.

    You can complete the timeseries in various ways, by changing the value of `mode`

        - 'full_range': Add rows for all possible entity-time pairs within the minimum and maximum times in the data.
        - 'full_range_entity': Add rows for all possible times within the minimum and maximum times in the data for a given entity.
        - 'reduced': Add rows for all times that appear in the data. Note that some times might be present for an entity, but not for another.
    """

    def _get_complete_date_range(ds):
        date_min = ds.min()
        date_max = ds.max()
        if isinstance(date_max, datetime):
            return pd.date_range(start=date_min, end=date_max)
        else:
            return range(date_min, date_max + 1)

    assert mode in {"full_range_entity", "full_range", "reduced"}, f"Wrong value for `mode` {mode}!"

    if mode == "full_range_entity":

        def _reindex_dates(group):
            complete_date_range = _get_complete_date_range(group["date"])
            group = group.set_index("date").reindex(complete_date_range).reset_index().rename(columns={"index": "date"})
            group["country"] = group["country"].ffill().bfill()  # Fill NaNs in 'country'
            return group

        # Apply the reindexing to each group
        df = df.groupby("country").apply(_reindex_dates).reset_index(drop=True).set_index(["country", "date"])  # type: ignore
    else:
        # For some countries we have population data only on certain years, e.g. 1900, 1910, etc.
        # Optionally fill missing years linearly.
        countries_in_data = df[entity_col].unique()
        # Get list of year-country tuples
        if mode == "full_range":
            date_values = _get_complete_date_range(df[time_col])
        elif mode == "reduced":
            date_values = df[time_col].unique()
        # Reindex
        df = (
            df.set_index([entity_col, time_col])
            .reindex(pd.MultiIndex.from_product([countries_in_data, date_values], names=[entity_col, time_col]))  # type: ignore
            .sort_index()
        )

    df = cast(TableOrDataFrame, df.reset_index())
    return df


########################################################################################################################
# TODO: Remote this temporary function once WDI has origins.
def add_origins_to_mortality_database(tb_who: Table) -> Table:
    tb_who = tb_who.copy()

    # List all non-index columns in the WDI table.
    data_columns = [column for column in tb_who.columns if column not in ["country", "year"]]

    # For each indicator, add an origin (using information from the old source) and then remove the source.
    for column in data_columns:
        tb_who[column].metadata.sources = []
        error = "Remove temporary solution where origins where manually created."
        assert tb_who[column].metadata.origins == [], error
        tb_who[column].metadata.origins = [
            Origin(
                title="Mortality Database",
                producer="World Health Organisation",
                url_main="https://platform.who.int/mortality/themes/theme-details/MDB/all-causes",
                date_accessed="2023-08-01",
                date_published="2023-08-01",
                citation_full="Mortality Database, World Health Organization. Licence: CC BY-NC-SA 3.0 IGO.",
                description="The WHO mortality database is a collection death registration data including cause-of-death information from member states. Where they are collected, death registration data are the best source of information on key health indicators, such as life expectancy, and death registration data with cause-of-death information are the best source of information on mortality by cause, such as maternal mortality and suicide mortality. WHO requests from all countries annual data by age, sex, and complete ICD code (e.g., 4-digit code if the 10th revision of ICD was used). Countries have reported deaths by cause of death, year, sex, and age for inclusion in the WHO Mortality Database since 1950. Data are included only for countries reporting data properly coded according to the International Classification of Diseases (ICD). Today the database is maintained by the WHO Division of Data, Analytics and Delivery for Impact (DDI) and contains data from over 120 countries and areas. Data reported by member states and selected areas are displayed in this portal’s interactive visualizations if the data are reported to the WHO mortality database in the requested format and at least 65% of deaths were recorded in each country and year.",
                license=License(name="CC BY 4.0"),
            )
        ]

        # Remove sources from indicator.
        tb_who[column].metadata.sources = []

    return tb_who


##################################################################################
# TODO: Remote this temporary function once WDI has origins.
def add_origins_to_global_burden_of_disease(tb_gbd: Table) -> Table:
    tb_gbd = tb_gbd.copy()

    # List all non-index columns in the WDI table.
    data_columns = [column for column in tb_gbd.columns if column not in ["country", "year"]]

    # For each indicator, add an origin (using information from the old source) and then remove the source.
    for column in data_columns:
        tb_gbd[column].metadata.sources = []
        error = "Remove temporary solution where origins were manually created."
        assert tb_gbd[column].metadata.origins == [], error
        tb_gbd[column].metadata.origins = [
            Origin(
                title="Global Burden of Disease",
                producer="Institute of Health Metrics and Evaluation",
                url_main="https://vizhub.healthdata.org/gbd-results/",
                date_accessed="2021-12-01",
                date_published="2020-10-17",
                citation_full="Global Burden of Disease Collaborative Network. Global Burden of Disease Study 2019 (GBD 2019). Seattle, United States: Institute for Health Metrics and Evaluation (IHME), 2020.",
                description="The Global Burden of Disease (GBD) provides a comprehensive picture of mortality and disability across countries, time, age, and sex. It quantifies health loss from hundreds of diseases, injuries, and risk factors, so that health systems can be improved and disparities eliminated. GBD research incorporates both the prevalence of a given disease or risk factor and the relative harm it causes. With these tools, decision-makers can compare different health issues and their effects.",
                license=License(
                    name="Free-of-Charge Non-commercial User Agreement",
                    url="https://www.healthdata.org/Data-tools-practices/data-practices/ihme-free-charge-non-commercial-user-agreement",
                ),
            )
        ]

        # Remove sources from indicator.
        tb_gbd[column].metadata.sources = []

    return tb_gbd


########################################################################################################################


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
    absolute_tolerance=1e-8,
    relative_tolerance=1e-8,
    max_num_charts=50,
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
    absolute_tolerance : float, optional
        Only relevant if skip_equal is True.
        Absolute tolerance when comparing old and new data, by default 1e-8.
    relative_tolerance : float, optional
        Only relevant if skip_equal is True.
        Relative tolerance when comparing old and new data, by default 1e-8.
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
            print(f"Skipping column {column}, which can't be converted into float.")
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
            if skip_equal:
                _old = filtered[filtered[legend] == old_label].reset_index()[[y_column]]
                _new = filtered[filtered[legend] == new_label].reset_index()[[y_column]]
                if dataframes.are_equal(
                    _old,
                    _new,
                    verbose=False,
                    absolute_tolerance=absolute_tolerance,
                    relative_tolerance=relative_tolerance,
                )[0]:
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
