"""General data tools.

Use this module with caution. Functions added here are half-way their final destination: owid-datautils.

When working on a specific project, it is often the case that we may identify functions that can be useful for other projects. These functions
should probably be moved to owid-datautils. However this can be time consuming at the time we are working on the project. Therefore:

- By adding them here we make them available for other projects.
- We have these functions in one place if we ever wanted to move them to owid-datautils.
- Prior to moving them to owid-datautils, we can test and discuss them.

"""
from typing import Any, List, Set, Union

import pandas as pd
import plotly.express as px
from owid.catalog import License, Origin, Table
from owid.datautils import dataframes
from tqdm.auto import tqdm


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


########################################################################################################################
# TODO: Remote this temporary function once WDI has origins.
def add_origins_to_wdi(tb_wdi: Table) -> Table:
    tb_wdi = tb_wdi.copy()

    # List all non-index columns in the WDI table.
    data_columns = [column for column in tb_wdi.columns if column not in ["country", "year"]]

    # For each indicator, add an origin (using information from the old source) and then remove the source.
    for column in data_columns:
        assert len(tb_wdi[column].metadata.sources) == 1, f"Expected only one source in column {column}"
        source = tb_wdi[column].metadata.sources[0]
        error = "Remove temporary solution where origins where manually created."
        assert tb_wdi[column].metadata.origins == [], error
        tb_wdi[column].metadata.origins = [
            Origin(
                title="World Development Indicators",
                producer=source.name,
                attribution="Multiple sources compiled by World Bank (2023)",
                url_main="https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators",
                url_download="http://databank.worldbank.org/data/download/WDI_csv.zip",
                date_accessed="2023-05-29",
                date_published="2023-05-11",
                citation_full="World Bank's World Development Indicators (WDI).",
                description="The World Development Indicators (WDI) is the primary World Bank collection of development indicators, compiled from officially-recognized international sources. It presents the most current and accurate global development data available, and includes national, regional and global estimates.",
                license=License(name="CC BY 4.0"),
            )
        ]

        # Remove sources from indicator.
        tb_wdi[column].metadata.sources = []

    return tb_wdi


def add_origins_to_energy_table(tb_energy: Table) -> Table:
    tb_energy = tb_energy.copy()

    # List all non-index columns.
    data_columns = [column for column in tb_energy.columns if column not in ["country", "year"]]

    # For each indicator, add an origin and remove sources.
    for column in data_columns:
        tb_energy[column].metadata.sources = []
        tb_energy[column].metadata.origins = [
            Origin(
                producer="Energy Institute",
                title="Statistical Review of World Energy",
                attribution="Energy Institute - Statistical Review of World Energy (2023)",
                url_main="https://www.energyinst.org/statistical-review/",
                url_download="https://www.energyinst.org/__data/assets/file/0007/1055761/Consolidated-Dataset-Panel-format-CSV.csv",
                date_published="2023-06-26",
                date_accessed="2023-06-27",
                description="The Energy Institute Statistical Review of World Energy analyses data on world energy markets from the prior year.",
                license=License(
                    name="©Energy Institute 2023",
                    url="https://www.energyinst.org/__data/assets/file/0007/1055761/Consolidated-Dataset-Panel-format-CSV.csv",
                ),
            ),
            Origin(
                producer="U.S. Energy Information Administration",
                title="International Energy Data",
                url_main="https://www.eia.gov/opendata/bulkfiles.php",
                url_download="https://api.eia.gov/bulk/INTL.zip",
                date_published="2023-06-27",
                date_accessed="2023-07-10",
                license=License(name="Public domain", url="https://www.eia.gov/about/copyrights_reuse.php"),
            ),
        ]

    return tb_energy


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
