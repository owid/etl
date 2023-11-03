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
from owid.catalog import License, Origin, Table


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
                description="The Energy Institute Statistical Review of World Energy analyses data on world energy markets from the prior year. Previously produced by BP, the Review has been providing timely, comprehensive and objective data to the energy community since 1952.",
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
