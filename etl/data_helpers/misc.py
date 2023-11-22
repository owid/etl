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


def add_origins_to_education_fasttracked(tb: Table) -> Table:
    tb = tb.copy()

    # For the historical indicator, add an origin (using information from the old sources) and then remove the source.
    for column in ["combined_expenditure", "combined_literacy"]:
        tb[column].metadata.sources = []
        if column == "combined_expenditure":
            tb[column].metadata.origins = tb[column].metadata.origins + [
                Origin(
                    title="Public Expenditure on Education OECD - Tanzi & Schuknecht (2000)",
                    producer="Tanzi and Schuknecht",
                    url_main="https://link.springer.com/article/10.1023%2FA%3A1017578302202?LI=true",
                    date_accessed="2017-09-30",
                    date_published="2023",
                    citation_full="Tanzi and Schuknecht (2000) Public Spending in the 20th Century A Global Perspective",
                    description="The underlying sources for Tanzi & Schuknecht (2000) include: League of Nations Statistical Yearbook (various years), Mitchell (1962), OECD Education at a Glance (1996), UNESCO World Education Report (1993), UNDP Human Development Report (1996), UN World Economics Survey (various years). To the extent that the authors do not specify which sources were prioritised for each year/country, it is not possible for us to reliably extend the time series with newer data. For instance, the OECD Education at a Glance report (1998), which presents estimates for the years 1990 and 1995, suggests discrepancies with the values reported by Tanzi & Schuknecht (2000) for 1993.",
                )
            ]

        else:
            tb[column].metadata.origins = tb[column].metadata.origins + [
                Origin(
                    title="Literacy rates (World Bank, CIA World Factbook, and other sources)",
                    producer="Our World in Data based on World Bank, CIA World Factbook, and other sources",
                    date_accessed="2018-04-18",
                    date_published="2018",
                    citation_full="Our World in Data based on World Bank, CIA World Factbook, and other sources.",
                    description="This dataset, curated by Our World in Data, is an extensive cross-country compilation of literacy rates, primarily based on the World Bank's World Development Indicators. It enriches this data with literacy estimates from the CIA World Factbook and other historical sources. These include pre-1800 data from Buringh and Van Zanden (2009), 1820 and 1870 data from Broadberry and O'Rourke (2010), U.S. specific data from the National Center for Education Statistics, global estimates for 1820-2000 from van Zanden et al. (2014), and historical data for Latin America from OxLAD. The dataset also features the most recent literacy rates for high-income countries and available 2016 estimates. Notably, it acknowledges variations among sources and advises careful interpretation of year-to-year changes, highlighting discrepancies in methodologies and definitions across different sources. Additionally, it includes specific instances where data preferences were made, as in the case of Paraguay in 1982.",
                )
            ]

        # Remove sources from indicator.
        tb[column].metadata.sources = []

    return tb
