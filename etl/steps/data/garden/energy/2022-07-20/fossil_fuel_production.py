"""Garden step for Fossil fuel production dataset (part of the OWID Energy dataset), based on a combination of
BP Statistical Review dataset and Shift data on fossil fuel production.

"""

import pandas as pd
from owid import catalog
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger
from typing import cast

from etl.paths import DATA_DIR
from shared import CURRENT_DIR

log = get_logger()

NAMESPACE = "energy"
DATASET_SHORT_NAME = "fossil_fuel_production"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
SHIFT_DATASET_NAME = "fossil_fuel_production"
SHIFT_VERSION = "2022-07-18"

# Original BP's Statistical Review Dataset name in the owid catalog (without the institution and year).
BP_DATASET_NAME = "statistical_review"
BP_NAMESPACE = "bp"
BP_VERSION = "2022-07-14"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION = {
    "Czechoslovakia": {
        "continent": "Europe",
        "income_group": "High-income countries",
        "members": [
            # Europe - High-income countries.
            "Czechia",
            "Slovakia",
        ],
    },
    "Netherlands Antilles": {
        "continent": "North America",
        "income_group": "High-income countries",
        "members": [
            # North America - High-income countries.
            "Aruba",
            "Curacao",
            "Sint Maarten (Dutch part)",
        ],
    },
    "USSR": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Lithuania",
            "Estonia",
            "Latvia",
            # Europe - Upper-middle-income countries.
            "Moldova",
            "Belarus",
            "Russia",
            # Europe - Lower-middle-income countries.
            "Ukraine",
            # Asia - Upper-middle-income countries.
            "Georgia",
            "Armenia",
            "Azerbaijan",
            "Turkmenistan",
            "Kazakhstan",
            # Asia - Lower-middle-income countries.
            "Kyrgyzstan",
            "Uzbekistan",
            "Tajikistan",
        ],
    },
    "Yugoslavia": {
        "continent": "Europe",
        "income_group": "Upper-middle-income countries",
        "members": [
            # Europe - High-income countries.
            "Croatia",
            "Slovenia",
            # Europe - Upper-middle-income countries.
            "North Macedonia",
            "Bosnia and Herzegovina",
            "Serbia",
            "Montenegro",
        ],
    },
}


def load_bp_data() -> catalog.Table:
    # Load BP Statistical Review dataset.
    bp_dataset = catalog.Dataset(
        DATA_DIR / "garden" / BP_NAMESPACE / BP_VERSION / BP_DATASET_NAME
    )

    # Get table.
    bp_table = bp_dataset[bp_dataset.table_names[0]].reset_index()
    bp_columns = {
        "country": "country",
        "year": "year",
        "coal_production__twh": "Coal production (TWh)",
        "gas_production__twh": "Gas production (TWh)",
        "oil_production__twh": "Oil production (TWh)",
    }
    bp_table = bp_table[list(bp_columns)].rename(columns=bp_columns)

    return bp_table


def load_shift_data() -> catalog.Table:
    shift_columns = {
        "country": "country",
        "year": "year",
        "coal": "Coal production (TWh)",
        "gas": "Gas production (TWh)",
        "oil": "Oil production (TWh)",
    }
    shift_dataset = catalog.Dataset(
        DATA_DIR / "garden" / "shift" / SHIFT_VERSION / SHIFT_DATASET_NAME
    )
    shift_table = shift_dataset[shift_dataset.table_names[0]].reset_index()
    shift_table = shift_table[list(shift_columns)].rename(columns=shift_columns)

    return shift_table


def combine_bp_and_shift_data(
    bp_table: catalog.Table, shift_table: catalog.Table
) -> pd.DataFrame:
    # Check that there are no duplicated rows in any of the two datasets.
    assert bp_table[
        bp_table.duplicated(subset=["country", "year"])
    ].empty, "Duplicated rows in BP data."
    assert shift_table[
        shift_table.duplicated(subset=["country", "year"])
    ].empty, "Duplicated rows in Shift data."

    # Combine Shift data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    data_columns = [col for col in bp_table.columns if col not in index_columns]
    # We should not concatenate bp and shift data directly, since there are nans in different places.
    # Instead, go column by column, concatenate, remove nans, and then keep the BP version on duplicated rows.

    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in data_columns:
        _shift_data = shift_table[index_columns + [variable]].dropna(subset=variable)
        _bp_data = bp_table[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_shift_data, _bp_data], ignore_index=True)
        # On rows where both datasets overlap, give priority to BP data.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="last")
        # Combine data for different variables.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(df: pd.DataFrame) -> pd.DataFrame:
    """Add annual change variables to combined BP & Shift dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & Shift dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & Shift dataset after adding annual change variables.

    """
    combined = df.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"Annual change in {cat.lower()} production (%)"] = (
            combined.groupby("country")[f"{cat} production (TWh)"].pct_change() * 100
        )
        combined[f"Annual change in {cat.lower()} production (TWh)"] = combined.groupby(
            "country"
        )[f"{cat} production (TWh)"].diff()

    return combined


def load_population() -> pd.DataFrame:
    """Load OWID population dataset, and add historical regions to it.

    Returns
    -------
    population : pd.DataFrame
        Population dataset.

    """
    # Load population dataset.
    population = (
        catalog.find("population", namespace="owid", dataset="key_indicators")
        .load()
        .reset_index()[["country", "year", "population"]]
    )

    # Add data for historical regions (if not in population) by adding the population of its current successors.
    countries_with_population = population["country"].unique()
    missing_countries = [
        country
        for country in HISTORIC_TO_CURRENT_REGION
        if country not in countries_with_population
    ]
    for country in missing_countries:
        members = HISTORIC_TO_CURRENT_REGION[country]["members"]
        _population = (
            population[population["country"].isin(members)]
            .groupby("year")
            .agg({"population": "sum", "country": "nunique"})
            .reset_index()
        )
        # Select only years for which we have data for all member countries.
        _population = _population[_population["country"] == len(members)].reset_index(
            drop=True
        )
        _population["country"] = country
        population = pd.concat(
            [population, _population], ignore_index=True
        ).reset_index(drop=True)

    error = "Duplicate country-years found in population. Check if historical regions changed."
    assert population[population.duplicated(subset=["country", "year"])].empty, error

    return cast(pd.DataFrame, population)


def add_population(
    df: pd.DataFrame,
    country_col: str = "country",
    year_col: str = "year",
    population_col: str = "population",
    warn_on_missing_countries: bool = True,
    show_full_warning: bool = True,
) -> pd.DataFrame:
    """Add a column of OWID population to the countries in the data, including population of historical regions.

    This function has been adapted from datautils.geo, because population currently does not include historic regions.
    We include them in this function.

    Parameters
    ----------
    df : pd.DataFrame
        Data without a column for population (after harmonizing elements, items and country names).
    country_col : str
        Name of country column in data.
    year_col : str
        Name of year column in data.
    population_col : str
        Name for new population column in data.
    warn_on_missing_countries : bool
        True to warn if population is not found for any of the countries in the data.
    show_full_warning : bool
        True to show affected countries if the previous warning is raised.

    Returns
    -------
    df_with_population : pd.DataFrame
        Data after adding a column for population for all countries in the data.

    """

    # Load population dataset.
    population = load_population().rename(
        columns={
            "country": country_col,
            "year": year_col,
            "population": population_col,
        }
    )[[country_col, year_col, population_col]]

    # Check if there is any missing country.
    missing_countries = set(df[country_col]) - set(population[country_col])
    if len(missing_countries) > 0:
        if warn_on_missing_countries:
            geo.warn_on_list_of_entities(
                list_of_entities=missing_countries,
                warning_message=(
                    f"{len(missing_countries)} countries not found in population"
                    " dataset. They will remain in the dataset, but have nan"
                    " population."
                ),
                show_list=show_full_warning,
            )

    # Add population to original dataframe.
    df_with_population = pd.merge(
        df, population, on=[country_col, year_col], how="left"
    )

    return df_with_population


def add_per_capita_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables to combined BP & Shift dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & Shift dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & Shift dataset after adding per-capita variables.

    """
    df = df.copy()

    # Add population to data.
    combined = add_population(
        df=df,
        country_col="country",
        year_col="year",
        population_col="population",
        warn_on_missing_countries=False,
    )

    # Calculate production per capita.
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"{cat} production per capita (kWh)"] = (
            combined[f"{cat} production (TWh)"] / combined["population"] * TWH_TO_KWH
        )
    combined = combined.drop(errors="raise", columns=["population"])

    return combined


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Load BP statistical review dataset.
    bp_table = load_bp_data()

    # Load Shift data on fossil fuel production.
    shift_table = load_shift_data()

    #
    # Process data.
    #
    # Combine BP and Shift data.
    df = combine_bp_and_shift_data(bp_table=bp_table, shift_table=shift_table)

    # Add annual change.
    df = add_annual_change(df=df)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df)

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add metadata to dataset.
    dataset.metadata.update_from_yaml(METADATA_PATH)
    # Create new dataset in garden.
    dataset.save()

    # Create new table and add it to new dataset.
    tb_garden = underscore_table(catalog.Table(df))
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    dataset.add(tb_garden)

    log.info(f"{DATASET_SHORT_NAME}.end")
