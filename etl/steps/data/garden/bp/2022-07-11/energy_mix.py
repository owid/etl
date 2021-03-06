"""Generate BP energy mix 2021 dataset using data from BP's statistical review of the world energy.

"""

from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from owid.datautils import geo

from owid import catalog

NAMESPACE = "bp"
VERSION = 2021
# Original BP's Statistical Review Dataset name in the owid catalog (without the institution and year).
DATASET_CATALOG_NAME = "statistical_review_of_world_energy"
NAMESPACE_IN_CATALOG = "bp_statreview"
# Path to metadata file.
METADATA_FILE_PATH = Path(__file__).parent / "energy_mix.meta.yml"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Exajoules to terawatt-hours.
EJ_TO_TWH = 1e6 / 3600
# Petajoules to exajoules.
PJ_TO_EJ = 1e-3

# List all energy sources in the data.
ONLY_DIRECT_ENERGY = ["Coal", "Fossil fuels", "Gas", "Oil", "Biofuels"]
DIRECT_AND_EQUIVALENT_ENERGY = [
    "Hydro",
    "Low-carbon energy",
    "Nuclear",
    "Other renewables",
    "Renewables",
    "Solar",
    "Wind",
]
ALL_SOURCES = sorted(ONLY_DIRECT_ENERGY + DIRECT_AND_EQUIVALENT_ENERGY)

REGIONS_TO_ADD = {
    "North America": {
        "area_code": "OWID_NAM",
    },
    "South America": {
        "area_code": "OWID_SAM",
    },
    "Europe": {
        "area_code": "OWID_EUR",
    },
    "European Union (27)": {
        "area_code": "OWID_EU27",
    },
    "Africa": {
        "area_code": "OWID_AFR",
    },
    "Asia": {
        "area_code": "OWID_ASI",
    },
    "Oceania": {
        "area_code": "OWID_OCE",
    },
    "Low-income countries": {
        "area_code": "OWID_LIC",
    },
    "Upper-middle-income countries": {
        "area_code": "OWID_UMC",
    },
    "Lower-middle-income countries": {
        "area_code": "OWID_LMC",
    },
    "High-income countries": {
        "area_code": "OWID_HIC",
    },
}

# When creating region aggregates, decide how to distribute historical regions.
# The following decisions are based on the current location of the countries that succeeded the region, and their income
# group. Continent and income group assigned corresponds to the continent and income group of the majority of the
# population in the member countries.
HISTORIC_TO_CURRENT_REGION = {
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
}


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


def load_income_groups() -> pd.DataFrame:
    """Load dataset of income groups and add historical regions to it.

    Returns
    -------
    income_groups : pd.DataFrame
        Income groups data.

    """
    income_groups = (
        catalog.find(
            table="wb_income_group",
            dataset="wb_income",
            namespace="wb",
            channels=["garden"],
        )
        .load()
        .reset_index()
    )
    # Add historical regions to income groups.
    for historic_region in HISTORIC_TO_CURRENT_REGION:
        historic_region_income_group = HISTORIC_TO_CURRENT_REGION[historic_region][
            "income_group"
        ]
        if historic_region not in income_groups["country"]:
            historic_region_df = pd.DataFrame(
                {
                    "country": [historic_region],
                    "income_group": [historic_region_income_group],
                }
            )
            income_groups = pd.concat(
                [income_groups, historic_region_df], ignore_index=True
            )

    return cast(pd.DataFrame, income_groups)


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


def get_bp_data(bp_table: catalog.Table) -> pd.DataFrame:
    """Extract a simple dataframe of BP statistical review data from the table in the dataset.

    Parameters
    ----------
    bp_table : catalog.Table
        BP table (from the dataset of BP statistical review).

    Returns
    -------
    bp_data : pd.DataFrame
        BP statistical review data.

    """
    bp_table = bp_table.copy()

    # Convert table (snake case) column names to human readable names.
    bp_table = bp_table.rename(
        columns={column: bp_table[column].metadata.title for column in bp_table.columns}
    ).reset_index()

    # Rename human-readable columns (and select only the ones that will be used).
    columns = {
        "entity_name": "Country",
        "entity_code": "Country code",
        "year": "Year",
        # Fossil fuel primary energy (in EJ).
        "Coal Consumption - EJ": "Coal (EJ)",
        "Gas Consumption - EJ": "Gas (EJ)",
        "Oil Consumption - EJ": "Oil (EJ)",
        # Non-fossil based electricity generation (in TWh).
        "Hydro Generation - TWh": "Hydro (TWh - direct)",
        "Nuclear Generation - TWh": "Nuclear (TWh - direct)",
        "Solar Generation - TWh": "Solar (TWh - direct)",
        "Wind Generation - TWh": "Wind (TWh - direct)",
        "Geo Biomass Other - TWh": "Other renewables (TWh - direct)",
        # Non-fossil based electricity generation converted into input-equivalent primary energy (in EJ).
        "Hydro Consumption - EJ": "Hydro (EJ - equivalent)",
        "Nuclear Consumption - EJ": "Nuclear (EJ - equivalent)",
        "Solar Consumption - EJ": "Solar (EJ - equivalent)",
        "Wind Consumption - EJ": "Wind (EJ - equivalent)",
        "Geo Biomass Other - EJ": "Other renewables (EJ - equivalent)",
        # Total, input-equivalent primary energy consumption (in EJ).
        "Primary Energy Consumption - EJ": "Primary energy (EJ - equivalent)",
        # Biofuels consumption (in PJ, that will be converted into EJ).
        "Biofuels Consumption - PJ - Total": "Biofuels (PJ)",
    }

    # Create a simple dataframe (without metadata and with a dummy index).
    assert set(columns) < set(bp_table.columns), "Column names have changed in BP data."
    bp_data = (
        pd.DataFrame(bp_table)[list(columns)]
        .rename(errors="raise", columns=columns)
        .astype({"Country code": str})
    )

    return bp_data


def _check_that_substitution_method_is_well_calculated(
    primary_energy: pd.DataFrame,
) -> None:
    # Check that the constructed primary energy using the substitution method (in TWh) coincides with the
    # input-equivalent primary energy (converted from EJ into TWh) given in the original data.
    check = primary_energy[
        [
            "Year",
            "Country",
            "Primary energy (EJ - equivalent)",
            "Primary energy (TWh - equivalent)",
        ]
    ].reset_index(drop=True)
    check["Primary energy (TWh - equivalent) - original"] = (
        check["Primary energy (EJ - equivalent)"] * EJ_TO_TWH
    )
    check = check.dropna().reset_index(drop=True)
    # They may not coincide exactly, but at least check that they differ (point by point) by less than 10%.
    max_deviation = max(
        abs(
            (
                check["Primary energy (TWh - equivalent)"]
                - check["Primary energy (TWh - equivalent) - original"]
            )
            / check["Primary energy (TWh - equivalent) - original"]
        )
    )
    assert max_deviation < 0.1


def calculate_direct_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Convert direct primary energy into TWh and create various aggregates (e.g. Fossil fuels and Renewables).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        Data, after adding direct primary energy.

    """
    primary_energy = primary_energy.copy()

    # Convert units of biofuels consumption.
    primary_energy["Biofuels (EJ)"] = primary_energy["Biofuels (PJ)"] * PJ_TO_EJ

    # Create column for fossil fuels primary energy (if any of them is nan, the sum will be nan).
    primary_energy["Fossil fuels (EJ)"] = (
        primary_energy["Coal (EJ)"]
        + primary_energy["Oil (EJ)"]
        + primary_energy["Gas (EJ)"]
    )

    # Convert primary energy of fossil fuels and biofuels into TWh.
    for cat in ["Coal", "Oil", "Gas", "Biofuels"]:
        primary_energy[f"{cat} (TWh)"] = primary_energy[f"{cat} (EJ)"] * EJ_TO_TWH

    # Create column for primary energy from fossil fuels (in TWh).
    primary_energy["Fossil fuels (TWh)"] = (
        primary_energy["Coal (TWh)"]
        + primary_energy["Oil (TWh)"]
        + primary_energy["Gas (TWh)"]
    )

    # Create column for direct primary energy from renewable sources in TWh.
    # (total renewable electricity generation and biofuels) (in TWh).
    # By visually inspecting the original data, it seems that biofuels is missing zeros in the data, so we do fillna(0).
    primary_energy["Renewables (TWh - direct)"] = (
        primary_energy["Hydro (TWh - direct)"]
        + primary_energy["Solar (TWh - direct)"]
        + primary_energy["Wind (TWh - direct)"]
        + primary_energy["Other renewables (TWh - direct)"]
        + primary_energy["Biofuels (TWh)"].fillna(0)
    )
    # Create column for direct primary energy from low-carbon sources in TWh.
    # (total renewable electricity generation, biofuels, and nuclear power) (in TWh).
    primary_energy["Low-carbon energy (TWh - direct)"] = (
        primary_energy["Renewables (TWh - direct)"]
        + primary_energy["Nuclear (TWh - direct)"]
    )
    # Create column for total direct primary energy.
    primary_energy["Primary energy (TWh - direct)"] = (
        primary_energy["Fossil fuels (TWh)"]
        + primary_energy["Low-carbon energy (TWh - direct)"]
    )

    return primary_energy


def calculate_equivalent_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Convert input-equivalent primary energy into TWh and create various aggregates (e.g. Fossil fuels and
    Renewables).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        Data, after adding input-equivalent primary energy.

    """
    primary_energy = primary_energy.copy()
    # Create column for total renewable input-equivalent primary energy (in EJ).
    # Fill missing values with zeros (see comment above).
    primary_energy["Renewables (EJ - equivalent)"] = (
        primary_energy["Hydro (EJ - equivalent)"]
        + primary_energy["Solar (EJ - equivalent)"]
        + primary_energy["Wind (EJ - equivalent)"]
        + primary_energy["Other renewables (EJ - equivalent)"]
        + primary_energy["Biofuels (EJ)"].fillna(0)
    )
    # Create column for low carbon energy (i.e. renewable plus nuclear energy).
    primary_energy["Low-carbon energy (EJ - equivalent)"] = (
        primary_energy["Renewables (EJ - equivalent)"]
        + primary_energy["Nuclear (EJ - equivalent)"]
    )
    # Convert input-equivalent primary energy of non-fossil based electricity into TWh.
    # The result is primary energy using the "substitution method".
    for cat in DIRECT_AND_EQUIVALENT_ENERGY:
        primary_energy[f"{cat} (TWh - equivalent)"] = (
            primary_energy[f"{cat} (EJ - equivalent)"] * EJ_TO_TWH
        )
    # Create column for primary energy from all sources (which corresponds to input-equivalent primary
    # energy for non-fossil based sources).
    primary_energy["Primary energy (TWh - equivalent)"] = (
        primary_energy["Fossil fuels (TWh)"]
        + primary_energy["Low-carbon energy (TWh - equivalent)"]
    )
    # Check that the primary energy constructed using the substitution method coincides with the
    # input-equivalent primary energy.
    _check_that_substitution_method_is_well_calculated(primary_energy)

    return primary_energy


def calculate_share_of_primary_energy(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Calculate the share (percentage) of (direct or direct and input-equivalent) primary energy for each energy
     source.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding columns for the share of primary energy.

    """
    primary_energy = primary_energy.copy()
    # Check that all sources are included in the data.
    expected_sources = sorted(
        set(
            [
                source.split("(")[0].strip()
                for source in primary_energy.columns
                if not source.startswith(("Country", "Year", "Primary"))
            ]
        )
    )
    assert expected_sources == ALL_SOURCES, "Sources may have changed names."

    for source in ONLY_DIRECT_ENERGY:
        # Calculate each source as share of direct primary energy.
        primary_energy[f"{source} (% direct primary energy)"] = (
            primary_energy[f"{source} (TWh)"]
            / primary_energy["Primary energy (TWh - direct)"]
            * 100
        )
        # Calculate each source as share of input-equivalent primary energy (i.e. substitution method).
        primary_energy[f"{source} (% equivalent primary energy)"] = (
            primary_energy[f"{source} (EJ)"]
            / primary_energy["Primary energy (EJ - equivalent)"]
            * 100
        )

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Calculate each source as share of direct primary energy.
        primary_energy[f"{source} (% direct primary energy)"] = (
            primary_energy[f"{source} (TWh - direct)"]
            / primary_energy["Primary energy (TWh - direct)"]
            * 100
        )
        # Calculate each source as share of input-equivalent primary energy (i.e. substitution method).
        primary_energy[f"{source} (% equivalent primary energy)"] = (
            primary_energy[f"{source} (EJ - equivalent)"]
            / primary_energy["Primary energy (EJ - equivalent)"]
            * 100
        )

    return primary_energy


def calculate_primary_energy_annual_change(
    primary_energy: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate annual change of (direct or direct and input-equivalent) primary energy for each energy source.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding annual changes.

    """
    primary_energy = primary_energy.copy()

    # Calculate annual change in each source.
    primary_energy = primary_energy.sort_values(["Country", "Year"]).reset_index(
        drop=True
    )
    for source in ONLY_DIRECT_ENERGY:
        # Create column for source percentage growth as a function of direct primary energy.
        primary_energy[f"{source} (% growth)"] = (
            primary_energy.groupby("Country")[f"{source} (TWh)"].pct_change() * 100
        )
        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth)"] = primary_energy.groupby("Country")[
            f"{source} (TWh)"
        ].diff()

    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        # Create column for source percentage growth as a function of primary energy
        # (as a percentage, it is irrelevant whether it is direct or equivalent).
        primary_energy[f"{source} (% growth)"] = (
            primary_energy.groupby("Country")[f"{source} (TWh - direct)"].pct_change()
            * 100
        )
        # Create column for source absolute growth as a function of direct primary energy.
        primary_energy[f"{source} (TWh growth - direct)"] = primary_energy.groupby(
            "Country"
        )[f"{source} (TWh - direct)"].diff()
        # Create column for source absolute growth as a function of input-equivalent primary energy.
        primary_energy[f"{source} (TWh growth - equivalent)"] = primary_energy.groupby(
            "Country"
        )[f"{source} (TWh - equivalent)"].diff()

    return primary_energy


def add_region_aggregates(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Add region aggregate for all regions.

    Regions are defined above, in REGIONS_TO_ADD.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding regions.

    """
    primary_energy = primary_energy.copy()

    income_groups = load_income_groups()
    aggregates = {
        column: "sum"
        for column in primary_energy.columns
        if column not in ["Country", "Year", "Country code"]
    }

    for region in REGIONS_TO_ADD:
        countries_in_region = geo.list_countries_in_region(
            region=region, income_groups=income_groups
        )
        # We do not impose a list of countries that must have data, because, for example, prior to 1985, there is
        # no data for Russia, and therefore we would have no data for Europe.
        # Also, for a similar reason, the fraction of nans allowed in the data is increased to avoid losing all
        # data in Europe prior to 1985.
        primary_energy = geo.add_region_aggregates(
            df=primary_energy,
            region=region,
            country_col="Country",
            year_col="Year",
            aggregations=aggregates,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=0.3,
        )
        # Add country code for current region.
        primary_energy.loc[
            primary_energy["Country"] == region, "Country code"
        ] = REGIONS_TO_ADD[region]["area_code"]

    return primary_energy


def add_per_capita_variables(primary_energy: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita variables.

    Parameters
    ----------
    primary_energy : pd.DataFrame
        BP data.

    Returns
    -------
    primary_energy : pd.DataFrame
        BP data after adding per-capita variables.

    """
    primary_energy = primary_energy.copy()

    primary_energy = add_population(
        df=primary_energy,
        country_col="Country",
        year_col="Year",
        population_col="Population",
        warn_on_missing_countries=False,
    )
    for source in ONLY_DIRECT_ENERGY:
        primary_energy[f"{source} per capita (kWh)"] = (
            primary_energy[f"{source} (TWh)"]
            / primary_energy["Population"]
            * TWH_TO_KWH
        )
    for source in DIRECT_AND_EQUIVALENT_ENERGY:
        primary_energy[f"{source} per capita (kWh - direct)"] = (
            primary_energy[f"{source} (TWh - direct)"]
            / primary_energy["Population"]
            * TWH_TO_KWH
        )
        primary_energy[f"{source} per capita (kWh - equivalent)"] = (
            primary_energy[f"{source} (TWh - equivalent)"]
            / primary_energy["Population"]
            * TWH_TO_KWH
        )

    # Drop unnecessary column.
    primary_energy = primary_energy.drop(columns=["Population"])

    return primary_energy


def prepare_output_table(primary_energy: pd.DataFrame) -> catalog.Table:
    """Create a table with the processed data, ready to be in a garden dataset and to be uploaded to grapher (although
    additional metadata may need to be added to the table).

    Parameters
    ----------
    primary_energy : pd.DataFrame
        Processed BP data.

    Returns
    -------
    table : catalog.Table
        Table, ready to be added to a new garden dataset.

    """
    # Keep only columns in TWh (and not EJ or PJ).
    table = catalog.Table(primary_energy).drop(
        errors="raise",
        columns=[
            column
            for column in primary_energy.columns
            if (("(EJ" in column) or ("(PJ" in column))
        ],
    )

    # Replace spurious inf values by nan.
    table = table.replace([np.inf, -np.inf], np.nan)

    # Sort conveniently and add an index.
    table = (
        table.sort_values(["Country", "Year"])
        .reset_index(drop=True)
        .set_index(["Country", "Year"], verify_integrity=True)
        .astype({"Country code": "category"})
    )

    # Add metadata (e.g. unit) to each column.
    # Define unit names (these are the long and short unit names that will be shown in grapher).
    # The keys of the dictionary should correspond to units expected to be found in each of the variable names in table.
    short_unit_to_unit = {
        "TWh": "terawatt-hours",
        "kWh": "kilowatt-hours",
        "%": "%",
    }
    # Define number of decimal places to show (only relevant for grapher, not for the data).
    short_unit_to_num_decimals = {
        "TWh": 0,
        "kWh": 0,
    }
    for column in table.columns:
        table[column].metadata.title = column
        for short_unit in ["TWh", "kWh", "%"]:
            if short_unit in column:
                table[column].metadata.short_unit = short_unit
                table[column].metadata.unit = short_unit_to_unit[short_unit]
                table[column].metadata.display = {}
                if short_unit in short_unit_to_num_decimals:
                    table[column].metadata.display[
                        "numDecimalPlaces"
                    ] = short_unit_to_num_decimals[short_unit]
                # Add the variable name without unit (only relevant for grapher).
                table[column].metadata.display["name"] = column.split(" (")[0]

    table = catalog.utils.underscore_table(table)

    return table


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load table from latest BP dataset.
    bp_table = catalog.find_one(
        DATASET_CATALOG_NAME,
        channels=["backport"],
        namespace=f"{NAMESPACE_IN_CATALOG}@{VERSION}",
    )

    #
    # Process data.
    #
    # Get a dataframe out of the BP table.
    primary_energy = get_bp_data(bp_table=bp_table)

    # Add region aggregates.
    primary_energy = add_region_aggregates(primary_energy=primary_energy)

    # Calculate direct and primary energy using the substitution method.
    primary_energy = calculate_direct_primary_energy(primary_energy=primary_energy)
    primary_energy = calculate_equivalent_primary_energy(primary_energy=primary_energy)

    # Add region aggregates.
    primary_energy = add_region_aggregates(primary_energy=primary_energy)

    # Calculate share of (direct and sub-method) primary energy.
    primary_energy = calculate_share_of_primary_energy(primary_energy=primary_energy)

    # Calculate annual change of primary energy.
    primary_energy = calculate_primary_energy_annual_change(primary_energy)

    # Add per-capita variables.
    primary_energy = add_per_capita_variables(primary_energy=primary_energy)

    # Prepare output data in a convenient way.
    table = prepare_output_table(primary_energy)

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add metadata to dataset.
    dataset.metadata.update_from_yaml(METADATA_FILE_PATH)
    # Create new dataset in garden.
    dataset.save()

    # Add table to the dataset.
    table.metadata.title = dataset.metadata.title
    table.metadata.description = dataset.metadata.description
    table.metadata.dataset = dataset.metadata
    table.metadata.short_name = dataset.metadata.short_name
    table.metadata.primary_key = list(table.index.names)
    dataset.add(table, repack=True)
