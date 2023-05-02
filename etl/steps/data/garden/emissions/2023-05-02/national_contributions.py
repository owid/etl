"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import map_series
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor to change from teragrams to tonnes.
TERAGRAMS_TO_TONNES = 1e6
# Conversion factor to change from petagrams to tonnes.
PETAGRAMS_TO_TONNES = 1e9

# Conversion factors to change from tonnes of gases emitted to tonnes of CO2 equivalents (taken from IPCC AR6).
CH4_FOSSIL_EMISSIONS_TO_CO2_EQUIVALENTS = 29.8
CH4_LAND_EMISSIONS_TO_CO2_EQUIVALENTS = 27.2
N2O_EMISSIONS_TO_CO2_EQUIVALENTS = 273

# Gases and components expected to be in the data, and how to rename them.
GASES_RENAMING = {
    "3-GHG": "ghg",
    "CH[4]": "ch4",
    "CO[2]": "co2",
    "N[2]*O": "n2o",
}
COMPONENTS_RENAMING = {
    "Fossil": "fossil",
    "LULUCF": "land",
    "Total": "total",
}

# Columns for which we will create "share" variables (percentage with respect to global).
SHARE_VARIABLES = [
    "annual_emissions_ch4_total",
    "annual_emissions_co2_total",
    "annual_emissions_n2o_total",
    "temperature_response_ghg_total",
]

# Columns for which a per-capita variable will be created.
PER_CAPITA_VARIABLES = [
    "annual_emissions_ch4_total_co2eq",
    "annual_emissions_n2o_total_co2eq",
    "annual_emissions_ghg_total_co2eq",
]

# Regions to be added by aggregating data from their member countries.
REGIONS = [
    # Continents.
    "Africa",
    "Asia",
    "Europe",
    "European Union (27)",
    "North America",
    "Oceania",
    "South America",
    # Income groups.
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run_sanity_checks_on_inputs(df):
    # Sanity checks.
    error = "Names of gases have changed."
    assert set(df["gas"]) == set(GASES_RENAMING), error
    error = "Names of components have changed."
    assert set(df["component"]) == set(COMPONENTS_RENAMING), error
    error = "Units have changed."
    assert set(df["unit"]) == set(
        ["Tg~CH[4]~year^-1", "Pg~CO[2]~year^-1", "Tg~N[2]*O~year^-1", "Pg~CO[2]*-e[100]", "°C"]
    ), error


def add_kuwaiti_oil_fires_to_kuwait(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # NOTE: Use this function before harmonizing country names. Otherwise adapt the following definitions.
    kuwait = "Kuwait"
    oil_fires = "Kuwaiti Oil Fires"

    # Sanity check.
    error = f"'{kuwait}' or '{oil_fires}' not found in the data."
    assert kuwait in set(df["country"]), error
    assert oil_fires in set(df["country"]), error

    # Add the emissions from the Kuwaiti oil fires (in 1991) to Kuwait.
    df_kuwait = df[df["country"] == kuwait].drop(columns="country").set_index("year")
    df_oil_fires = df[df["country"] == oil_fires].drop(columns="country").fillna(0).set_index(["year"])
    df_combined = (df_kuwait + df_oil_fires).reset_index().assign(**{"country": kuwait})

    # Replace the origina data for Kuwait by the combined data.
    df_updated = pd.concat([df[df["country"] != kuwait].reset_index(drop=True), df_combined], ignore_index=True)

    # Sort conveniently.
    df_updated = df_updated.sort_values(["country", "year"]).reset_index(drop=True)

    return df_updated


def add_emissions_in_co2_equivalents(df: pd.DataFrame) -> pd.DataFrame:
    # Add columns for fossil/land/total emissions of CH4 in terms of CO2 equivalents.
    df["annual_emissions_ch4_fossil_co2eq"] = (
        df["annual_emissions_ch4_fossil"] * CH4_FOSSIL_EMISSIONS_TO_CO2_EQUIVALENTS
    )
    df["annual_emissions_ch4_land_co2eq"] = df["annual_emissions_ch4_land"] * CH4_LAND_EMISSIONS_TO_CO2_EQUIVALENTS
    df["annual_emissions_ch4_total_co2eq"] = (
        df["annual_emissions_ch4_fossil_co2eq"] + df["annual_emissions_ch4_land_co2eq"]
    )

    # Add columns for fossil/land/total emissions of N2O in terms of CO2 equivalents.
    for component in ["fossil", "land", "total"]:
        df[f"annual_emissions_n2o_{component}_co2eq"] = (
            df[f"annual_emissions_n2o_{component}"] * N2O_EMISSIONS_TO_CO2_EQUIVALENTS
        )

    # Add columns for fossil/land/total emissions of all GHG in terms of CO2 equivalents.
    for component in ["fossil", "land", "total"]:
        df[f"annual_emissions_ghg_{component}_co2eq"] = (
            df[f"annual_emissions_co2_{component}"]
            + df[f"annual_emissions_ch4_{component}_co2eq"]
            + df[f"annual_emissions_n2o_{component}_co2eq"]
        )

    return df


def add_region_aggregates(df: pd.DataFrame, ds_regions: Dataset, ds_income_groups: Dataset) -> pd.DataFrame:
    for region in REGIONS:
        # List members in this region.
        members = geo.list_members_of_region(region, ds_regions=ds_regions, ds_income_groups=ds_income_groups)
        df = geo.add_region_aggregates(
            df=df,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            # Here we allow aggregating even when there are few countries informed.
            # However, if absolutely all countries have nan, we want the aggregate to be nan, not zero.
            frac_allowed_nans_per_year=0.999,
        )

    return df


def add_share_variables(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Create "share" variables (percentages with respect to global).
    # To do that, first create a separate dataframe for global data, and add it to the main dataframe.
    df_global = df[df["country"] == "World"][["year"] + SHARE_VARIABLES].reset_index(drop=True)

    df = pd.merge(df, df_global, on=["year"], how="left", suffixes=("", "_global"))
    # For a list of variables, add the percentage with respect to global.
    for variable in SHARE_VARIABLES:
        df[f"share_of_{variable}"] = 100 * df[variable] / df[f"{variable}_global"]

    # Drop unnecessary columns for global data.
    df = df.drop(columns=[column for column in df.columns if column.endswith("_global")])

    return df


def add_per_capita_variables(df: pd.DataFrame, ds_population: Dataset) -> pd.DataFrame:
    df = df.copy()

    # Add population to data.
    df = geo.add_population_to_dataframe(
        df=df,
        ds_population=ds_population,
        warn_on_missing_countries=False,
    )

    # Add per-capita variables.
    for variable in PER_CAPITA_VARIABLES:
        df[f"{variable}_per_capita"] = df[variable] / df["population"]

    # Drop population column.
    df = df.drop(columns="population")

    return df


def run(dest_dir: str) -> None:
    log.info("national_contributions.start")

    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("national_contributions")
    tb_meadow = ds_meadow["national_contributions"]

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("wb_income")

    # Load population dataset.
    ds_population = paths.load_dependency("population")

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    # Sanity checks.
    run_sanity_checks_on_inputs(df=df)

    # Rename gases and components.
    df["gas"] = map_series(
        series=df["gas"], mapping=GASES_RENAMING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    df["component"] = map_series(
        series=df["component"], mapping=COMPONENTS_RENAMING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Convert units from teragrams and petagrams to tonnes.
    df.loc[df["unit"].str.startswith("Tg"), "data"] *= TERAGRAMS_TO_TONNES
    df.loc[df["unit"].str.startswith("Pg"), "data"] *= PETAGRAMS_TO_TONNES

    # Transpose data.
    df = df.pivot(index=["country", "year"], columns=["file", "gas", "component"], values="data")
    df.columns = ["_".join(column) for column in df.columns]
    df = df.reset_index()

    # We add the emissions from the Kuwaiti oil fires in 1991 (which are also included as a separate country) as part
    # of the emissions of Kuwait.
    # This ensures that these emissions will be included in aggregates of regions that include Kuwait.
    df = add_kuwaiti_oil_fires_to_kuwait(df=df)

    # Harmonize country names.
    df = geo.harmonize_countries(
        df,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Add region aggregates.
    df = add_region_aggregates(df=df, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Add columns for emissions in terms of CO2 equivalents.
    df = add_emissions_in_co2_equivalents(df=df)

    # Add "share" variables (percentages with respect to global emissions).
    df = add_share_variables(df=df)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df, ds_population=ds_population)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name, underscore=True)

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("national_contributions.end")
