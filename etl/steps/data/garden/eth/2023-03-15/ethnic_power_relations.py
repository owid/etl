"""Load the GROWup meadow dataset, the countries_regions dataset and the population dataset to create
the Ethnic Power Relations garden dataset with regional aggregations."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# EPR classifies some countries as both their historical and current name. This is not ideal for some cases where their borders changed completely
def separate_historical_countries(df: pd.DataFrame) -> pd.DataFrame:

    df["country"] = df["country"].astype(str)
    # RUSSIA / SOVIET UNION
    # Change the country name for the Soviet Union to Russia for years less or equal to 1991
    df.loc[(df["country"] == "Russia (Soviet Union)") & (df["year"] <= 1991), "country"] = "USSR"
    df.loc[(df["country"] == "Russia (Soviet Union)") & (df["year"] > 1991), "country"] = "Russia"

    # YEMEN
    # Rename "Yemen (Arab Republic of Yemen)" to "Yemen" for years greater than 1990
    df.loc[(df["country"] == "Yemen (Arab Republic of Yemen)") & (df["year"] > 1990), "country"] = "Yemen"

    # GERMANY
    # Rename "German Federal Republic" to "Germany" for years greater than 1990
    df.loc[(df["country"] == "German Federal Republic") & (df["year"] > 1990), "country"] = "Germany"

    return df


# Data processing function for the Ethnic Power-Relations Dataset
def process_ethnic_power_relations(df: pd.DataFrame) -> pd.DataFrame:
    # Select only the variables related to the Ethnic Power-Relations Dataset
    epr_vars = [
        "egip_groups_count",
        "excl_groups_count",
        "regaut_groups_count",
        "regaut_excl_groups_count",
        "regaut_egip_groups_count",
        "rlvt_groups_count",
        "actv_groups_count",
        "lpop",
        "egippop",
        "legippop",
        "exclpop",
        "lexclpop",
        "discrimpop",
        "ldiscrimpop",
        "maxexclpop",
        "lmaxexclpop",
        "regautpop",
        "regautexclpop",
        "regautegippop",
        "cntr_relevance",
        "nstar",
    ]

    acd2epr_vars = [
        "onset_ko_eth_flag",
        "onset_ko_noneth_flag",
        "onset_ko_terr_eth_flag",
        "onset_ko_gov_eth_flag",
        "onset_ko_terr_noneth_flag",
        "onset_ko_gov_noneth_flag",
        "incidence_eth_flag",
        "incidence_noneth_flag",
        "incidence_terr_eth_flag",
        "incidence_gov_eth_flag",
        "incidence_terr_noneth_flag",
        "incidence_gov_noneth_flag",
        "onset_do_eth_flag",
        "onset_do_noneth_flag",
        "onset_do_terr_eth_flag",
        "onset_do_gov_eth_flag",
        "onset_do_terr_noneth_flag",
        "onset_do_gov_noneth_flag",
    ]

    df = df[["country", "year"] + epr_vars + acd2epr_vars]

    # Multiply share variables by 100
    share_vars = [
        "lpop",
        "egippop",
        "legippop",
        "exclpop",
        "lexclpop",
        "discrimpop",
        "ldiscrimpop",
        "maxexclpop",
        "lmaxexclpop",
        "regautpop",
        "regautexclpop",
        "regautegippop",
    ]

    df[share_vars] *= 100

    # Calculate share variable for population not in EGIP or MEG
    df["restpop"] = 100 - df["egippop"] - df["exclpop"]

    return df


def load_population() -> pd.DataFrame:
    # Load population table from key_indicators dataset.
    ds_indicators: Dataset = paths.load_dependency("population")
    tb_population = ds_indicators["population"]
    tb_population = pd.DataFrame(tb_population).reset_index(drop=False)

    return tb_population


# Function to add regional aggregations to data
def add_regional_aggregations(df: pd.DataFrame) -> pd.DataFrame:

    # Load population data
    tb_population = load_population()

    # Merge df with tb_population to get population
    df = pd.merge(df, tb_population[["country", "year", "population"]], how="left", on=["country", "year"])

    # Define variables which their aggregation is weighted by population
    # Do not aggregate: legippop, lexclpop, ldiscrimpop, lmaxexclpop
    pop_vars = [
        "lpop",
        "egippop",
        "exclpop",
        "restpop",
        "discrimpop",
        "maxexclpop",
        "regautpop",
        "regautexclpop",
        "regautegippop",
    ]

    # Define binary variables to create binary aggregations
    binary_vars = [
        "incidence_eth_flag",
        "incidence_noneth_flag",
        "incidence_terr_eth_flag",
        "incidence_gov_eth_flag",
        "incidence_terr_noneth_flag",
        "incidence_gov_noneth_flag",
    ]

    # Multiply variables by population
    pop_vars_headcount = []
    for var in pop_vars:
        pop_vars_headcount.append(f"{var}_headcount")
        df[f"{var}_headcount"] = df[var] * df["population"] / 100

    # Define regions to aggregate
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
        "World",
    ]

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = {
        "egip_groups_count": "sum",
        "excl_groups_count": "sum",
        "regaut_groups_count": "sum",
        "regaut_excl_groups_count": "sum",
        "regaut_egip_groups_count": "sum",
        "rlvt_groups_count": "sum",
        "actv_groups_count": "sum",
        "lpop_headcount": "sum",
        "egippop_headcount": "sum",
        "exclpop_headcount": "sum",
        "restpop_headcount": "sum",
        "discrimpop_headcount": "sum",
        "maxexclpop_headcount": "sum",
        "regautpop_headcount": "sum",
        "regautexclpop_headcount": "sum",
        "regautegippop_headcount": "sum",
        "incidence_eth_flag": "sum",
        "incidence_noneth_flag": "sum",
        "incidence_terr_eth_flag": "sum",
        "incidence_gov_eth_flag": "sum",
        "incidence_terr_noneth_flag": "sum",
        "incidence_gov_noneth_flag": "sum",
        "population": "sum",
    }

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        df = geo.add_region_aggregates(
            df, region=region, aggregations=aggregations, countries_that_must_have_data=[], population=df
        )

    # Filter dataset by regions to make additional calculations and drop regions in df
    df_regions = df[df["country"].isin(regions)].reset_index(drop=True)
    df = df[~df["country"].isin(regions)].reset_index(drop=True)

    # Calculate share variables for regions
    for var in pop_vars:
        df_regions[var] = df_regions[f"{var}_headcount"] / df_regions["population"] * 100

    # Calculate variables that have not been aggregated
    df_regions["legippop"] = df_regions["egippop"] / df_regions["lpop"]
    df_regions["lexclpop"] = df_regions["exclpop"] / df_regions["lpop"]
    df_regions["ldiscrimpop"] = df_regions["discrimpop"] / df_regions["lpop"]
    df_regions["lmaxexclpop"] = df_regions["maxexclpop"] / df_regions["lpop"]

    # Calculate binary variables for regions
    # For each variable in binary_vars, replace values greater than 1 with 1
    for var in binary_vars:
        df_regions[var] = df_regions[var] > 0
        df_regions[var] = df_regions[var].astype(int)

    # Concatenate df with df_regions
    df = pd.concat([df, df_regions], ignore_index=True)

    # Drop headcount and population columns
    df = df.drop(columns=["population"] + pop_vars_headcount)

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    return df


def run(dest_dir: str) -> None:
    log.info("ethnic_power_relations.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("growup")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["growup"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #

    # Separate historical countries (Soviet Union, Yemen)
    df = separate_historical_countries(df)

    log.info("ethnic_power_relations.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Select and transform variables
    df = process_ethnic_power_relations(df)

    # Add regional aggregations
    df = add_regional_aggregations(df)

    # Create a new table with the processed data and add a new short_name.
    tb_garden = Table(df)
    tb_garden.metadata.short_name = "ethnic_power_relations"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # For now the variable descriptions are stored as a list of strings, this transforms them into a single string
    tb_garden = ds_garden["ethnic_power_relations"]
    for col in tb_garden.columns:
        if isinstance(tb_garden[col].metadata.description, list):
            tb_garden[col].metadata.description = "\n".join(tb_garden[col].metadata.description)
    ds_garden.add(tb_garden)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ethnic_power_relations.end")
