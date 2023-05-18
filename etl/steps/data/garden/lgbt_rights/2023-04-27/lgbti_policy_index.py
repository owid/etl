"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators: Dataset = paths.load_dependency(channel="garden", namespace="demography", short_name="population")
    tb_population = ds_indicators["population"].reset_index(drop=False)

    return tb_population


def add_regional_aggregations(df: pd.DataFrame) -> pd.DataFrame:
    """Add regional aggregations using population and country-region mapping."""

    # Load population data
    tb_population = load_population()

    # Merge df with tb_population to get population
    df = pd.merge(df, tb_population[["country", "year", "population"]], how="left", on=["country", "year"])

    # Define a list of variable to make them binary
    binary_vars = [
        "equal_age",
        "unequal_age",
        "constitution",
        "conversion_therapies",
        "death_penalty",
        "employment_discrim",
        "gender_surgery",
        "hate_crimes",
        "incite_hate",
        "joint_adoption",
        "lgb_military",
        "lgb_military_ban",
        "marriage_equality",
        "marriage_ban",
        "samesex_legal",
        "third_gender",
        "trans_military",
        "civil_unions",
        "gendermarker",
        "propaganda",
    ]

    # Create a new column _yes that is 1 if the variable is 1 and 0 otherwise; and _no that is 1 if the variable is < 1 and 0 otherwise
    # Also create a new column _yes_pop that is the product of _yes and population; and _no_pop that is the product of _no and population
    binary_vars_yes = []
    binary_vars_no = []
    binary_vars_yes_pop = []
    binary_vars_no_pop = []
    for var in binary_vars:
        binary_vars_yes.append(f"{var}_yes")
        binary_vars_no.append(f"{var}_no")
        binary_vars_yes_pop.append(f"{var}_yes_pop")
        binary_vars_no_pop.append(f"{var}_no_pop")

        df[f"{var}_yes"] = df[f"{var}"].apply(lambda x: 1 if x == 1 else 0)
        df[f"{var}_no"] = df[f"{var}"].apply(lambda x: 1 if x < 1 else 0)

        df[f"{var}_yes_pop"] = df[f"{var}_yes"] * df["population"]
        df[f"{var}_no_pop"] = df[f"{var}_no"] * df["population"]

    # Define variables which their aggregation is weighted by population
    pop_vars = ["policy_index", "samesex_illegal"]

    # Multiply variables by population
    pop_vars_weighted = []
    for var in pop_vars:
        pop_vars_weighted.append(f"{var}_weighted")
        df[f"{var}_weighted"] = df[var] * df["population"]

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
    aggregations = dict.fromkeys(
        pop_vars_weighted
        + binary_vars_yes
        + binary_vars_no
        + binary_vars_yes_pop
        + binary_vars_no_pop
        + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        df = geo.add_region_aggregates(
            df, region=region, aggregations=aggregations, countries_that_must_have_data=[], population=df
        )

    # Filter dataset by regions to make additional calculations and drop regions in df
    df_regions = df[df["country"].isin(regions)].reset_index(drop=True)
    df = df[~df["country"].isin(regions)].reset_index(drop=True)

    # Also drop binary_vars_yes and binary_vars_no in df (they are only useful for regions)
    df = df.drop(columns=binary_vars_yes + binary_vars_no + binary_vars_yes_pop + binary_vars_no_pop)

    # Calculate average variables for regions
    for var in pop_vars:
        df_regions[var] = df_regions[f"{var}_weighted"] / df_regions["population"]

    # Concatenate df with df_regions
    df = pd.concat([df, df_regions], ignore_index=True)

    # Drop weighted and population columns
    df = df.drop(columns=["population"] + pop_vars_weighted)

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    return df


def run(dest_dir: str) -> None:
    log.info("lgbti_policy_index.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("lgbti_policy_index")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["lgbti_policy_index"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index(drop=False)

    #
    # Process data.
    #
    # Drop numiso and uncode columns (auxiliary columns from the author's original dataset).
    df = df.drop(columns=["numiso", "uncode"])

    # Harmonize country names.
    log.info("lgbti_policy_index.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Add regional aggregations
    df = add_regional_aggregations(df)

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="lgbti_policy_index")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # For now the variable descriptions are stored as a list of strings, this transforms them into a single string
    tb_garden = ds_garden["lgbti_policy_index"]
    for col in tb_garden.columns:
        if isinstance(tb_garden[col].metadata.description, list):
            tb_garden[col].metadata.description = "\n".join(tb_garden[col].metadata.description)
    ds_garden.add(tb_garden)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("lgbti_policy_index.end")
