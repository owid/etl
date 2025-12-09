"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicators that are expressed in millions (people or currency units).
INDICATORS_IN_MILLIONS = [
    "rgdpe",
    "rgdpo",
    "cgdpe",
    "cgdpo",
    "rgdpna",
    "ccon",
    "cda",
    "cn",
    "rconna",
    "rdana",
    "rnna",
    "pop",
    "emp",
]

# Define indicators that are expressed as shares (0-1)
INDICATORS_AS_SHARES = [
    "labsh",
    "irr",
    "delta",
    "csh_c",
    "csh_i",
    "csh_g",
    "csh_x",
    "csh_m",
    "csh_r",
]

# Define GDP indicators
GDP_INDICATORS = ["rgdpe", "rgdpo", "cgdpe", "cgdpo", "rgdpna"]

# Define excluded countries for trade openness calculation.
EXCLUDED_COUNTRIES = [
    "China (alternative inflation series)",
    "Czechoslovakia",
    "Netherlands Antilles",
    "USSR",
    "Yugoslavia",
]

# Define data information variables, mostly valuable for processing, but not for presentation
# NOTE: I exclude `i_outlier` from this list, given that I use it and drop it in the processing.
DATA_INFO_VARS = [
    "i_cig",
    "i_xm",
    "i_xr",
    "i_irr",
    "cor_exp",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("penn_world_table")
    ds_meadow_na = paths.load_dataset("penn_world_table_national_accounts")

    # Read table from meadow dataset.
    tb = ds_meadow.read("penn_world_table")
    tb_na = ds_meadow_na.read("penn_world_table_national_accounts")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Multiply indicators that are expressed in millions by 1,000,000.
    tb[INDICATORS_IN_MILLIONS] *= 1_000_000

    # Multiply indicators that are expressed as shares by 100 to express as percentages.
    tb[INDICATORS_AS_SHARES] *= 100

    tb = correct_outliers_in_data(tb=tb)

    tb = calculate_gdp_per_capita_and_productivity(tb=tb)

    tb = calculate_trade_openness(tb=tb, tb_na=tb_na)

    tb = drop_unnecessary_columns(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def correct_outliers_in_data(tb: Table) -> Table:
    """
    This function corrects outliers tagged as such in the data. It also replaces data for Bermuda's rgdpo with cgdpo values.

    From Robert Inklaar:

    The chaining of reference prices may be causing these problems. Normally, cgdpo and rgdpo are not too different, but with the wild swings and even negative prices for Bermuda, that seems an exception.

    Perhaps the most elegant way would be to either using cgdpo for all countries or just for Bermuda.

    --
    Some country/year observations, we label as outliers because indeed the relative price levels become implausible.
    See the i_outlier variable for those observations and in our documentation we have discussion in what qualifies as an outlier.

    """
    # Replace `rgdpo` values of Bermuda with `cgdpo` values, because of issues with the data
    tb.loc[tb["country"] == "Bermuda", "rgdpo"] = tb.loc[tb["country"] == "Bermuda", "cgdpo"]

    # Filter dataframe with i_outlier different to "Outlier"
    tb = tb[tb["i_outlier"] != "Outlier"].reset_index(drop=True)

    # Drop i_outlier column
    tb = tb.drop(columns=["i_outlier"])

    return tb


def calculate_gdp_per_capita_and_productivity(tb: Table) -> Table:
    """
    Calculate GDP per capita and productivity variables.
    """

    # Calculate GDP per capita variables.
    for col in GDP_INDICATORS:
        tb[f"{col}_pc"] = tb[col] / tb["pop"]

    # Calculate productivity as output per hour worked.
    tb["productivity"] = tb["rgdpo"] / (tb["avh"] * tb["emp"])

    return tb


def calculate_trade_openness(tb: Table, tb_na: Table) -> Table:
    """
    Calculate trade openness as the imports and exports as a share of GDP, using the National Accounts dataset.
    """

    # Trade openness in individual countries (doesn't matter to use current national prices)
    tb_na["trade_openness"] = (tb_na["v_x"] + tb_na["v_m"]) / tb_na["v_gdp"] * 100

    # Convert v_gdp to USD using exchange rates
    tb_na["v_gdp_usd"] = tb_na["v_gdp"] / tb_na["xr2"]

    # Keep only relevant columns
    tb_na = tb_na[["country", "year", "trade_openness", "v_gdp_usd"]].reset_index(drop=True)

    # Cleaning tb_na from the excluded countries
    tb_na = tb_na[~tb_na["country"].isin(EXCLUDED_COUNTRIES)].reset_index(drop=True)

    # Drop na values in trade_openness
    tb_na = tb_na.dropna(subset=["trade_openness"]).reset_index(drop=True)

    # Create tb_na_world for aggregated world trade openness
    tb_na_world = tb_na.copy()

    # Calculate the product per year of trade_openness and v_gdp_usd
    tb_na_world["trade_openness_x_v_gdp_usd"] = tb_na_world["trade_openness"] * tb_na_world["v_gdp_usd"]

    # Calculate the sum of trade_openness_x_v_gdp_usd and v_gdp_usd per year
    tb_na_world = (
        tb_na_world.groupby("year")
        .agg(
            trade_openness_x_v_gdp_usd=("trade_openness_x_v_gdp_usd", "sum"),
            v_gdp_usd=("v_gdp_usd", "sum"),
        )
        .reset_index()
    )

    # Calculate the trade openness for the world
    tb_na_world["trade_openness"] = tb_na_world["trade_openness_x_v_gdp_usd"] / tb_na_world["v_gdp_usd"]

    # Add country column with "World"
    tb_na_world["country"] = "World"

    # Remove unnecessary columns
    tb_na_world = tb_na_world[["country", "year", "trade_openness"]].reset_index(drop=True)

    # Concatenate the world data with the rest of entities in the NA dataframe
    tb_na = pr.concat([tb_na, tb_na_world], ignore_index=True)

    # Merging both df and df_na (only with trade openness) with a outer join, to get all the non matched countries-years:
    tb = pr.merge(tb, tb_na[["country", "year", "trade_openness"]], how="outer", on=["country", "year"], sort=True)

    # Drop columns that are not needed in tb
    tb = tb.drop(columns=["countrycode", "currency_unit"])

    return tb


def drop_unnecessary_columns(tb: Table) -> Table:
    """
    Drop unnecessary columns from the table.
    """
    tb = tb.drop(columns=DATA_INFO_VARS)

    return tb
