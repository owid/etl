import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("penn_world_table.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/ggdc/2022-11-28/penn_world_table")
    tb_meadow = ds_meadow["penn_world_table"]

    df = pd.DataFrame(tb_meadow)

    # %% [markdown]
    # ## Adjusting units
    # A range of variables are provided in millions. Here we multiply by 1,000,000 to express
    # these in individual units.
    # %%
    # Multiplying by 1 million to get $ instead of millions of $

    df[["rgdpe", "rgdpo", "cgdpe", "cgdpo", "rgdpna", "ccon", "cda", "cn", "rconna", "rdana", "rnna"]] *= 1000000

    # Multiplying by 1 million to get "people" instead of "millions of people"
    df[["pop", "emp"]] *= 1000000

    # %% [markdown]
    # A range of variables are provided as shares (0-1), which we multiply by 100 to express as a percentage.

    # %%
    df[["labsh", "irr", "delta", "csh_c", "csh_i", "csh_g", "csh_x", "csh_m", "csh_r"]] *= 100

    # %% [markdown]
    # ## GDP per capita variables
    # Penn World Table do not directly provide GDP per capita. We calculate
    # these by dividing GDP by the population figures they provide (both now multiplied
    # by 1,000,000).
    # %%
    cols_per_capita = ["rgdpe", "rgdpo", "cgdpe", "cgdpo", "rgdpna"]
    for col in cols_per_capita:
        df[f"{col}_pc"] = df[col] / df["pop"]

    # %% [markdown]
    # ## Labour productivity
    # We derive a measure of productivity – defined as output per hour worked.
    #
    # For this we use GDP measured in terms of output and using multiple price benchmarks
    # (see [this notebook](https://github.com/owid/notebooks/blob/main/BetterDataDocs/PabloArriagada/pwt/notebooks/analysis_notebooks/aux_compare_gdp.py) for a discussion of the different GDP variables available in Penn World Table).
    #
    # We divide this GDP variable by the total hours worked – calculated by multiplying the number of
    # workers by the annual number of hours of work per worker.

    # %%
    # Productivity = (rgdpo) / (avh*emp) – NB, both rgdpo and emp have been multiplied by 1,000,000 above.
    df["productivity"] = df["rgdpo"] / (df["avh"] * df["emp"])

    # Harmonize countries from main dataset before merge with national accounts data
    df = harmonize_countries(df)

    # %% [markdown]
    # ## Trade openness
    #
    # We define trade openness as the share of imports and exports in GDP. The estimation of this variable requires the use of the National Accounts dataset from PWT (see [this notebook](https://htmlpreview.github.io/?https://github.com/owid/notebooks/blob/main/BetterDataDocs/PabloArriagada/pwt/notebooks/analysis_notebooks/compare_trade_shares/compare_trade_shares.html) for more details about the methodology)
    # %%
    # The National Accounts dataset is loaded here:

    # read dataset from meadow
    ds_meadow_na = Dataset(DATA_DIR / "meadow/ggdc/2022-11-28/penn_world_table_national_accounts")
    tb_meadow_na = ds_meadow_na["penn_world_table_national_accounts"]

    df_na = pd.DataFrame(tb_meadow_na)

    # Trade openness in individual countries
    df_na["trade_openness"] = (df_na["v_x"] + df_na["v_m"]) / df_na["v_gdp"] * 100

    # The World value for this is just the GDP-weighted average across countries.

    df_na["v_gdp_usd"] = df_na["v_gdp"] / df_na["xr2"]

    # Weighted average (dropping alt China and extinct countries with no data)

    excluded_countries = [
        "China (alternative inflation series)",
        "Czechoslovakia",
        "Netherlands Antilles",
        "USSR",
        "Yugoslavia",
    ]

    # Create a list of countries available only in the national accounts dataset, with missing v_x and v_m data

    world_trade_openness_na = (
        df_na[~df_na["country"].isin(excluded_countries)]
        .dropna(subset=["trade_openness", "v_gdp_usd"], how="all")
        .groupby("year")
        .apply(lambda x: np.average(x["trade_openness"], weights=x["v_gdp_usd"]))
        .reset_index()
    )

    world_trade_openness_na.rename(columns={0: "trade_openness"}, inplace=True)
    world_trade_openness_na["country"] = "World"

    # Cleaning df_na from the excluded countries and countries-years with no trade_openness value
    df_na = df_na[~df_na["country"].isin(excluded_countries)].dropna(subset=["trade_openness"], how="all").reset_index()

    # Concatenate the world data with the rest of entities in the NA dataframe
    df_na = pd.concat([df_na, world_trade_openness_na], ignore_index=True)

    # Merging both df and df_na (only with trade openness) with a outer join, to get all the non matched countries-years:
    df = pd.merge(df, df_na[["country", "year", "trade_openness"]], how="outer", on=["country", "year"], sort=True)
    df = df.drop(columns=["countrycode", "currency_unit"])

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = underscore_table(Table(df))

    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "penn_world_table")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("penn_world_table.end")


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df
