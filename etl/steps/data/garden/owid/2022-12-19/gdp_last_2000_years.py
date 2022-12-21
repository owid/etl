"""
This code combines Maddison and WDI GDP and GDP per capita data, using the WDI values from 1990 onwards and using
GDP growth from Maddison data.

"""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

VERSION_MADDISON = "2020-10-01"
VERSION_WDI = "2022-05-26"

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("gdp_last_2000_years.start")

    # Load Maddison garden step
    ds = Dataset(DATA_DIR / f"garden/ggdc/{VERSION_MADDISON}/ggdc_maddison")
    df_maddison = ds["maddison_gdp"]
    df_maddison = pd.DataFrame(df_maddison)
    df_maddison = df_maddison.reset_index()
    df_maddison = df_maddison.sort_values(by=["year"]).reset_index(drop=True)

    # Load WDI garden step
    ds = Dataset(DATA_DIR / f"garden/worldbank_wdi/{VERSION_WDI}/wdi")
    df_wdi = ds["wdi"]
    df_wdi = pd.DataFrame(df_wdi)
    df_wdi = df_wdi.reset_index()
    df_wdi = df_wdi.sort_values(by=["year"]).reset_index(drop=True)

    # Select GDP and GDP pc in international-$ in 2017 prices
    df_wdi = df_wdi[["country", "year", "ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]]
    df_wdi = df_wdi.rename(columns={"ny_gdp_mktp_pp_kd": "gdp", "ny_gdp_pcap_pp_kd": "gdp_per_capita"})
    # Filter "World" entity
    df_wdi = df_wdi[df_wdi["country"] == "World"]
    # Drop empty World GDP estimations
    df_wdi = df_wdi.dropna().reset_index(drop=True)

    # Select variables to evaluate and reference year to compare Maddison world estimations to World Bank WDI (1990)
    var_list = ["gdp", "gdp_per_capita"]
    year_ref = 1990

    # Select only "World" entity in Maddison dataframe
    df_world = df_maddison[df_maddison["country"] == "World"].copy().reset_index(drop=True)
    df_world = df_world[df_world["year"] <= year_ref].reset_index(drop=True)

    # Estimate growth for GDP and GDP per capita in Maddison
    for var in var_list:
        df_world[f"{var}_growth"] = df_world[var].pct_change() + 1

    # Drop population, as it's not needed
    df_world = df_world.drop(columns=["population"])

    # Merge both Maddison world estimation and WDI world estimation and sort years in descending order to estimate GDP by growth retroactively
    df = pd.merge(df_world, df_wdi, on="year", how="left", suffixes=("_maddworld", "_wdi"))
    df = df.sort_values(by=["year"], ascending=False).reset_index(drop=True)

    for var in var_list:
        # Estimate cumulative growth to multiply it by the 1990 WDI value
        df[f"{var}_growth_cum"] = df[f"{var}_growth"].cumprod()
        for i in range(len(df)):
            # The estimated value is the 1990 WDI value divided by the cumulative growth
            df.loc[i, f"{var}_estimated"] = df[f"{var}_wdi"][0] / df[f"{var}_growth_cum"][i]

        df[f"{var}_estimated"] = df[f"{var}_estimated"].shift(1)
        # Create a new column
        df[var] = np.where(df[f"{var}_wdi"].isnull(), df[f"{var}_estimated"], df[f"{var}_wdi"])

    # Specify "World" entity for each row
    df["country"] = "World"

    # Select only country, year and the new gdp and gdp_per_capita columns, filtering the year 1990
    df = df[["country", "year"] + var_list + ["gdp_maddworld", "gdp_per_capita_maddworld"]]

    # Re-sorting the years ascendingly
    df = df.sort_values(by=["year"]).reset_index(drop=True)

    # Concatenate WDI results with estimation
    df = pd.concat([df, df_wdi[df_wdi["year"] > year_ref]], ignore_index=True)

    # List regions to exclude to generate Maddison country dataset
    regions_to_exclude = [
        "East Asia",
        "South and South-East Asia",
        "Eastern Europe",
        "Latin America",
        "Middle East",
        "Sub-Sahara Africa",
        "Western Europe",
        "Western Offshoots",
        "World",
    ]

    # Specify reference year from estimations
    year_ref = 1820

    # Filter only country data
    df_countries = df_maddison[~df_maddison["country"].isin(regions_to_exclude)].reset_index(drop=True)

    # Sum both GDP and population by year and calculate a GDP per capita estimate
    df_countries = df_countries.groupby(by=["year"])[["gdp", "population"]].sum().reset_index()
    df_countries["gdp_per_capita"] = df_countries["gdp"] / df_countries["population"]

    # Drop null values (mostly when population = 0)
    df_countries = df_countries.dropna().reset_index(drop=True)

    # Keep the data until 1820 only and drop population column
    df_countries = df_countries[df_countries["year"] <= year_ref].reset_index(drop=True)
    df_countries = df_countries.drop(columns=["population"])

    df = pd.merge(df, df_countries, on="year", how="outer", suffixes=(None, "_maddcountries"), sort=True)

    for var in var_list:
        value_ref_countries = df.loc[df["year"] == year_ref, f"{var}_maddworld"].iloc[0]
        df[f"{var}_scalar"] = value_ref_countries / df[f"{var}_maddcountries"]

        value_ref = df.loc[df["year"] == year_ref, f"{var}"].iloc[0]
        df[f"{var}_estimated"] = value_ref / df[f"{var}_scalar"]
        df[f"{var}"] = np.where(df[f"{var}"].isnull(), df[f"{var}_estimated"], df[f"{var}"])
    df["country"] = "World"
    df = df[
        ["country", "year"]
        + var_list
        + ["gdp_maddcountries", "gdp_per_capita_maddcountries"]
        + ["gdp_maddworld", "gdp_per_capita_maddworld"]
    ]

    df = pd.merge(df, df_wdi[["year"] + var_list], on="year", how="left", suffixes=(None, "_wdi"))

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir)

    tb_garden = Table(df)

    # update metadata from yaml file
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "gdp_last_2000_years")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("gdp_last_2000_years.end")
