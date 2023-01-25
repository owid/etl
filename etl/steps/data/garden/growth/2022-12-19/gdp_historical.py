"""
This code combines Maddison Project Database 2020 with Maddison Database 2010 and WDI to get historical and up-to-date GDP and GDP per capita data, using the WDI values from 1990 onwards and estimating
GDP growth from Maddison data to retroactively adjust WDI data. Maddison Database 2010 is obtained for the 1 to 1820 GDP global estimations. These data is dropped in subsequent versions (Maddison Project Database, after Angus Maddison's death).
As we are processing data with already a high degree of uncertainty, the adjustments in GDP prior to 1990 are rounded.

"""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

VERSION_MADDISON = "2020-10-01"
VERSION_WDI = "2022-05-26"
VERSION_MADDISON2010 = "2022-12-23"

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def create_estimations_from_growth(
    df: pd.DataFrame, var_list: list, year_ref: int, input_suffix: str, output_suffix: str
) -> pd.DataFrame:
    for var in var_list:
        # Get reference value from where I get the growth (year_ref)
        value_ref_input = df.loc[df["year"] == year_ref, f"{var}{input_suffix}"].iloc[0]
        # The scalar is this reference value divided by each GDP/GDP pc source value
        df[f"{var}_scalar"] = value_ref_input / df[f"{var}{input_suffix}"]

        # Get reference value for year_ref where the growth will be applied
        value_ref = df.loc[df["year"] == year_ref, f"{var}{output_suffix}"].iloc[0]
        # The estimated values are the division between the reference value and the scalars
        df[f"{var}_estimated"] = value_ref / df[f"{var}_scalar"]

        # Rename the estimated variables to gdp/gdp_per_capita
        df[f"{var}"] = df[f"{var}{output_suffix}"].fillna(df[f"{var}_estimated"])

    # Specify "World" entity for each row
    df["country"] = "World"

    # Keep only new variables
    df = df[["country", "year"] + var_list]

    return df


def run(dest_dir: str) -> None:
    log.info("gdp_historical.start")

    # Load Maddison garden step
    ds = Dataset(DATA_DIR / f"garden/ggdc/{VERSION_MADDISON}/ggdc_maddison")
    df_maddison = ds["maddison_gdp"]
    df_maddison = pd.DataFrame(df_maddison, dtype=np.float64)
    df_maddison = df_maddison.reset_index()
    df_maddison = df_maddison.sort_values(by=["year"]).reset_index(drop=True)

    # Load WDI garden step
    ds = Dataset(DATA_DIR / f"garden/worldbank_wdi/{VERSION_WDI}/wdi")
    df_wdi = ds["wdi"]
    df_wdi = pd.DataFrame(df_wdi, dtype=np.float64)
    df_wdi = df_wdi.reset_index()
    df_wdi = df_wdi.sort_values(by=["year"]).reset_index(drop=True)

    # Select GDP and GDP pc in international-$ in 2017 prices
    df_wdi = df_wdi[["country", "year", "ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]]
    df_wdi = df_wdi.rename(columns={"ny_gdp_mktp_pp_kd": "gdp", "ny_gdp_pcap_pp_kd": "gdp_per_capita"})
    # Filter "World" entity
    df_wdi = df_wdi[df_wdi["country"] == "World"]
    # Drop empty World GDP estimations
    df_wdi = df_wdi.dropna().reset_index(drop=True)

    # Load Maddison Dataset 2010 - garden step. It contains global GDP estimations between 1 and 1820
    ds = Dataset(DATA_DIR / f"garden/ggdc/{VERSION_MADDISON2010}/maddison_database")
    df_maddison2010 = ds["maddison_database"]
    df_maddison2010 = pd.DataFrame(df_maddison2010)

    # Select variables to evaluate and reference year to compare Maddison world estimations to World Bank WDI (1990)
    var_list = ["gdp", "gdp_per_capita"]
    year_ref = 1990

    # Select only "World" entity in Maddison dataframe
    df_world = df_maddison[df_maddison["country"] == "World"].copy().reset_index(drop=True)
    df_world = df_world[df_world["year"] <= year_ref].reset_index(drop=True)

    # Drop population, as it's not needed
    df_world = df_world.drop(columns=["population"])

    # Merge both Maddison world estimation and WDI world estimation
    df = pd.merge(df_world, df_wdi, on="year", how="left", suffixes=("_maddison", "_wdi"))

    # Apply Maddison Project Database growth (1820-1990) retroactively to 1990 WDI data
    df = create_estimations_from_growth(df, var_list, year_ref, input_suffix="_maddison", output_suffix="_wdi")

    # Concatenate WDI results with estimation
    df = pd.concat([df, df_wdi[df_wdi["year"] > year_ref]], ignore_index=True)

    # Specify new reference year for the 1 - 1820 period
    year_ref = 1820

    # Keep the data until 1820 in the old Maddison Dataset 2010
    df_maddison2010 = df_maddison2010[df_maddison2010["year"] <= year_ref].reset_index(drop=True)
    df_maddison2010 = df_maddison2010.drop(columns=["population"])

    # Merge datasets to include Maddison Dataset 2010
    df = pd.merge(df, df_maddison2010, on="year", how="outer", suffixes=(None, "_maddison2010"), sort=True)

    # Apply Maddison Database 2010 growth (1-1820) retroactively to 1820 estimations
    df = create_estimations_from_growth(df, var_list, year_ref, input_suffix="_maddison2010", output_suffix="")

    # Round variables to address uncertainty on old estimations (previous to 1990)
    df["gdp"] = np.where(df["year"] < 1990, df["gdp"].round(-3), df["gdp"])
    df["gdp_per_capita"] = np.where(df["year"] < 1990, df["gdp_per_capita"].round(-1), df["gdp_per_capita"])

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir)

    # tb_garden = Table(df)

    # # update metadata from yaml file
    # ds_garden.metadata.update_from_yaml(N.metadata_path)
    # tb_garden.update_metadata_from_yaml(N.metadata_path, "gdp_historical")

    # ds_garden.add(tb_garden)
    # ds_garden.save()

    tb_garden = Table(df, short_name="gdp_historical")
    ds_garden.add(tb_garden)
    ds_garden.update_metadata(N.metadata_path)
    ds_garden.save()

    log.info("gdp_historical.end")
