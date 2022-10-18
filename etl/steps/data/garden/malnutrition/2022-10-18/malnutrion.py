"""Combine WDI's prevalence of stunting, wasting and underweight in children under five. 
Convert this into absolute numbers using UNWPP population data. 

"""

import numpy as np
import pandas as pd
from owid import catalog
from shared import CURRENT_DIR

from etl.paths import DATA_DIR

# Details for dataset to export.
DATASET_SHORT_NAME = "malnutrition"
DATASET_TITLE = "Number of malnourished children"
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Details for datasets to import.
UNWPP_DATASET_PATH = DATA_DIR / "garden/un/2022-07-11/un_wpp"
WDI_DATASET_PATH = DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi"


def run(dest_dir: str) -> None:
    # Load data.
    #
    # Read all required datasets.
    ds_unwpp = catalog.Dataset(UNWPP_DATASET_PATH)
    ds_wdi = catalog.Dataset(WDI_DATASET_PATH)

    # columns of required indicators - stunting, underweight and wasting prevalence
    cols = ["sh_sta_stnt_me_zs", "sh_sta_maln_zs", "sh_sta_wast_zs"]
    new_cols = ["number_of_stunted_children", "number_of_underweight_children", "number_of_wasted_children"]
    base_cols = ["country", "year"]
    df_wdi = ds_wdi["wdi"][cols].dropna().reset_index()
    df_wdi[cols] = (df_wdi[cols].astype(float).round(2)) / 100

    # get under-five population data
    df_wpp = ds_unwpp["population"].reset_index()
    df_wpp = df_wpp[
        (df_wpp["age"] == "0-4")
        & (df_wpp["sex"] == "all")
        & (df_wpp["variant"] == "estimates")
        & (df_wpp["metric"] == "population")
    ]

    # Join the two datasets

    df_both = df_wdi.merge(df_wpp, left_on=["country", "year"], right_on=["location", "year"], how="inner")
    df_both[new_cols] = df_both[cols].multiply(df_both["value"], axis="index").astype(int).round(0)
    base_cols.extend(new_cols)
    df_num = df_both[base_cols]

    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # Get the rest of the metadata from the yaml file.
    ds_garden.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create dataset.
    ds_garden.save()
