"""Load historical data on UK yields and combine it with the latest FAOSTAT data."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"

# Item codes for required items.
ITEM_CODE_FOR_WHEAT = "00000015"
ITEM_CODE_FOR_BARLEY = "00000044"
ITEM_CODE_FOR_OATS = "00000075"
ITEM_CODE_FOR_POTATOES = "00000116"
ITEM_CODE_FOR_PULSES = "00001726"
ITEM_CODE_FOR_RYE = "00000071"
ITEM_CODE_FOR_SUGAR_BEET = "00000157"
ITEM_CODES = [
    ITEM_CODE_FOR_WHEAT,
    ITEM_CODE_FOR_BARLEY,
    ITEM_CODE_FOR_OATS,
    ITEM_CODE_FOR_POTATOES,
    ITEM_CODE_FOR_PULSES,
    ITEM_CODE_FOR_RYE,
    ITEM_CODE_FOR_SUGAR_BEET,
]


def run(dest_dir: str) -> None:
    log.info("uk_long_term_yields.start")

    #
    # Load inputs.
    #
    # Load UK long-term yields data from Broadberry et al. (2015).
    ds_broadberry: Dataset = paths.load_dependency("broadberry_et_al_2015")

    # Read main table from dataset.
    tb_broadberry = ds_broadberry["broadberry_et_al_2015"]

    # Create a convenient dataframe.
    df_broadberry = pd.DataFrame(tb_broadberry).reset_index()

    # Load UK long-term yields data from Brassley (2000).
    ds_brassley: Dataset = paths.load_dependency("brassley_2000")

    # Read main table from dataset.
    tb_brassley = ds_brassley["brassley_2000"]

    # Create a convenient dataframe.
    df_brassley = pd.DataFrame(tb_brassley).reset_index()

    # Load faostat data on crop and livestock production.
    ds_qcl: Dataset = paths.load_dependency("faostat_qcl")

    # Read main table from dataset.
    tb_qcl = ds_qcl["faostat_qcl"]

    # Create a convenient dataframe.
    df_qcl = pd.DataFrame(tb_qcl).reset_index()

    #
    # Process data.
    #
    # Select required country, element and items.
    df_qcl = df_qcl[
        (df_qcl["country"] == "United Kingdom")
        & (df_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD)
        & (df_qcl["item_code"].isin(ITEM_CODES))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units for yield have changed."
    assert list(df_qcl["unit"].unique()) == ["tonnes per hectare"], error

    # Transpose data.
    df_qcl = df_qcl.pivot(index=["country", "year"], columns=["item"], values=["value"])
    df_qcl.columns = [column[1].lower().replace(" ", "_") + "_yield" for column in df_qcl.columns]
    df_qcl = df_qcl.reset_index()

    # Combine historical data.
    df_historical = combine_two_overlapping_dataframes(
        df1=df_broadberry, df2=df_brassley, index_columns=["country", "year"]
    )

    # Combine historical data with faostat data.
    df_combined = combine_two_overlapping_dataframes(df1=df_qcl, df2=df_historical, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    df_combined = df_combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df_combined, short_name=paths.short_name)

    # Add variable metadata.
    for column in tb_garden.columns:
        tb_garden[column].metadata.title = column.capitalize().replace("_", " ")
        tb_garden[column].metadata.unit = "tonnes per hectare"
        tb_garden[column].metadata.short_unit = "tonnes/ha"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Combine sources and licenses.
    ds_garden.metadata.sources = ds_broadberry.metadata.sources + ds_brassley.metadata.sources + ds_qcl.metadata.sources
    ds_garden.metadata.licenses = (
        ds_broadberry.metadata.licenses + ds_brassley.metadata.licenses + ds_qcl.metadata.licenses
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("uk_long_term_yields.end")
