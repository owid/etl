"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Item code for "Wheat".
ITEM_CODE_FOR_WHEAT = "00000015"

# Element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"


def run(dest_dir: str) -> None:
    log.info("long_term_wheat_yields.start")

    #
    # Load inputs.
    #
    # Load long-term wheat yield data from Bayliss-Smith & Wanmali (1984).
    ds_bayliss: Dataset = paths.load_dependency("bayliss_smith_wanmali_1984")

    # Read main table from dataset.
    tb_bayliss = ds_bayliss["long_term_wheat_yields"]

    # Create a convenient dataframe.
    df_bayliss = pd.DataFrame(tb_bayliss).reset_index()

    # Load faostat data on crops and livestock products.
    ds_qcl: Dataset = paths.load_dependency("faostat_qcl")

    # Read main table from dataset.
    tb_qcl = ds_qcl["faostat_qcl"]

    # Create a convenient dataframe.
    df_qcl = pd.DataFrame(tb_qcl).reset_index()

    #
    # Process data.
    #
    # Select the relevant item and element from faostat data.
    df_qcl = df_qcl[
        (df_qcl["item_code"] == ITEM_CODE_FOR_WHEAT) & (df_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD)
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units of yield have changed."
    assert list(df_qcl["unit"].unique()) == ["tonnes per hectare"], error

    # Prepare variable description.
    element_description = df_qcl["element_description"].drop_duplicates().item()

    # Transpose data.
    df_qcl = (
        df_qcl.pivot(index=["country", "year"], columns="item", values="value")
        .reset_index()
        .rename(columns={"Wheat": "wheat_yield"}, errors="raise")
    )

    # Combine Bayliss and faostat data.
    combined = combine_two_overlapping_dataframes(df1=df_qcl, df2=df_bayliss, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(combined, short_name="long_term_wheat_yields")

    # Add variable metadata, using faostat unit and element description (there was no item description).
    tb_garden["wheat_yield"].metadata.title = "Wheat yields"
    tb_garden["wheat_yield"].metadata.description = f"Long-term wheat yields.\n{element_description}"
    tb_garden["wheat_yield"].metadata.unit = "tonnes per hectare"
    tb_garden["wheat_yield"].metadata.short_unit = "tonnes/ha"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Combine sources and licenses.
    ds_garden.metadata.sources = ds_bayliss.metadata.sources + ds_qcl.metadata.sources
    ds_garden.metadata.licenses = ds_bayliss.metadata.licenses + ds_qcl.metadata.licenses

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("long_term_wheat_yields.end")
