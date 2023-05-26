"""Combine attainable yields from Mueller et al. (2012) with the latest FAOSTAT yields data."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Element code for "Yield".
ELEMENT_CODE_FOR_YIELD = "005419"

ITEM_CODES = {
    # Item code for "Barley".
    "barley": "00000044",
    # Item code for "Cassava, fresh"
    "cassava": "00000125",
    # Item code for "Seed cotton, unginned".
    "cotton": "00000328",
    # Item code for "Groundnuts, excluding shelled".
    "groundnut": "00000234",
    # Item code for "Maize (corn)".
    "maize": "00000056",
    # Item code for "Millet".
    "millet": "00000079",
    # Item code for "Oil palm fruit".
    "oilpalm": "00000254",
    # Item code for "Potatoes".
    "potato": "00000116",
    # Item code for "Rape or colza seed".
    "rapeseed": "00000270",
    # Item code for "Rice".
    "rice": "00000027",
    # Item code for "Rye".
    "rye": "00000071",
    # Item code for "Sorghum".
    "sorghum": "00000083",
    # Item code for "Soya beans".
    "soybean": "00000236",
    # Item code for "Sugar beet".
    "sugarbeet": "00000157",
    # Item code for "Sugar cane".
    "sugarcane": "00000156",
    # Item code for "Sunflower seed".
    "sunflower": "00000267",
    # Item code for "Wheat".
    "wheat": "00000015",
}


def add_table_and_variable_metadata(tb: Table) -> Table:
    # Add a short name to the combined table.
    tb.metadata.short_name = "attainable_yields"

    # Update each variable's metadata.
    for column in tb.columns:
        title = (
            column.capitalize()
            .replace("_", " ")
            .replace("Oilpalm", "Oil palm")
            .replace("Sugarbeet", "Sugar beet")
            .replace("Sugarcane", "Sugar cane")
            .replace("Sunflower", "Sunflower seed")
        )
        tb[column].metadata.title = title
        tb[column].metadata.unit = "tonnes per hectare"
        tb[column].metadata.short_unit = "tonnes/ha"

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load attainable yields data from Mueller et al. (2012).
    ds_mueller: Dataset = paths.load_dependency("mueller_et_al_2012")

    # Read main table from dataset.
    tb_mueller = ds_mueller["mueller_et_al_2012"].reset_index()

    # Load faostat data on crop and livestock production.
    ds_qcl: Dataset = paths.load_dependency("faostat_qcl")

    # Read main table from dataset.
    tb_qcl = ds_qcl["faostat_qcl"].reset_index()

    #
    # Process data.
    #
    # Select required country, element and items.
    tb_qcl = tb_qcl[
        (tb_qcl["element_code"] == ELEMENT_CODE_FOR_YIELD) & (tb_qcl["item_code"].isin(ITEM_CODES.values()))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units for yield have changed."
    assert list(tb_qcl["unit"].unique()) == ["tonnes per hectare"], error

    # Transpose data.
    tb_qcl = tb_qcl.pivot(index=["country", "year"], columns=["item_code"], values=["value"])
    item_code_to_name = {code: name for name, code in ITEM_CODES.items()}
    tb_qcl.columns = [f"{item_code_to_name[column[1]]}_yield" for column in tb_qcl.columns]
    tb_qcl = tb_qcl.reset_index()

    # Combine both tables.
    tb = pd.merge(tb_qcl, tb_mueller.drop(columns=["year"]), on=["country"], how="inner")

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Add the yield gap (difference between maximum attainable yields minus actual yield).
    for item in ITEM_CODES:
        # Clip the series at zero (negative values mean that the yield has been attained).
        tb[f"{item}_yield_gap"] = tb[f"{item}_attainable_yield"] - tb[f"{item}_yield"].clip(0)

    # Update table and variable metadata.
    tb = add_table_and_variable_metadata(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Combine sources and licenses.
    # Skip FAOSTAT source description (which is long and mostly irrelevant for the topic at hand).
    ds_qcl.metadata.sources[0].description = None
    ds_garden.metadata.sources = ds_mueller.metadata.sources + ds_qcl.metadata.sources
    ds_garden.metadata.licenses = ds_mueller.metadata.licenses + ds_qcl.metadata.licenses

    # Save changes in the new garden dataset.
    ds_garden.save()
