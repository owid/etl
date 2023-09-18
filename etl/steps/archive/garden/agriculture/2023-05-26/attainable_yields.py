"""Combine attainable yields from Mueller et al. (2012) with the latest FAOSTAT yields data.

The resulting dataset contains:
1. item_yield: Yield from FAOSTAT (e.g. barley_yield).
2. item_attainable_yield: Maximum attainable yield from Mueller et al. (2012) (e.g. barley_attainable_yield).
3. item_yield_gap: Yield gap, which is the difference between the previous two (e.g. barley_yield_gap).

Elements 2 and 3 are provided only for items that were included in Mueller et al. (2012), whereas element 1 is
provided also for other items.

This dataset will be imported by the crop_yields explorers step, which feeds our Crop Yields explorer:
https://ourworldindata.org/explorers/crop-yields
"""

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
    # Items included in Mueller et al. (2012):
    # Item code for "Barley".
    "barley": "00000044",
    # Item code for "Cassava, fresh"
    "cassava": "00000125",
    # Item code for "Seed cotton, unginned".
    "cotton": "00000328",
    # Item code for "Groundnuts, excluding shelled".
    # NOTE: This was wrong, the correct item code should have been "00000242". This is corrected in the new version.
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
    # Additional items not included in Mueller et al. (2012):
    # Item code for "Almonds, in shell".
    "almond": "00000221",
    # Item code for "Bananas".
    "banana": "00000486",
    # Item code for "Beans, dry".
    "bean": "00000176",
    # Item code for "Cereals, primary".
    "cereal": "00001717",
    # Item code for "Cocoa beans".
    "cocoa": "00000661",
    # Item code for "Coffee, green".
    "coffee": "00000656",
    # Item code for "Lettuce and chicory".
    "lettuce": "00000372",
    # Item code for "Oranges".
    "orange": "00000490",
    # Item code for "Peas, dry".
    "pea": "00000187",
    # Item code for "Tomatoes".
    "tomato": "00000388",
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
        if f"{item}_attainable_yield" in tb.columns:
            # Clip the series at zero (negative values mean that the yield has been attained).
            tb[f"{item}_yield_gap"] = (tb[f"{item}_attainable_yield"] - tb[f"{item}_yield"]).clip(0)

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
