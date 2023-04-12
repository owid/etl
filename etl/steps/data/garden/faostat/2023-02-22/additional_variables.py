"""Dataset that combines different variables of other FAOSTAT datasets.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from shared import NAMESPACE

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def generate_arable_land_per_crop_output(df_rl: pd.DataFrame, df_qi: pd.DataFrame) -> Table:
    # Item code for item "Arable land" of faostat_rl dataset.
    ITEM_CODE_FOR_ARABLE_LAND = "00006621"
    # Element code for element "Area" of faostat_rl dataset.
    ELEMENT_CODE_FOR_AREA = "005110"
    # Item code for item "Crops" of faostat_qi dataset.
    ITEM_CODE_FOR_CROPS = "00002041"
    # Element code for "Gross Production Index Number (2014-2016 = 100)" of faostat_qi dataset.
    ELEMENT_CODE_PRODUCTION_INDEX = "000432"
    # Reference year for production index (values of area/index will be divided by the value on this year).
    PRODUCTION_INDEX_REFERENCE_YEAR = 1961

    # Select the necessary item and element of the land use dataset.
    df_rl = df_rl[
        (df_rl["item_code"] == ITEM_CODE_FOR_ARABLE_LAND) & (df_rl["element_code"] == ELEMENT_CODE_FOR_AREA)
    ].reset_index(drop=True)
    # Sanity check.
    error = "Unit for element 'Area' in faostat_rl has changed."
    assert list(df_rl["unit"].unique()) == ["hectares"], error
    # Rename columns and select only necessary columns.
    df_rl = df_rl[["country", "year", "value"]].rename(columns={"value": "area"}).reset_index(drop=True)

    # Select the necessary item and element of the production index dataset.
    df_qi = df_qi[
        (df_qi["element_code"] == ELEMENT_CODE_PRODUCTION_INDEX) & (df_qi["item_code"] == ITEM_CODE_FOR_CROPS)
    ].reset_index(drop=True)
    # Sanity check.
    error = "Unit for element 'Gross Production Index Number (2014-2016 = 100)' in faostat_qi has changed."
    assert list(df_qi["unit"].unique()) == ["index"], error
    # Rename columns and select only necessary columns.
    df_qi = df_qi[["country", "year", "value"]].rename(columns={"value": "index"})

    # Combine both dataframes.
    combined = pd.merge(df_rl, df_qi, on=["country", "year"], how="inner")

    # Create the new variable of arable land per crop output.
    combined["value"] = combined["area"] / combined["index"]

    # Add a column of a reference value for each country, and normalize data by dividing by the reference value.
    reference = combined[combined["year"] == PRODUCTION_INDEX_REFERENCE_YEAR][["country", "value"]].reset_index(
        drop=True
    )
    combined = pd.merge(
        combined, reference[["country", "value"]], on=["country"], how="left", suffixes=("", "_reference")
    )
    combined["value"] /= combined["value_reference"]

    # Remove all countries for which we did not have data for the reference year.
    combined = combined.dropna(subset="value").reset_index(drop=True)

    # Remove unnecessary columns and rename conveniently.
    combined = combined.drop(columns=["value_reference"]).rename(columns={"value": "arable_land_per_crop_output"})

    # Set an appropriate index and sort conveniently.
    tb_combined = Table(
        combined.set_index(["country", "year"], verify_integrity=True).sort_index(),
        short_name="arable_land_per_crop_output",
    )

    return tb_combined


def generate_area_used_for_production_per_crop_type(df_qcl: pd.DataFrame) -> Table:
    # Element code for "Area harvested" of faostat_qcl dataset.
    ELEMENT_CODE_FOR_AREA_HARVESTED = "005312"

    # List of items belonging to item group "Coarse Grain, Total", according to
    # https://www.fao.org/faostat/en/#definitions
    ITEM_CODES_COARSE_GRAINS = [
        "00000044",  # Barley
        "00000089",  # Buckwheat
        "00000101",  # Canary seed
        "00000108",  # Cereals n.e.c.
        "00000108",  # Cereals nes
        "00000094",  # Fonio
        "00000103",  # Grain, mixed
        "00000056",  # Maize
        "00000056",  # Maize (corn)
        "00000079",  # Millet
        "00000103",  # Mixed grain
        "00000075",  # Oats
        "00000092",  # Quinoa
        "00000071",  # Rye
        "00000083",  # Sorghum
        "00000097",  # Triticale
    ]

    # Item codes for croup groups from faostat_qcl.
    # Additionally, coarse grains will be added to these groups.
    ITEM_CODES_OF_CROP_GROUPS = [
        "00001717",  # Cereals
        "00001804",  # Citrus Fruit
        "00001738",  # Fruit
        "00000780",  # Jute
        "00001732",  # Oilcrops, Oil Equivalent
        "00001726",  # Pulses
        "00001720",  # Roots and tubers
        "00001729",  # Treenuts
        "00001735",  # Vegetables
        "00001814",  # Coarse Grain
    ]

    error = "Not all expected item codes were found in QCL."
    assert set(ITEM_CODES_COARSE_GRAINS) < set(df_qcl["item_code"]), error

    # Select the world and the element code for area harvested.
    area_by_crop_type = df_qcl[
        (df_qcl["country"] == "World") & (df_qcl["element_code"] == ELEMENT_CODE_FOR_AREA_HARVESTED)
    ].reset_index(drop=True)
    error = "Unit for element 'Area harvested' in faostat_qcl has changed."
    assert list(area_by_crop_type["unit"].unique()) == ["hectares"], error

    # Add items for item group "Coarse Grain, Total".
    coarse_grains = (
        area_by_crop_type[(area_by_crop_type["item_code"].isin(ITEM_CODES_COARSE_GRAINS))]
        .groupby("year", as_index=False)
        .agg({"value": "sum"})
        .assign(**{"item": "Coarse Grain", "item_code": "00001814"})
    )
    area_by_crop_type = pd.concat(
        [area_by_crop_type[~area_by_crop_type["item_code"].isin(ITEM_CODES_COARSE_GRAINS)], coarse_grains],
        ignore_index=True,
    )

    area_by_crop_type = area_by_crop_type[area_by_crop_type["item_code"].isin(ITEM_CODES_OF_CROP_GROUPS)].reset_index(
        drop=True
    )

    descriptions = "Definitions by FAOSTAT:"
    for item in sorted(set(area_by_crop_type["item"])):
        descriptions += f"\n\nItem: {item}"
        item_description = area_by_crop_type[area_by_crop_type["item"] == item]["item_description"].fillna("").iloc[0]
        if len(item_description) > 0:
            descriptions += f"\nDescription: {item_description}"

    descriptions += f"\n\nMetric: {area_by_crop_type['element'].iloc[0]}"
    descriptions += f"\nDescription: {area_by_crop_type['element_description'].iloc[0]}"

    # Create a table with the necessary columns, set an appropriate index, and sort conveniently.
    tb_area_by_crop_type = Table(
        area_by_crop_type[["item", "year", "value"]]
        .rename(columns={"value": "area_used_for_production"})
        .set_index(["item", "year"], verify_integrity=True)
        .sort_index(),
        short_name="area_used_per_crop_type",
    )

    # Add a table description.
    tb_area_by_crop_type.metadata.description = descriptions

    return tb_area_by_crop_type


def run(dest_dir: str) -> None:

    #
    # Load inputs.
    #
    # Load dataset about land use, load its main (long-format) table, and create a convenient dataframe.
    ds_rl: Dataset = paths.load_dependency(f"{NAMESPACE}_rl")
    tb_rl = ds_rl[f"{NAMESPACE}_rl"]
    df_rl = pd.DataFrame(tb_rl).reset_index()

    # Load dataset about production indices, load its main (long-format) table, and create a convenient dataframe.
    ds_qi: Dataset = paths.load_dependency(f"{NAMESPACE}_qi")
    tb_qi = ds_qi[f"{NAMESPACE}_qi"]
    df_qi = pd.DataFrame(tb_qi).reset_index()

    # Load dataset about crops and livestock, load its main (long-format) table, and create a convenient dataframe.
    ds_qcl: Dataset = paths.load_dependency(f"{NAMESPACE}_qcl")
    tb_qcl = ds_qcl[f"{NAMESPACE}_qcl"]
    df_qcl = pd.DataFrame(tb_qcl).reset_index()

    #
    # Process data.
    #
    # Create data for arable land per crop output.
    tb_arable_land_per_crop_output = generate_arable_land_per_crop_output(df_rl=df_rl, df_qi=df_qi)

    # Create data for area used for production per crop type.
    tb_area_by_crop_type = generate_area_used_for_production_per_crop_type(df_qcl=df_qcl)
    # Issues: Oilcrops start in 1991 instead of 1961 like in the old version.

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    # Take by default the metadata of one of the datasets, simply to get the FAOSTAT sources (the rest of the metadata
    # will be defined in the metadata yaml file).
    ds_garden = create_dataset(dest_dir, tables=[tb_arable_land_per_crop_output, tb_area_by_crop_type])
    ds_garden.save()
