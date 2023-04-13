"""Dataset that combines different variables of other FAOSTAT datasets.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from owid.datautils.dataframes import multi_merge
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

    # Prepare variable description.
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
    tb_area_by_crop_type["area_used_for_production"].metadata.description = descriptions

    return tb_area_by_crop_type


def generate_percentage_of_sustainable_and_overexploited_fish(df_sdgb: pd.DataFrame) -> Table:
    # "14.4.1 Proportion of fish stocks within biologically sustainable levels (not overexploited) (%)"
    ITEM_CODE_SUSTAINABLE_FISH = "00024029"

    # Select the necessary item.
    df_sdgb = df_sdgb[df_sdgb["item_code"] == ITEM_CODE_SUSTAINABLE_FISH].reset_index(drop=True)
    error = "Unit for fish data has changed."
    assert list(df_sdgb["unit"].unique()) == ["percent"], error
    error = "Element for fish data has changed."
    assert list(df_sdgb["element"].unique()) == ["Value"], error

    # Select necessary columns (item and element descriptions are empty in the current version).
    df_sdgb = df_sdgb[["country", "year", "value"]].rename(columns={"value": "sustainable_fish"})

    error = "Percentage of sustainable fish larger than 100%."
    assert (df_sdgb["sustainable_fish"] <= 100).all(), error

    # Add column of percentage of overexploited fish.
    df_sdgb["overexploited_fish"] = 100 - df_sdgb["sustainable_fish"]

    # Create a table with the necessary columns, set an appropriate index, and sort conveniently.
    tb_fish = (
        Table(df_sdgb, short_name="share_of_sustainable_and_overexploited_fish")
        .set_index(["country", "year"], verify_integrity=True)
        .sort_index()
    )

    return tb_fish


def generate_spared_land_from_increased_yields(df_qcl: pd.DataFrame) -> Table:
    # Reference year (to see how much land we spare from increased yields).
    REFERENCE_YEAR = 1961
    # Element code for "Yield" of faostat_qcl dataset.
    ELEMENT_CODE_FOR_YIELD = "005419"
    # Element code for "Production" of faostat_qcl dataset.
    ELEMENT_CODE_FOR_PRODUCTION = "005510"

    # Item codes for crop groups from faostat_qcl.
    ITEM_CODES_OF_CROP_GROUPS = [
        "00001717",  # Cereals
        "00001738",  # Fruit
        "00001726",  # Pulses
        "00001720",  # Roots and tubers
        "00001735",  # Vegetables
        "00001723",  # Sugar Crops
        "00001729",  # Treenuts
        # Data for fibre crops has changed significantly since last version, and is also significantly smaller than
        # other crop groups, so we omit it.
        # "00000821",  # Fibre crops.
    ]

    # Select necessary items and elements.
    spared_land = df_qcl[
        (df_qcl["item_code"].isin(ITEM_CODES_OF_CROP_GROUPS))
        & (df_qcl["element_code"].isin([ELEMENT_CODE_FOR_PRODUCTION, ELEMENT_CODE_FOR_YIELD]))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units for production and yield have changed."
    assert set(spared_land["unit"]) == set(["tonnes per hectare", "tonnes"]), error

    # Transpose dataframe.
    spared_land = spared_land.pivot(
        index=["country", "year", "item"], columns=["element"], values="value"
    ).reset_index()

    # Fix column name after pivotting.
    spared_land.columns = ["country", "year", "item", "Yield", "Production"]

    # Add columns for production and yield for a given reference year.
    reference_values = spared_land[spared_land["year"] == REFERENCE_YEAR].drop(columns=["year"])
    spared_land = pd.merge(
        spared_land, reference_values, on=["country", "item"], how="left", suffixes=("", f" in {REFERENCE_YEAR}")
    )

    # Drop countries for which we did not have data in the reference year.
    spared_land = spared_land.dropna().reset_index(drop=True)

    # Calculate area harvested that would be required given current production, but with the yield of the reference year.
    spared_land[f"Area with yield of {REFERENCE_YEAR}"] = (
        spared_land["Production"] / spared_land[f"Yield in {REFERENCE_YEAR}"]
    )
    # Calculate the real area harvested (given the current production and yield).
    spared_land["Area"] = spared_land["Production"] / spared_land["Yield"]

    # Keep only required columns
    spared_land = spared_land[["country", "year", "item", "Area", f"Area with yield of {REFERENCE_YEAR}"]].reset_index(
        drop=True
    )

    # Add total area for all crops.
    all_crops = (
        spared_land.groupby(["country", "year"], as_index=False, observed=True)
        .agg({"Area": sum, f"Area with yield of {REFERENCE_YEAR}": sum})
        .assign(**{"item": "All crops"})
    )
    spared_land = pd.concat([spared_land, all_crops], ignore_index=True)

    # Calculate the spared land in absolute value, and as a percentage of the land we would have used with no yield increase.
    spared_land["Spared land"] = spared_land[f"Area with yield of {REFERENCE_YEAR}"] - spared_land["Area"]
    spared_land["Spared land (%)"] = (
        100 * spared_land["Spared land"] / spared_land[f"Area with yield of {REFERENCE_YEAR}"]
    )

    # Create a table with the necessary columns, set an appropriate index, and sort conveniently.
    tb_spared_land = Table(
        spared_land.set_index(["country", "year", "item"], verify_integrity=True).sort_index(),
        short_name="land_spared_by_increased_crop_yields",
        underscore=True,
    )

    return tb_spared_land


def generate_food_available_for_consumption(df_fbsc: pd.DataFrame) -> Table:
    # Element code for "Food available for consumption" of faostat_fbsc (in kilocalories per day per capita).
    ELEMENT_CODE_FOR_PER_CAPITA_FOOD = "0664pc"
    # Expected unit.
    CONSUMPTION_UNIT = "kilocalories per day per capita"

    # df_fbsc[df_fbsc["unit"].str.contains("kilocal")][["element_code", "element", "unit"]].drop_duplicates()

    df_fbsc = df_fbsc[(df_fbsc["element_code"] == ELEMENT_CODE_FOR_PER_CAPITA_FOOD)].reset_index(drop=True)

    # Sanity check.
    error = "Units for food available for consumption have changed."
    assert list(df_fbsc["unit"].unique()) == [CONSUMPTION_UNIT], error

    # List of food groups created by OWID.
    # Each food group contains one or more "item groups", defined by FAOSTAT.
    # Each item group contains one or more "item", defined by FAOSTAT.
    # The complete list of items coincides exactly with the complete list of items of FAOSTAT item group "Grand Total"
    # (with item group code 2901).
    # So all existing food items in FBSC are contained here, and there are no repetitions.
    # Notes:
    # * There are a few item groups that are not included here, namely "Vegetal Products" (item group code 2903),
    #   and "Animal Products" (item group code 2941). But their items are contained in other item groups, so including them
    #   would cause unnecessary repetition of items.
    # * To check for the components of an individual item group:
    # from etl.paths import DATA_DIR
    # metadata = Dataset(DATA_DIR / "meadow/faostat/2023-02-22/faostat_metadata")
    # item_groups = metadata["faostat_fbs_item_group"]
    # set(item_groups.loc[2941]["item"])
    FOOD_GROUPS = {
        "Cereals and grains": [
            "00002905",  # Cereals, Excluding Beer
            # Item group contains:
            # 'Barley and products',
            # 'Cereals, Other',
            # 'Maize and products',
            # 'Millet and products',
            # 'Oats',
            # 'Rice and products',
            # 'Rye and products',
            # 'Sorghum and products',
            # 'Wheat and products',
        ],
        "Pulses": [
            "00002911",  # Pulses
            # Item group contains:
            # 'Beans',
            # 'Peas',
            # 'Pulses, Other and products',
        ],
        "Starchy roots": [
            "00002907",  # Starchy Roots
            # Item group contains:
            # 'Cassava and products',
            # 'Potatoes and products',
            # 'Roots, Other',
            # 'Sweet potatoes',
            # 'Yams',
        ],
        "Fruits and vegetables": [
            "00002919",  # Fruits - Excluding Wine
            # Item group contains:
            # 'Apples and products',
            # 'Bananas',
            # 'Citrus, Other',
            # 'Dates',
            # 'Fruits, other',
            # 'Grapefruit and products',
            # 'Grapes and products (excl wine)',
            # 'Lemons, Limes and products',
            # 'Oranges, Mandarines',
            # 'Pineapples and products',
            # 'Plantains',
            "00002918",  # Vegetables
            # Item group contains:
            # 'Onions',
            # 'Tomatoes and products',
            # 'Vegetables, other',
        ],
        "Oils and fats": [
            "00002914",  # Vegetable Oils
            # Item group contains:
            # 'Coconut Oil',
            # 'Cottonseed Oil',
            # 'Groundnut Oil',
            # 'Maize Germ Oil',
            # 'Oilcrops Oil, Other',
            # 'Olive Oil',
            # 'Palm Oil',
            # 'Palmkernel Oil',
            # 'Rape and Mustard Oil',
            # 'Ricebran Oil',
            # 'Sesameseed Oil',
            # 'Soyabean Oil',
            # 'Sunflowerseed Oil'
            "00002946",  # Animal fats group
            # Item group contains:
            # 'Butter, Ghee',
            # 'Cream',
            # 'Fats, Animals, Raw',
            # 'Fish, Body Oil',
            # 'Fish, Liver Oil'
            "00002913",  # Oilcrops
            # Item group contains:
            # 'Coconuts - Incl Copra',
            # 'Cottonseed',
            # 'Groundnuts',
            # 'Oilcrops, Other',
            # 'Olives (including preserved)',
            # 'Palm kernels',
            # 'Rape and Mustardseed',
            # 'Sesame seed',
            # 'Soyabeans',
            # 'Sunflower seed'
            "00002912",  # Treenuts
            # Item group contains:
            # 'Nuts and products',
        ],
        "Sugar": [
            "00002909",  # Sugar & Sweeteners
            # Item group contains:
            # 'Honey',
            # 'Sugar (Raw Equivalent)',
            # 'Sugar non-centrifugal',
            # 'Sweeteners, Other',
            "00002908",  # Sugar crops
            # Item group contains:
            # 'Sugar beet',
            # 'Sugar cane',
        ],
        "Meat": [
            "00002960",  # Fish and seafood
            # Item group contains:
            # 'Aquatic Animals, Others',
            # 'Cephalopods',
            # 'Crustaceans',
            # 'Demersal Fish',
            # 'Freshwater Fish',
            # 'Marine Fish, Other',
            # 'Molluscs, Other',
            # 'Pelagic Fish',
            "00002943",  # Meat, total
            # Item group contains:
            # 'Bovine Meat',
            # 'Meat, Other',
            # 'Mutton & Goat Meat',
            # 'Pigmeat',
            # 'Poultry Meat',
        ],
        "Dairy and eggs": [
            "00002948",  # Milk - Excluding Butter
            # Item group contains:
            # 'Milk - Excluding Butter',
            "00002949",  # Eggs
            # Item group contains:
            # 'Eggs',
        ],
        "Alcoholic beverages": [
            "00002924",  # Alcoholic Beverages
            # Item group contains:
            # 'Alcohol, Non-Food',
            # 'Beer',
            # 'Beverages, Alcoholic',
            # 'Beverages, Fermented',
            # 'Wine',
        ],
        "Other": [
            "00002928",  # Miscellaneous
            # Item group contains:
            # 'Infant food',
            # 'Miscellaneous',
            "00002923",  # Spices
            # Item group contains:
            # 'Cloves',
            # 'Pepper',
            # 'Pimento',
            # 'Spices, Other',
            "00002922",  # Stimulants
            # Item group contains:
            # 'Cocoa Beans and products',
            # 'Coffee and products',
            # 'Tea (including mate)',
            "00002945",  # Offals
            # Item group contains:
            # 'Offals, Edible',
            "00002961",  # Aquatic Products, Other
            # 'Aquatic Plants',
            # 'Meat, Aquatic Mammals',
        ],
    }

    # Sanity check.
    error = "Not all expected item codes are found in the data."
    assert set([item_code for group in FOOD_GROUPS.values() for item_code in group]) <= set(df_fbsc["item_code"]), error

    # Create a list of dataframes, one for each food group.
    dfs = [
        df_fbsc[df_fbsc["item_code"].isin(FOOD_GROUPS[group])]
        .groupby(["country", "year"], as_index=False, observed=True)
        .agg({"value": "sum"})
        .rename(columns={"value": group})
        for group in FOOD_GROUPS
    ]
    combined = multi_merge(dfs=dfs, on=["country", "year"], how="outer")

    # Create a table, set an appropriate index, and sort conveniently.
    tb_food_available_for_consumption = Table(
        combined.set_index(["country", "year"], verify_integrity=True).sort_index(),
        short_name="food_available_for_consumption",
        underscore=True,
    )

    # Prepare variable metadata.
    common_description = "Data represents the average daily per capita supply of calories from the full range of commodities, grouped by food categories. Note that these figures do not correct for waste at the household/consumption level so may not directly reflect the quantity of food finally consumed by a given individual.\n\nSpecific food commodities have been grouped into higher-level categories."
    for group in FOOD_GROUPS:
        item_names = list(df_fbsc[df_fbsc["item_code"].isin(FOOD_GROUPS[group])]["item"].unique())
        description = (
            common_description
            + f" Food group '{group}' includes the FAO item groups: '"
            + "', '".join(item_names)
            + "'."
        )
        tb_food_available_for_consumption[underscore(group)].metadata.title = group
        tb_food_available_for_consumption[underscore(group)].metadata.unit = CONSUMPTION_UNIT
        tb_food_available_for_consumption[underscore(group)].metadata.short_unit = "kcal"
        tb_food_available_for_consumption[underscore(group)].metadata.description = description

    return tb_food_available_for_consumption


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

    # Load dataset about SDG indicators, load its main (long-format) table, and create a convenient dataframe.
    ds_sdgb: Dataset = paths.load_dependency(f"{NAMESPACE}_sdgb")
    tb_sdgb = ds_sdgb[f"{NAMESPACE}_sdgb"]
    df_sdgb = pd.DataFrame(tb_sdgb).reset_index()

    # Load dataset about food balances, load its main (long-format) table, and create a convenient dataframe.
    ds_fbsc: Dataset = paths.load_dependency(f"{NAMESPACE}_fbsc")
    tb_fbsc = ds_fbsc[f"{NAMESPACE}_fbsc"]
    df_fbsc = pd.DataFrame(tb_fbsc).reset_index()

    #
    # Process data.
    #
    # Create table for arable land per crop output.
    tb_arable_land_per_crop_output = generate_arable_land_per_crop_output(df_rl=df_rl, df_qi=df_qi)

    # Create table for area used for production per crop type.
    tb_area_by_crop_type = generate_area_used_for_production_per_crop_type(df_qcl=df_qcl)

    # Create table for the share of sustainable and overexploited fish.
    tb_sustainable_and_overexploited_fish = generate_percentage_of_sustainable_and_overexploited_fish(df_sdgb=df_sdgb)

    # Create table for spared land due to increased yields.
    tb_spared_land_from_increased_yields = generate_spared_land_from_increased_yields(df_qcl=df_qcl)

    # Create table for dietary compositions by commodity group.
    tb_food_available_for_consumption = generate_food_available_for_consumption(df_fbsc=df_fbsc)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    # Take by default the metadata of one of the datasets, simply to get the FAOSTAT sources (the rest of the metadata
    # will be defined in the metadata yaml file).
    ds_garden = create_dataset(
        dest_dir,
        tables=[
            tb_arable_land_per_crop_output,
            tb_area_by_crop_type,
            tb_sustainable_and_overexploited_fish,
            tb_spared_land_from_increased_yields,
            tb_food_available_for_consumption,
        ],
    )
    ds_garden.save()
