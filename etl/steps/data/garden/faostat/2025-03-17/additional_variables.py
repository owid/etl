"""Dataset that combines different variables of other FAOSTAT datasets."""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of food groups created by OWID for FBSC (combination of FBS and FBSH).
# Each food group contains one or more "item groups", defined in dataset by FAOSTAT.
# Each item group contains one or more "item", defined by FAOSTAT.
# The complete list of items coincides exactly with the complete list of items of FAOSTAT item group "Grand Total"
# (with item group code 2901).
# So all existing food items in FBSC are contained here, and there are no repetitions.
# Notes:
# * There are a few item groups that are not included here, namely "Vegetal Products" (item group code 2903),
#   and "Animal Products" (item group code 2941). But their items are contained in other item groups, so including
#   them would cause unnecessary repetition of items.
# * To check for the components of an individual item group:
# from etl.paths import DATA_DIR
# metadata = Dataset(DATA_DIR / "meadow/faostat/2023-02-22/faostat_metadata")
# item_groups = metadata["faostat_fbs_item_group"]
# set(item_groups.loc[2941]["item"])
FOOD_GROUPS_FBSC = {
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


def generate_arable_land_per_crop_output(tb_rl: Table, tb_qi: Table) -> Table:
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
    tb_rl = tb_rl[
        (tb_rl["item_code"] == ITEM_CODE_FOR_ARABLE_LAND) & (tb_rl["element_code"] == ELEMENT_CODE_FOR_AREA)
    ].reset_index(drop=True)
    # Sanity check.
    error = "Unit for element 'Area' in faostat_rl has changed."
    assert list(tb_rl["unit"].unique()) == ["hectares"], error
    # Rename columns and select only necessary columns.
    tb_rl = tb_rl[["country", "year", "value"]].rename(columns={"value": "area"}, errors="raise").reset_index(drop=True)

    # Select the necessary item and element of the production index dataset.
    tb_qi = tb_qi[
        (tb_qi["element_code"] == ELEMENT_CODE_PRODUCTION_INDEX) & (tb_qi["item_code"] == ITEM_CODE_FOR_CROPS)
    ].reset_index(drop=True)
    # Sanity check.
    error = "Unit for element 'Gross Production Index Number (2014-2016 = 100)' in faostat_qi has changed."
    assert list(tb_qi["unit"].unique()) == [""], error
    # Rename columns and select only necessary columns.
    tb_qi = tb_qi[["country", "year", "value"]].rename(columns={"value": "index"}, errors="raise")

    # Combine both tables.
    combined = tb_rl.merge(tb_qi, on=["country", "year"], how="inner", validate="one_to_one")

    # Create the new variable of arable land per crop output.
    combined["value"] = combined["area"] / combined["index"]

    # Add a column of a reference value for each country, and normalize data by dividing by the reference value.
    reference = combined[combined["year"] == PRODUCTION_INDEX_REFERENCE_YEAR][["country", "value"]].reset_index(
        drop=True
    )
    combined = combined.merge(reference[["country", "value"]], on=["country"], how="left", suffixes=("", "_reference"))
    combined["value"] /= combined["value_reference"]

    # Remove all countries for which we did not have data for the reference year.
    combined = combined.dropna(subset="value").reset_index(drop=True)

    # Remove unnecessary columns and rename conveniently.
    combined = combined.drop(columns=["value_reference"], errors="raise").rename(
        columns={"value": "arable_land_per_crop_output"}, errors="raise"
    )

    # Set an appropriate index and sort conveniently.
    tb_combined = combined.format(["country", "year"], short_name="arable_land_per_crop_output")

    return tb_combined


def generate_area_used_for_production_per_crop_type(tb_qcl: Table) -> Table:
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
    assert set(ITEM_CODES_COARSE_GRAINS) < set(tb_qcl["item_code"]), error

    # Select the world and the element code for area harvested.
    area_by_crop_type = tb_qcl[
        (tb_qcl["country"] == "World") & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_AREA_HARVESTED)
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
    area_by_crop_type = pr.concat(
        [area_by_crop_type[~area_by_crop_type["item_code"].isin(ITEM_CODES_COARSE_GRAINS)], coarse_grains],
        ignore_index=True,
    )

    area_by_crop_type = area_by_crop_type[area_by_crop_type["item_code"].isin(ITEM_CODES_OF_CROP_GROUPS)].reset_index(
        drop=True
    )

    # Prepare variable description.
    descriptions = ""
    for item in sorted(set(area_by_crop_type["item"])):
        item_description = area_by_crop_type[area_by_crop_type["item"] == item]["item_description"].fillna("").iloc[0]
        if len(item_description) > 0:
            descriptions += f"\n\n- {item}: {item_description}"

    descriptions += f"\n\n- {area_by_crop_type['element'].iloc[0]}: {area_by_crop_type['element_description'].iloc[0]}"

    # Select the necessary columns, set an appropriate index, and sort conveniently.
    tb_area_by_crop_type = (
        area_by_crop_type[["item", "year", "value"]]
        .rename(columns={"value": "area_used_for_production"}, errors="raise")
        .set_index(["item", "year"], verify_integrity=True)
        .sort_index()
    )
    tb_area_by_crop_type.metadata.short_name = "area_used_per_crop_type"

    # Add table description to the indicator's key description.
    tb_area_by_crop_type["area_used_for_production"].metadata.description_from_producer = descriptions

    return tb_area_by_crop_type


def generate_percentage_of_sustainable_and_overexploited_fish(tb_sdgb: Table) -> Table:
    # "14.4.1 Proportion of fish stocks within biologically sustainable levels"
    ITEM_CODE_SUSTAINABLE_FISH = "000000000024029"

    # Select the necessary item.
    tb_sdgb = tb_sdgb[tb_sdgb["item_code"] == ITEM_CODE_SUSTAINABLE_FISH].reset_index(drop=True)
    error = "Unit for fish data has changed."
    assert list(tb_sdgb["unit"].unique()) == ["percent"], error
    error = "Element for fish data has changed."
    assert list(tb_sdgb["element"].unique()) == ["Value"], error

    # Select necessary columns (item and element descriptions are empty in the current version).
    tb_sdgb = tb_sdgb[["country", "year", "value"]].rename(columns={"value": "sustainable_fish"}, errors="raise")

    error = "Percentage of sustainable fish larger than 100%."
    assert (tb_sdgb["sustainable_fish"] <= 100).all(), error

    # Add column of percentage of overexploited fish.
    tb_sdgb["overexploited_fish"] = 100 - tb_sdgb["sustainable_fish"]

    # Set an appropriate index and sort conveniently.
    tb_fish = tb_sdgb.format(["country", "year"], short_name="share_of_sustainable_and_overexploited_fish")

    return tb_fish


def generate_spared_land_from_increased_yields(tb_qcl: Table) -> Table:
    # Reference year (to see how much land we spare from increased yields).
    REFERENCE_YEAR = 1961
    # Element code for "Yield" of faostat_qcl dataset.
    ELEMENT_CODE_FOR_YIELD = "005412"
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
    spared_land = tb_qcl[
        (tb_qcl["item_code"].isin(ITEM_CODES_OF_CROP_GROUPS))
        & (tb_qcl["element_code"].isin([ELEMENT_CODE_FOR_PRODUCTION, ELEMENT_CODE_FOR_YIELD]))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units for production and yield have changed."
    assert set(spared_land["unit"]) == set(["tonnes per hectare", "tonnes"]), error

    # Transpose table.
    spared_land = spared_land.pivot(
        index=["country", "year", "item"], columns=["element"], values="value", join_column_levels_with="_"
    )

    # Add columns for production and yield for a given reference year.
    reference_values = spared_land[spared_land["year"] == REFERENCE_YEAR].drop(columns=["year"], errors="raise")
    spared_land = spared_land.merge(
        reference_values, on=["country", "item"], how="left", suffixes=("", f" in {REFERENCE_YEAR}")
    )

    # Drop countries for which we did not have data in the reference year.
    spared_land = spared_land.dropna().reset_index(drop=True)

    # Calculate area harvested that would be required given current production, with the yield of the reference year.
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
        .agg({"Area": "sum", f"Area with yield of {REFERENCE_YEAR}": "sum"})
        .assign(**{"item": "All crops"})
    )
    spared_land = pr.concat([spared_land, all_crops], ignore_index=True)

    # Calculate the spared land in total value, and as a percentage of land we would have used with no yield increase.
    spared_land["Spared land"] = spared_land[f"Area with yield of {REFERENCE_YEAR}"] - spared_land["Area"]
    spared_land["Spared land (%)"] = (
        100 * spared_land["Spared land"] / spared_land[f"Area with yield of {REFERENCE_YEAR}"]
    )

    # Set an appropriate index and sort conveniently.
    tb_spared_land = spared_land.format(["country", "year", "item"], short_name="land_spared_by_increased_crop_yields")

    return tb_spared_land


def generate_food_available_for_consumption(tb_fbsc: Table) -> Table:
    # Element code for "Food available for consumption" of faostat_fbsc (in kilocalories per day per capita).
    ELEMENT_CODE_FOR_PER_CAPITA_FOOD = "0664pc"
    # Expected unit.
    CONSUMPTION_UNIT = "kilocalories per day per capita"

    # Select relevant metric.
    tb_fbsc = tb_fbsc[(tb_fbsc["element_code"] == ELEMENT_CODE_FOR_PER_CAPITA_FOOD)].reset_index(drop=True)

    # Sanity check.
    error = "Units for food available for consumption have changed."
    assert list(tb_fbsc["unit"].unique()) == [CONSUMPTION_UNIT], error

    # Sanity check.
    error = "Not all expected item codes are found in the data."
    assert set([item_code for group in FOOD_GROUPS_FBSC.values() for item_code in group]) <= set(
        tb_fbsc["item_code"]
    ), error

    # Create a list of tables, one for each food group.
    tables = [
        tb_fbsc[tb_fbsc["item_code"].isin(FOOD_GROUPS_FBSC[group])]
        .groupby(["country", "year"], as_index=False, observed=True)
        .agg({"value": "sum"})
        .rename(columns={"value": group}, errors="raise")
        for group in FOOD_GROUPS_FBSC
    ]
    combined = pr.multi_merge(tables=tables, on=["country", "year"], how="outer")

    # Ensure all column names are snake-case, set an appropriate index and and sort conveniently.
    tb_food_available_for_consumption = combined.underscore().format(
        ["country", "year"], short_name="food_available_for_consumption"
    )

    # Prepare variable metadata.
    common_description = (
        "This data represents the average daily per capita supply of calories from the full range of "
        "commodities, grouped by food categories. Note that these figures do not correct for waste at the "
        "household or consumption level, so they may not directly reflect the quantity of food finally consumed by a "
        "given individual.\n\nSpecific food commodities have been grouped into higher-level categories."
    )
    for group in FOOD_GROUPS_FBSC:
        item_names = list(tb_fbsc[tb_fbsc["item_code"].isin(FOOD_GROUPS_FBSC[group])]["item"].unique())
        description = (
            common_description
            + f" Food group '{group}' includes the FAO item groups: '"
            + "', '".join(item_names)
            + "'."
        )
        tb_food_available_for_consumption[
            underscore(group)
        ].metadata.title = f"Daily calorie supply per person from {group.lower().replace('other', 'other commodities')}"
        tb_food_available_for_consumption[underscore(group)].metadata.unit = CONSUMPTION_UNIT
        tb_food_available_for_consumption[underscore(group)].metadata.short_unit = "kcal"
        tb_food_available_for_consumption[underscore(group)].metadata.description_key = [description]

    return tb_food_available_for_consumption


def generate_macronutrient_compositions(tb_fbsc: Table) -> Table:
    # Item code for "Total" of faostat_fbsc.
    ITEM_CODE_ALL_PRODUCTS = "00002901"
    # Item code for "Vegetal Products" of faostat_fbsc.
    ITEM_CODE_VEGETAL_PRODUCTS = "00002903"
    # Item code for "Animal Products" of faostat_fbsc.
    ITEM_CODE_ANIMAL_PRODUCTS = "00002941"

    # Element code for "Food available for consumption" of faostat_fbsc (in kilocalories per day per capita).
    ELEMENT_CODE_FOR_ENERGY_PER_DAY = "0664pc"
    # Element code for "Food available for consumption" of faostat_fbsc (in grams of protein per day per capita).
    ELEMENT_CODE_FOR_PROTEIN_PER_DAY = "0674pc"
    # Element code for "Food available for consumption" of faostat_fbsc (in grams of fat per day per capita).
    ELEMENT_CODE_FOR_FAT_PER_DAY = "0684pc"

    # Assumed energy density by macronutrient, in kilocalories per gram of fat, protein or carbohydrates.
    KCAL_PER_GRAM_OF_FAT = 9
    KCAL_PER_GRAM_OF_PROTEIN = 4
    KCAL_PER_GRAM_OF_CARBOHYDRATES = 4

    # Select relevant items and elements.
    tb = tb_fbsc[
        (tb_fbsc["item_code"].isin([ITEM_CODE_ALL_PRODUCTS, ITEM_CODE_ANIMAL_PRODUCTS, ITEM_CODE_VEGETAL_PRODUCTS]))
        & (
            tb_fbsc["element_code"].isin(
                [ELEMENT_CODE_FOR_ENERGY_PER_DAY, ELEMENT_CODE_FOR_PROTEIN_PER_DAY, ELEMENT_CODE_FOR_FAT_PER_DAY]
            )
        )
    ].reset_index(drop=True)

    # Sanity check.
    error = "One or more of the units of food available for consumption has changed."
    assert list(tb["unit"].unique()) == [
        "kilocalories per day per capita",
        "grams of protein per day per capita",
        "grams of fat per day per capita",
    ], error

    # Food contents and element code for the metric of their consumption per day per capita.
    food_contents = {
        "energy": ELEMENT_CODE_FOR_ENERGY_PER_DAY,
        "fat": ELEMENT_CODE_FOR_FAT_PER_DAY,
        "protein": ELEMENT_CODE_FOR_PROTEIN_PER_DAY,
    }

    # Initialize a list of tables, one for each food content (energy, fat or protein).
    tables = []
    for content in food_contents:
        # Create a table for each food content, and add it to the list.
        tb_content = tb[tb["element_code"] == food_contents[content]].pivot(
            index=["country", "year"], columns=["item"], values=["value"], join_column_levels_with="_"
        )
        tb_content = tb_content.rename(
            columns={
                "value_Total": f"Total {content}",
                "value_Vegetal Products": f"{content.capitalize()} from vegetal products",
                "value_Animal Products": f"{content.capitalize()} from animal products",
            },
            errors="raise",
        )
        tables.append(tb_content)

        # Sanity check.
        error = f"The sum of animal and vegetable {content} does not add up to the total."
        assert (
            100
            * abs(
                tb_content[f"{content.capitalize()} from animal products"]
                + tb_content[f"{content.capitalize()} from vegetal products"]
                - tb_content[f"Total {content}"]
            )
            / tb_content[f"Total {content}"]
            < 1
        ).all(), error

    # Combine all tables.
    combined = pr.multi_merge(tables=tables, on=["country", "year"], how="outer")

    # Daily calorie supply from fat, per person.
    combined["Total energy from fat"] = combined["Total fat"] * KCAL_PER_GRAM_OF_FAT
    # Daily calorie supply from protein, per person.
    combined["Total energy from protein"] = combined["Total protein"] * KCAL_PER_GRAM_OF_PROTEIN
    # Daily calorie supply from carbohydrates (assumed to be the rest of the daily calorie supply), per person.
    # This is the difference between the total calorie supply minus the calorie supply from protein and fat.
    combined["Total energy from carbohydrates"] = (
        combined["Total energy"] - combined["Total energy from fat"] - combined["Total energy from protein"]
    )

    # Daily supply of carbohydrates per person.
    combined["Total carbohydrates"] = combined["Total energy from carbohydrates"] / KCAL_PER_GRAM_OF_CARBOHYDRATES

    # Calorie supply from fat as a percentage of the total daily calorie supply.
    combined["Share of energy from fat"] = 100 * combined["Total energy from fat"] / combined["Total energy"]
    # Calorie supply from protein as a percentage of the total daily calorie supply.
    combined["Share of energy from protein"] = 100 * combined["Total energy from protein"] / combined["Total energy"]
    # Calorie supply from carbohydrates as a percentage of the total daily calorie supply.
    combined["Share of energy from carbohydrates"] = (
        100 * combined["Total energy from carbohydrates"] / combined["Total energy"]
    )

    # Daily calorie supply from animal protein.
    combined["Energy from animal protein"] = combined["Protein from animal products"] * KCAL_PER_GRAM_OF_PROTEIN
    # Calorie supply from animal protein as a percentage of the total daily calorie supply.
    combined["Share of energy from animal protein"] = (
        100 * combined["Energy from animal protein"] / combined["Total energy"]
    )
    # Daily calorie supply from vegetal protein.
    combined["Energy from vegetal protein"] = combined["Protein from vegetal products"] * KCAL_PER_GRAM_OF_PROTEIN
    # Calorie supply from vegetal protein as a percentage of the total daily calorie supply.
    combined["Share of energy from vegetal protein"] = (
        100 * combined["Energy from vegetal protein"] / combined["Total energy"]
    )

    # Ensure all column names are snake-case, set an appropriate index, and sort conveniently.
    tb_combined = combined.underscore().format(["country", "year"], short_name="macronutrient_compositions")

    return tb_combined


def generate_fertilizers(tb_rfn: Table, tb_rl: Table) -> Table:
    # Item code for "Cropland" (which includes arable land and permanent crops).
    ITEM_CODE_FOR_CROPLAND = "00006620"

    # Element code for element "Area" of faostat_rl dataset.
    ELEMENT_CODE_FOR_AREA = "005110"

    # Item codes for fertilizers in faostat_rfn (namely nitrogen, phosphate and potash).
    ITEM_CODES_FOR_FERTILIZERS = ["00003102", "00003103", "00003104"]

    # Element code for use per area of cropland.
    ELEMENT_CODE_FOR_USE_PER_AREA = "005159"

    # Convert units from kilograms to tonnes.
    KG_TO_TONNES = 1e-3

    # Select necessary element (use per area).
    fertilizers = tb_rfn[(tb_rfn["element_code"] == ELEMENT_CODE_FOR_USE_PER_AREA)].reset_index(drop=True)

    # Sanity checks.
    error = "Unit for use per area has changed."
    assert list(fertilizers["unit"].unique()) == ["kilograms per hectare"], error

    error = "Unexpected list of item codes for fertilizers (maybe another was added to faostat_rfn)."
    assert set(fertilizers["item_code"]) == set(ITEM_CODES_FOR_FERTILIZERS), error

    # Transpose fertilizers data.
    fertilizers = fertilizers.pivot(
        index=["country", "year"], columns=["item"], values=["value"], join_column_levels_with="_"
    )

    # Rename columns conveniently.
    fertilizers = fertilizers.rename(
        columns={
            "value_Nutrient nitrogen N (total)": "nitrogen_per_cropland",
            "value_Nutrient phosphate P2O5 (total)": "phosphate_per_cropland",
            "value_Nutrient potash K2O (total)": "potash_per_cropland",
        },
        errors="raise",
    )

    # Add column for total fertilizers per area cropland.
    fertilizers["all_fertilizers_per_cropland"] = fertilizers[
        ["nitrogen_per_cropland", "phosphate_per_cropland", "potash_per_cropland"]
    ].sum(axis=1)

    # To get total agricultural use of fertilizers, we need cropland area.
    area = tb_rl[
        (tb_rl["element_code"] == ELEMENT_CODE_FOR_AREA) & (tb_rl["item_code"] == ITEM_CODE_FOR_CROPLAND)
    ].reset_index(drop=True)

    # Sanity check.
    error = "Unit for area has changed."
    assert list(area["unit"].unique()) == ["hectares"], error

    # Transpose area data.
    area = area.pivot(
        index=["country", "year"], columns=["item"], values=["value"], join_column_levels_with="_"
    ).rename(columns={"value_Cropland": "cropland"}, errors="raise")

    # Combine fertilizers and area.
    combined = fertilizers.merge(area, on=["country", "year"], how="outer", validate="one_to_one")

    # Add variables for total fertilizer use.
    for fertilizer in ["nitrogen", "phosphate", "potash", "all_fertilizers"]:
        combined[f"{fertilizer}_use"] = combined[f"{fertilizer}_per_cropland"] * combined["cropland"] * KG_TO_TONNES

    # Set an appropriate index and sort conveniently.
    tb_fertilizers = combined.format(["country", "year"], short_name="fertilizers")

    return tb_fertilizers


def generate_vegetable_oil_yields(tb_qcl: Table, tb_fbsc: Table) -> Table:
    # Element code for "Production" in faostat_qcl.
    ELEMENT_CODE_FOR_PRODUCTION_QCL = "005510"
    # Element code for "Production" in faostat_fbsc.
    ELEMENT_CODE_FOR_PRODUCTION_FBSC = "005511"
    # Unit for "Production".
    UNIT_FOR_PRODUCTION = "tonnes"
    # Element code for "Area harvested".
    ELEMENT_CODE_FOR_AREA = "005312"
    # Unit for "Area harvested".
    UNIT_FOR_AREA = "hectares"
    # Item code for "Vegetable Oils" (required to get the global production of vegetable oils on a given year).
    ITEM_CODE_FOR_VEGETABLE_OILS_TOTAL = "00002914"
    # Item codes in faostat_qcl for the area of the crops (we don't need the production of the crops).
    ITEM_CODE_FOR_EACH_CROP_AREA = {
        # The item "Palm fruit oil" refers to the fruit that contains both the pulp (that leads to palm oil)
        # as well as the kernel (that leads to palm kernel oil).
        "palm": "00000254",  # Palm fruit oil
        "sunflower": "00000267",  # Sunflower seed
        "rapeseed": "00000270",  # Rapeseed
        "soybean": "00000236",  # Soybeans
        "olive": "00000260",  # Olives
        "coconut": "00000249",  # Coconuts
        "groundnut": "00000242",  # Groundnuts
        "cottonseed": "00000328",  # Seed cotton
        "sesame": "00000289",  # Sesame seed
        # Item "Maize" has the description "[...] This class includes: -  maize harvested for their dry grains only"
        # So it's not clear whether it includes area used for maize oil, and therefore I won't consider it.
        # "maize": "00000056",  # Maize
        # Other vegetable oils not considered.
        # "safflower": "00000280",  # Safflower seed
        # "linseed": "00000333",  # Linseed
    }
    # Item codes in faostat_qcl for the production of the oils (there is no area harvested data for oils).
    ITEM_CODE_FOR_EACH_CROP_PRODUCTION = {
        # The item "Palm oil" doesn't have a description, but it probably refers to only the oil from the pulp of the
        # palm fruit (therefore it does not include the kernel).
        "palm": "00000257",  # Palm oil
        # The item "Palm kernel oil" clearly refers to only the oil produced from the kernel of the palm fruit.
        # Therefore, "Palm oil" and "Palm kernel oil" will need to be combined to account for all oils produced from
        # the palm fruit (item "Palm fruit oil" for which we have the area harvested).
        "palm_kernel": "00000258",  # Palm kernel oil
        "sunflower": "00000268",  # Sunflower oil
        "rapeseed": "00000271",  # Rapeseed oil
        "soybean": "00000237",  # Soybean oil
        "olive": "00000261",  # Olive oil
        "coconut": "00000252",  # Coconut oil
        "groundnut": "00000244",  # Groundnut oil
        "cottonseed": "00000331",  # Cottonseed oil
        "sesame": "00000290",  # Sesame oil
        # Item "maize" is not included (see comment above).
        # "maize": "00000060",  # Maize oil
        # Other vegetable oils not considered.
        # "safflower": "00000281",  # Safflower oil
        # "linseed": "00000334",  # Linseed oil
    }

    # Extract the total production of vegetable oil. This is given in fbsc but not qcl.
    total_production = tb_fbsc[
        (tb_fbsc["country"] == "World")
        & (tb_fbsc["item_code"] == ITEM_CODE_FOR_VEGETABLE_OILS_TOTAL)
        & (tb_fbsc["element_code"] == ELEMENT_CODE_FOR_PRODUCTION_FBSC)
        & (tb_fbsc["unit"] == UNIT_FOR_PRODUCTION)
    ].reset_index(drop=True)

    # Transpose data.
    total_production = (
        total_production.pivot(
            index=["country", "year"], columns=["item_code"], values=["value"], join_column_levels_with="_"
        )
        .rename(columns={"value_" + ITEM_CODE_FOR_VEGETABLE_OILS_TOTAL: "vegetable_oils_production"}, errors="raise")
        .drop(columns=["country"], errors="raise")
    )

    # Select relevant items, elements and units for the production of crops.
    production = tb_qcl[
        (tb_qcl["item_code"].isin(ITEM_CODE_FOR_EACH_CROP_PRODUCTION.values()))
        & (tb_qcl["unit"] == UNIT_FOR_PRODUCTION)
        & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_PRODUCTION_QCL)
    ].reset_index(drop=True)

    # Transpose data.
    production = production.pivot(
        index=["country", "year"], columns=["item_code"], values=["value"], join_column_levels_with="_"
    )
    production = production.rename(
        columns={
            column: column.replace("value_", "") for column in production.columns if column not in ["country", "year"]
        },
        errors="raise",
    )

    # Assign a convenient name to each crop.
    CROP_NAME_FOR_ITEM_CODE = {
        ITEM_CODE_FOR_EACH_CROP_PRODUCTION[item_code]: item_code for item_code in ITEM_CODE_FOR_EACH_CROP_PRODUCTION
    }
    production = production.rename(
        columns={
            item_code: CROP_NAME_FOR_ITEM_CODE[item_code] + "_production"
            for item_code in production.columns
            if item_code not in ["country", "year"]
        },
        errors="raise",
    )

    # Select relevant items, elements and units for the area of crops.
    area = tb_qcl[
        (tb_qcl["item_code"].isin(ITEM_CODE_FOR_EACH_CROP_AREA.values()))
        & (tb_qcl["unit"] == UNIT_FOR_AREA)
        & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_AREA)
    ].reset_index(drop=True)

    # Transpose data.
    area = area.pivot(index=["country", "year"], columns=["item_code"], values=["value"], join_column_levels_with="_")
    area = area.rename(
        columns={column: column.replace("value_", "") for column in area.columns if column not in ["country", "year"]},
        errors="raise",
    )

    # Assign a convenient name to each crop.
    CROP_NAME_FOR_ITEM_CODE = {
        ITEM_CODE_FOR_EACH_CROP_AREA[item_code]: item_code for item_code in ITEM_CODE_FOR_EACH_CROP_AREA
    }
    area = area.rename(
        columns={
            item_code: CROP_NAME_FOR_ITEM_CODE[item_code] + "_area"
            for item_code in area.columns
            if item_code not in ["country", "year"]
        },
        errors="raise",
    )

    # Combine production and area.
    combined = production.merge(area, on=["country", "year"], how="outer")

    # Add column for global vegetable oil production.
    combined = combined.merge(total_production, on=["year"], how="left")

    # Combine the production of palm oil and palm kernel oil, since we have the area harvested for the palm fruit
    # (which leads to the production of both palm oil and palm kernel oil).
    combined["palm_production"] += combined["palm_kernel_production"]
    combined = combined.drop(columns=["palm_kernel_production"], errors="raise")

    # For each crop, create three relevant metrics.
    for crop in ITEM_CODE_FOR_EACH_CROP_AREA:
        # Vegetable oil yield, which is the amount of oil produced per area harvested of the original crop.
        combined[f"{crop}_tonnes_per_hectare"] = combined[f"{crop}_production"] / combined[f"{crop}_area"]
        # Hectares of the original crop harvested per tonne of oil produced (inverse of the previous).
        combined[f"{crop}_hectares_per_tonne"] = combined[f"{crop}_area"] / combined[f"{crop}_production"]
        # Area required to produce the total demand of vegetable oils using only one specific crop.
        combined[f"{crop}_area_to_meet_global_oil_demand"] = (
            combined[f"{crop}_hectares_per_tonne"] * combined["vegetable_oils_production"]
        )

    # Replace infinite values (obtained when dividing by a null area) by nans.
    combined = combined.replace(np.inf, np.nan)

    # Set an appropriate index and sort conveniently.
    tb_vegetable_oil_yields = combined.format(["country", "year"], short_name="vegetable_oil_yields")

    return tb_vegetable_oil_yields


def generate_agriculture_land_evolution(tb_rl: Table) -> Table:
    # Element code for "Area".
    ELEMENT_CODE_FOR_AREA = "005110"
    # Unit for element of area.
    UNIT_FOR_AREA = "hectares"
    # Item code for "Land under perm. meadows and pastures".
    ITEM_CODE_FOR_PASTURES = "00006655"
    # Item code for "Cropland".
    ITEM_CODE_FOR_CROPLAND = "00006620"
    # Item code for "Agricultural land".
    ITEM_CODE_FOR_AGRICULTURAL_LAND = "00006610"

    # Select the relevant items, elements and units.
    land = tb_rl[
        (tb_rl["unit"] == UNIT_FOR_AREA)
        & (tb_rl["element_code"] == ELEMENT_CODE_FOR_AREA)
        & (tb_rl["item_code"].isin([ITEM_CODE_FOR_AGRICULTURAL_LAND, ITEM_CODE_FOR_CROPLAND, ITEM_CODE_FOR_PASTURES]))
    ].reset_index(drop=True)

    # Transpose data and rename columns conveniently.
    land = land.pivot(index=["country", "year"], columns=["item_code"], values="value", join_column_levels_with="_")
    land = land.rename(
        columns={
            ITEM_CODE_FOR_AGRICULTURAL_LAND: "agriculture_area",
            ITEM_CODE_FOR_CROPLAND: "cropland_area",
            ITEM_CODE_FOR_PASTURES: "pasture_area",
        },
        errors="raise",
    )

    # Add columns corresponding to the values of one decade before.
    _land = land.copy()
    _land["_year"] = _land["year"] + 10
    combined = land.merge(
        _land,
        left_on=["country", "year"],
        right_on=["country", "_year"],
        how="inner",
        suffixes=("", "_one_decade_back"),
    ).drop(columns=["_year"], errors="raise")

    # For each item, add the percentage change of land use this year with respect to one decade back.
    for item in ["agriculture_area", "cropland_area", "pasture_area"]:
        combined[f"{item}_change"] = (
            100 * (combined[f"{item}"] - combined[f"{item}_one_decade_back"]) / combined[f"{item}_one_decade_back"]
        )

    # To avoid warnings, copy metadata of "year" to the new column "year_one_decade_back".
    combined["year_one_decade_back"] = combined["year_one_decade_back"].copy_metadata(combined["year"])

    # Set an appropriate index and sort conveniently.
    tb_agriculture_land_use_evolution = combined.format(
        ["country", "year"], sort_columns=True, short_name="agriculture_land_use_evolution"
    )

    return tb_agriculture_land_use_evolution


def generate_hypothetical_meat_consumption(tb_fbsc: Table) -> Table:
    # Element code and unit for "Food per-capita".
    ELEMENT_CODE_FOR_FOOD_PER_CAPITA = "5142pc"
    UNIT_FOR_PRODUCTION_PER_CAPITA = "tonnes per capita"
    # Item code for "Meat, total".
    ITEM_CODE_FOR_MEAT_TOTAL = "00002943"

    # Select the required items/elements/units to get national data on per-capita consumption.
    meat = tb_fbsc[
        (tb_fbsc["item_code"] == ITEM_CODE_FOR_MEAT_TOTAL)
        & (tb_fbsc["element_code"].isin([ELEMENT_CODE_FOR_FOOD_PER_CAPITA]))
        & (tb_fbsc["unit"].isin([UNIT_FOR_PRODUCTION_PER_CAPITA]))
    ].reset_index(drop=True)
    # Extract global population.
    # NOTE: I checked that "population_with_data" for World coincides with the global population.
    global_population = (
        meat[meat["country"] == "World"]
        .rename(columns={"population_with_data": "global_population"}, errors="raise")[["year", "global_population"]]
        .reset_index(drop=True)
    )
    # Get food available for consumption for each country.
    meat = meat.pivot(index=["country", "year"], columns="element_code", values="value", join_column_levels_with="_")
    meat = meat.rename(
        columns={
            ELEMENT_CODE_FOR_FOOD_PER_CAPITA: "meat_per_capita",
        },
        errors="raise",
    )

    # Combine national with global data.
    combined = pr.multi_merge(tables=[meat, global_population], on=["year"], how="left")

    # Sanity check.
    error = "Rows have changed after merging national data with global data."
    assert len(combined) == len(meat), error

    # Add columns for hypothetical global meat consumption (more precisely, meat available for consumption).
    # This is a proxy for the amount of meat that would be needed worldwide to meet the demand of a given country.
    combined["meat_global_hypothetical"] = combined["meat_per_capita"] * combined["global_population"]

    # Set an appropriate index and sort conveniently.
    tb_hypothetical_meat_consumption = combined.format(
        ["country", "year"], sort_columns=True, short_name="hypothetical_meat_consumption"
    )

    ####################################################################################################################
    # TODO: I noticed a bug (https://github.com/owid/etl/issues/4241) in the way presentation.attribution is propagated.
    #  It causes that these specific variables show the attribution of the population dataset instead of FAOSTAT.
    #  So, assert that the issue is happening, and if so, remove that attribution.
    for column in tb_hypothetical_meat_consumption.columns:
        if tb_hypothetical_meat_consumption[column].metadata.presentation is not None:
            assert (
                tb_hypothetical_meat_consumption[column].metadata.presentation.attribution
                == "HYDE (2023); Gapminder (2022); UN WPP (2024)"
            )
            tb_hypothetical_meat_consumption[column].metadata.presentation.attribution = None
    ####################################################################################################################

    return tb_hypothetical_meat_consumption


def generate_hypothetical_animals_slaughtered(tb_qcl: Table) -> Table:
    # Element code and unit for "Producing or slaughtered animals".
    ELEMENT_CODE_FOR_ANIMALS = "005320"
    UNIT_FOR_ANIMALS = "animals"
    # Element code and unit for per-capita "Producing or slaughtered animals".
    ELEMENT_CODE_FOR_ANIMALS_PER_CAPITA = "5320pc"
    UNIT_FOR_ANIMALS_PER_CAPITA = "animals per capita"
    # Item code for "Meat, total".
    ITEM_CODE_FOR_MEAT_TOTAL = "00001765"

    # Select the required items/elements/units to get national data on per-capita slaughtered animals.
    animals = tb_qcl[
        (tb_qcl["item_code"] == ITEM_CODE_FOR_MEAT_TOTAL)
        & (tb_qcl["element_code"].isin([ELEMENT_CODE_FOR_ANIMALS_PER_CAPITA]))
        & (tb_qcl["unit"].isin([UNIT_FOR_ANIMALS_PER_CAPITA]))
    ].reset_index(drop=True)
    # Extract global population.
    # NOTE: I checked that "population_with_data" for World coincides with the global population.
    global_population = (
        animals[animals["country"] == "World"]
        .rename(columns={"population_with_data": "global_population"}, errors="raise")[["year", "global_population"]]
        .reset_index(drop=True)
    )
    # Get number of slaughtered animals per person for each country.
    animals = animals.pivot(
        index=["country", "year"], columns="element_code", values="value", join_column_levels_with="_"
    )
    animals = animals.rename(
        columns={
            ELEMENT_CODE_FOR_ANIMALS_PER_CAPITA: "animals_per_capita",
        },
        errors="raise",
    )

    global_animals = (
        tb_qcl[
            (tb_qcl["country"] == "World")
            & (tb_qcl["element_code"] == ELEMENT_CODE_FOR_ANIMALS)
            & (tb_qcl["unit"] == UNIT_FOR_ANIMALS)
            & (tb_qcl["item_code"] == ITEM_CODE_FOR_MEAT_TOTAL)
        ][["year", "value"]]
        .reset_index(drop=True)
        .rename(columns={"value": "animals_global"}, errors="raise")
    )

    # Combine national with global data.
    combined = pr.multi_merge(tables=[animals, global_population, global_animals], on=["year"], how="left")

    # Sanity check.
    error = "Rows have changed after merging national data with global data."
    assert len(combined) == len(animals), error

    # Add columns for hypothetical global number of slaughtered animals.
    # This is the number of slaughtered animals that would be needed worldwide if the number of slaughtered animals per person in the world was the same as in a given country.
    combined["animals_global_hypothetical"] = combined["animals_per_capita"] * combined["global_population"]

    # Set an appropriate index and sort conveniently.
    tb_hypothetical_animals_slaughtered = combined.format(
        ["country", "year"], sort_columns=True, short_name="hypothetical_animals_slaughtered"
    )

    ####################################################################################################################
    # TODO: I noticed a bug (https://github.com/owid/etl/issues/4241) in the way presentation.attribution is propagated.
    #  It causes that these specific variables show the attribution of the population dataset instead of FAOSTAT.
    #  So, assert that the issue is happening, and if so, remove that attribution.
    for column in tb_hypothetical_animals_slaughtered.columns:
        if tb_hypothetical_animals_slaughtered[column].metadata.presentation is not None:
            assert (
                tb_hypothetical_animals_slaughtered[column].metadata.presentation.attribution
                == "HYDE (2023); Gapminder (2022); UN WPP (2024)"
            )
            tb_hypothetical_animals_slaughtered[column].metadata.presentation.attribution = None
    ####################################################################################################################

    return tb_hypothetical_animals_slaughtered


def generate_cereal_allocation(tb_fbsc: Table) -> Table:
    # Item code for "Cereals - Excluding Beer".
    ITEM_CODE_FOR_CEREALS = "00002905"
    # Note: We disregard the contribution from "00002520" ("Cereals, Other"), which is usually negligible compared to the total.
    # Element code and unit for "Food".
    # Note: The element code for "Food available for consumption" is "000645"; this should be the same data, except that
    #  it is given in kilograms (originally it was given per capita). Therefore, we use "Food", which is more convenient.
    ELEMENT_CODE_FOR_FOOD = "005142"
    UNIT_FOR_FOOD = "tonnes"
    # Element code and unit for "Feed".
    ELEMENT_CODE_FOR_FEED = "005521"
    UNIT_FOR_FEED = "tonnes"
    # Element code and unit for "Other uses".
    ELEMENT_CODE_FOR_OTHER_USES = "005154"
    UNIT_FOR_OTHER_USES = "tonnes"

    # Select the relevant items/elements.
    cereals = tb_fbsc[
        (tb_fbsc["item_code"] == ITEM_CODE_FOR_CEREALS)
        & (tb_fbsc["element_code"].isin([ELEMENT_CODE_FOR_FOOD, ELEMENT_CODE_FOR_FEED, ELEMENT_CODE_FOR_OTHER_USES]))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units have changed"
    assert set(cereals["unit"]) == set([UNIT_FOR_FOOD, UNIT_FOR_FEED, UNIT_FOR_OTHER_USES]), error

    # Transpose data and rename columns conveniently.
    cereals = cereals.pivot(
        index=["country", "year"], columns="element_code", values="value", join_column_levels_with="_"
    ).rename(
        columns={
            ELEMENT_CODE_FOR_FOOD: "cereals_allocated_to_food",
            ELEMENT_CODE_FOR_FEED: "cereals_allocated_to_animal_feed",
            ELEMENT_CODE_FOR_OTHER_USES: "cereals_allocated_to_other_uses",
        },
        errors="raise",
    )

    # Add variables for the share of cereals allocated to each use.
    all_cereal_uses = ["food", "animal_feed", "other_uses"]
    for item in all_cereal_uses:
        cereals[f"share_of_cereals_allocated_to_{item}"] = (
            100
            * cereals[f"cereals_allocated_to_{item}"]
            / cereals[[f"cereals_allocated_to_{use}" for use in all_cereal_uses]].sum(axis=1)
        )

    # Set an appropriate index and sort conveniently.
    tb_cereal_allocation = cereals.format(["country", "year"], sort_columns=True, short_name="cereal_allocation")

    return tb_cereal_allocation


def generate_maize_and_wheat(tb_fbsc: Table) -> Table:
    # Item code for "Wheat".
    ITEM_CODE_FOR_WHEAT = "00002511"
    # Item code for "Maize".
    ITEM_CODE_FOR_MAIZE = "00002514"
    # Element code for "Exports".
    ELEMENT_CODE_FOR_EXPORTS = "005911"
    # Element code for "Feed".
    ELEMENT_CODE_FOR_FEED = "005521"
    # Element code for "Other uses".
    ELEMENT_CODE_FOR_OTHER_USES = "005154"

    # Select the relevant items/elements.
    maize_and_wheat = tb_fbsc[
        (tb_fbsc["item_code"].isin([ITEM_CODE_FOR_MAIZE, ITEM_CODE_FOR_WHEAT]))
        & (tb_fbsc["element_code"].isin([ELEMENT_CODE_FOR_EXPORTS, ELEMENT_CODE_FOR_FEED, ELEMENT_CODE_FOR_OTHER_USES]))
    ]

    # Sanity check.
    error = "Units have changed."
    assert list(maize_and_wheat["unit"].unique()) == ["tonnes"], error

    # Transpose data and rename columns conveniently.
    maize_and_wheat = maize_and_wheat.pivot(
        index=["country", "year"], columns=["item_code", "element_code"], values="value", join_column_levels_with="_"
    ).rename(
        columns={
            f"{ITEM_CODE_FOR_MAIZE}_{ELEMENT_CODE_FOR_EXPORTS}": "maize_exports",
            f"{ITEM_CODE_FOR_MAIZE}_{ELEMENT_CODE_FOR_FEED}": "maize_animal_feed",
            f"{ITEM_CODE_FOR_MAIZE}_{ELEMENT_CODE_FOR_OTHER_USES}": "maize_other_uses",
            f"{ITEM_CODE_FOR_WHEAT}_{ELEMENT_CODE_FOR_EXPORTS}": "wheat_exports",
            f"{ITEM_CODE_FOR_WHEAT}_{ELEMENT_CODE_FOR_FEED}": "wheat_animal_feed",
            f"{ITEM_CODE_FOR_WHEAT}_{ELEMENT_CODE_FOR_OTHER_USES}": "wheat_other_uses",
        },
        errors="raise",
    )

    # Set an appropriate index and sort conveniently.
    tb_maize_and_wheat = maize_and_wheat.format(["country", "year"], sort_columns=True, short_name="maize_and_wheat")

    # Add minimal variable metadata (more metadata will be added at the grapher step).
    for column in tb_maize_and_wheat.columns:
        tb_maize_and_wheat[column].metadata.unit = "tonnes"
        tb_maize_and_wheat[column].metadata.short_unit = "t"

    return tb_maize_and_wheat


def generate_fertilizer_exports(tb_rfn: Table) -> Table:
    # Element code for "Export Quantity".
    ELEMENT_CODE_FOR_EXPORTS = "005910"
    # Item code for "Nutrient nitrogen N (total)".
    ITEM_CODE_FOR_NITROGEN = "00003102"
    # Item code for "Nutrient phosphate P2O5 (total)".
    ITEM_CODE_FOR_PHOSPHATE = "00003103"
    # Item code for "Nutrient potash K2O (total)".
    ITEM_CODE_FOR_POTASH = "00003104"

    # Select the relevant items and elements.
    fertilizer_exports = tb_rfn[
        (tb_rfn["element_code"] == ELEMENT_CODE_FOR_EXPORTS)
        & (tb_rfn["item_code"].isin([ITEM_CODE_FOR_NITROGEN, ITEM_CODE_FOR_PHOSPHATE, ITEM_CODE_FOR_POTASH]))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units have changed."
    assert list(fertilizer_exports["unit"].unique()) == ["Tonnes"], error

    # Rename columns and items conveniently.
    fertilizer_exports = fertilizer_exports[["country", "year", "item_code", "value"]].rename(
        columns={"item_code": "item", "value": "exports"}, errors="raise"
    )
    fertilizer_exports["item"] = fertilizer_exports["item"].replace(
        {ITEM_CODE_FOR_NITROGEN: "Nitrogen", ITEM_CODE_FOR_PHOSPHATE: "Phosphorous", ITEM_CODE_FOR_POTASH: "Potassium"}
    )

    # Add column of global exports.
    global_exports = (
        fertilizer_exports[fertilizer_exports["country"] == "World"].drop(columns=["country"]).reset_index(drop=True)
    )
    fertilizer_exports = fertilizer_exports.merge(
        global_exports, how="left", on=["year", "item"], suffixes=("", "_global")
    )

    # Create columns for the share of exports.
    fertilizer_exports["share_of_exports"] = 100 * fertilizer_exports["exports"] / fertilizer_exports["exports_global"]

    # Drop column of global exports.
    fertilizer_exports = fertilizer_exports.drop(columns=["exports_global"], errors="raise")

    # Set an appropriate index and sort conveniently.
    tb_fertilizer_exports = fertilizer_exports.format(
        ["country", "year", "item"], sort_columns=True, short_name="fertilizer_exports"
    )

    # Add minimal variable metadata (more metadata will be added at the grapher step).
    tb_fertilizer_exports["share_of_exports"].metadata.unit = "%"
    tb_fertilizer_exports["share_of_exports"].metadata.short_unit = "%"
    tb_fertilizer_exports["exports"].metadata.unit = "tonnes"
    tb_fertilizer_exports["exports"].metadata.short_unit = "t"

    return tb_fertilizer_exports


def generate_net_exports_as_share_of_supply(tb_fbsc: Table) -> Table:
    # I want to create a new indicator for the net trade balance as a share of consumption (or rather, domestic supply).
    # In other words, I want to calculate (Exports - Imports) / Domestic supply.
    # Here, note that we don't use "Food", since imports and exports include all agricultural products (including e.g. feed), whereas "Food" includes only food allocated for human consumption.
    # "Domestic supply" is the total supply of an item (including food, feed, and other uses) available for consumption.
    # However, I want to have this indicator for a global total, not for each item.
    # There is a grand total in the data, but only for "Fat supply quantity (t)", "Food available for consumption", "Food supply (kcal)", and "Protein supply quantity (t)".
    # We would need to create this total for Imports, Exports, and Domestic supply.
    # To do that, I can simply sum those elements over all items in FOOD_GROUPS.

    # Element code for "Exports".
    ELEMENT_CODE_FOR_EXPORTS = "005911"
    # Element code for "Imports".
    ELEMENT_CODE_FOR_IMPORTS = "005611"
    # Element code for "Domestic supply quantity".
    ELEMENT_CODE_FOR_DOMESTIC_SUPPLY = "005301"
    # Gather the items that make up all foods.
    all_items = sum(FOOD_GROUPS_FBSC.values(), [])

    # Select the relevant items/elements.
    tb = tb_fbsc[
        (tb_fbsc["item_code"].isin(all_items))
        & (
            tb_fbsc["element_code"].isin(
                [ELEMENT_CODE_FOR_EXPORTS, ELEMENT_CODE_FOR_IMPORTS, ELEMENT_CODE_FOR_DOMESTIC_SUPPLY]
            )
        )
    ][["country", "year", "item", "element", "value", "unit"]].reset_index(drop=True)

    # Sanity check.
    error = "Units have changed."
    assert list(tb["unit"].unique()) == ["tonnes"], error
    tb = tb.drop(columns="unit", errors="raise")

    # Visually inspect how many item groups are informed for each element.
    # tb.groupby(["element", "item"], observed=True, as_index=False).size().sort_values(["item", "element"])
    # I see that, for all item groups, there is roughly a similar number of imports, exports, and food.
    # It is possible that supply is better informed that imports and exports, but if so, it's not by a significant percentage.

    # Add up the total of imports, exports and food for each country and year.
    tb = tb.groupby(["country", "year", "element"], observed=True, as_index=False).agg({"value": "sum"})

    # Transpose data and rename columns conveniently.
    tb = tb.pivot(index=["country", "year"], columns="element", values="value", join_column_levels_with="_")

    # Create a new column for food trade balance relative to domestic supply, defined as net exports as a share of domestic supply.
    tb["net_exports_as_share_of_supply"] = 100 * (tb["Exports"] - tb["Imports"]) / tb["Domestic supply"]

    # Remove unnecessary columns.
    tb = tb.drop(columns=["Exports", "Imports", "Domestic supply"], errors="raise")

    # Improve table format.
    tb = tb.format(short_name="net_exports_as_share_of_supply")

    return tb


def generate_milk_per_animal(tb_qcl: Table) -> Table:
    # FAOSTAT QCL used to have yield for milk (milk per animal), but it was removed at some point (that combination of item and element is empty in the 2025 release).
    # Numerically, it seems that their old milk yield indicator coincides with production / number of animals.
    # So I will reproduce that indicator using their latest data on production and number of milk animals.

    # Firstly, check if yield is still missing in the data.
    error = "Yield for milk is not missing in the data anymore. Consider reusing it instead of calculating it."
    assert tb_qcl[(tb_qcl["item_code"] == "00001780") & (tb_qcl["element_code"] == "005420")].empty, error

    # Element code for "Production".
    ELEMENT_CODE_FOR_PRODUCTION = "005510"
    # Unit for element of production.
    UNIT_FOR_PRODUCTION = "tonnes"
    # Element code for "Milk animals".
    ELEMENT_CODE_FOR_MILK_ANIMALS = "005318"
    # Unit for element of milk animals.
    UNIT_FOR_ANIMALS = "animals"
    # Item code for "Milk".
    ITEM_CODE_FOR_MILK = "00001780"

    # Select the relevant items/elements/units.
    tb_milk = tb_qcl[
        (tb_qcl["item_code"] == ITEM_CODE_FOR_MILK)
        & (tb_qcl["element_code"].isin([ELEMENT_CODE_FOR_PRODUCTION, ELEMENT_CODE_FOR_MILK_ANIMALS]))
    ].reset_index(drop=True)

    # Sanity check.
    error = "Units of milk production or milk animals have changed."
    assert set(tb_milk["unit"]) == {UNIT_FOR_PRODUCTION, UNIT_FOR_ANIMALS}, error

    # Transpose data and rename columns conveniently.
    tb_milk = tb_milk.pivot(
        index=["country", "year"], columns="element_code", values="value", join_column_levels_with="_"
    ).rename(
        columns={
            ELEMENT_CODE_FOR_PRODUCTION: "milk_production",
            ELEMENT_CODE_FOR_MILK_ANIMALS: "animals_used_for_milk",
        },
        errors="raise",
    )

    # Add column for milk production per animal.
    # NOTE: We change the units from tonnes to kg.
    tb_milk["milk_per_animal"] = 1000 * tb_milk["milk_production"] / tb_milk["animals_used_for_milk"]

    # Sanity checks.
    error = "Unexpected infinite values in the calculation of milk per animal."
    assert not np.isinf(tb_milk["milk_per_animal"]).any(), error
    error = "Yield per animal is unexpectedly high."
    assert (tb_milk["milk_per_animal"] <= 15000).all(), error

    # Improve table format.
    tb_milk_per_animal = tb_milk.format(["country", "year"], short_name="milk_per_animal")

    return tb_milk_per_animal


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset about land use and load its main (long-format) table.
    ds_rl = paths.load_dataset("faostat_rl")
    tb_rl = ds_rl.read("faostat_rl")

    # Load dataset about production indices and load its main (long-format) table.
    ds_qi = paths.load_dataset("faostat_qi")
    tb_qi = ds_qi.read("faostat_qi")

    # Load dataset about crops and livestock and load its main (long-format) table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl")

    # Load dataset about SDG indicators and load its main (long-format) table.
    ds_sdgb = paths.load_dataset("faostat_sdgb")
    tb_sdgb = ds_sdgb.read("faostat_sdgb")

    # Load dataset about food balances and load its main (long-format) table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load dataset about fertilizers by nutrient and load its main (long-format) table.
    ds_rfn = paths.load_dataset("faostat_rfn")
    tb_rfn = ds_rfn.read("faostat_rfn")

    #
    # Process data.
    #
    # Create table for arable land per crop output.
    tb_arable_land_per_crop_output = generate_arable_land_per_crop_output(tb_rl=tb_rl, tb_qi=tb_qi)

    # Create table for area used for production per crop type.
    tb_area_by_crop_type = generate_area_used_for_production_per_crop_type(tb_qcl=tb_qcl)

    # Create table for the share of sustainable and overexploited fish.
    tb_sustainable_and_overexploited_fish = generate_percentage_of_sustainable_and_overexploited_fish(tb_sdgb=tb_sdgb)

    # Create table for spared land due to increased yields.
    tb_spared_land_from_increased_yields = generate_spared_land_from_increased_yields(tb_qcl=tb_qcl)

    # Create table for dietary compositions by commodity group.
    tb_food_available_for_consumption = generate_food_available_for_consumption(tb_fbsc=tb_fbsc)

    # Create table for macronutrient compositions.
    tb_macronutrient_compositions = generate_macronutrient_compositions(tb_fbsc=tb_fbsc)

    # Create table for fertilizers data.
    tb_fertilizers = generate_fertilizers(tb_rfn=tb_rfn, tb_rl=tb_rl)

    # Create table for vegetable oil yields.
    tb_vegetable_oil_yields = generate_vegetable_oil_yields(tb_qcl=tb_qcl, tb_fbsc=tb_fbsc)

    # Create table for peak agricultural land.
    tb_agriculture_land_use_evolution = generate_agriculture_land_evolution(tb_rl=tb_rl)

    # Create table for hypothetical meat consumption.
    tb_hypothetical_meat_consumption = generate_hypothetical_meat_consumption(tb_fbsc=tb_fbsc)

    # Create table for hypothetical number of slaughtered animals.
    tb_hypothetical_animals_slaughtered = generate_hypothetical_animals_slaughtered(tb_qcl=tb_qcl)

    # Create table for cereal allocation.
    tb_cereal_allocation = generate_cereal_allocation(tb_fbsc=tb_fbsc)

    # Create table for maize and wheat data (used in the context of the Ukraine war).
    tb_maize_and_wheat = generate_maize_and_wheat(tb_fbsc=tb_fbsc)

    # Create table for fertilizer exports (used in the context of the Ukraine war).
    tb_fertilizer_exports = generate_fertilizer_exports(tb_rfn=tb_rfn)

    # Create table for food trade as a share of consumption.
    tb_net_exports_as_share_of_supply = generate_net_exports_as_share_of_supply(tb_fbsc=tb_fbsc)

    # Create atable for milk production per animal.
    tb_milk_per_animal = generate_milk_per_animal(tb_qcl=tb_qcl)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[
            tb_arable_land_per_crop_output,
            tb_area_by_crop_type,
            tb_sustainable_and_overexploited_fish,
            tb_spared_land_from_increased_yields,
            tb_food_available_for_consumption,
            tb_macronutrient_compositions,
            tb_fertilizers,
            tb_vegetable_oil_yields,
            tb_agriculture_land_use_evolution,
            tb_hypothetical_meat_consumption,
            tb_hypothetical_animals_slaughtered,
            tb_cereal_allocation,
            tb_maize_and_wheat,
            tb_fertilizer_exports,
            tb_net_exports_as_share_of_supply,
            tb_milk_per_animal,
        ],
    )
    ds_garden.save()
