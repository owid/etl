"""TODO: Properly describe once it's finished. And move this explanation to the gdoc."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns from food balances dataset.
ELEMENT_CODES = [
    # "Food available for consumption", in kcal/capita/day. It was converted from the original "Food supply (kcal/capita/day)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0664pc",
    # "Food available for consumption", in kg/capita/year. It was converted from the original "Food supply quantity (kg/capita/yr)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0645pc",
    # "Production", in tonnes.
    "005511",
]

# List of food groups (copied from the garden additional_variables step) created by OWID for FBSC (combination of FBS and FBSH).
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


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    # NOTE: It may be necessary to load the meadow FBSH and FBS datasets. For now, try with FBSC.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # # Load regions dataset, and read its main table.
    # ds_regions = paths.load_dataset("regions")
    # tb_regions = ds_regions.read("regions")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Select relevant elements.
    tb = tb_fbsc[(tb_fbsc["element_code"].isin(ELEMENT_CODES))].reset_index(drop=True)

    # Sanity check.
    check = tb[
        [
            "element_code",
            "fao_element",
            "element",
            "element_description",
            "unit",
            "fao_unit_short_name",
            "unit_short_name",
        ]
    ].drop_duplicates()
    assert set(check["element"]) == {"Food available for consumption", "Production"}
    assert set(check["unit"]) == {"kilograms per year per capita", "kilocalories per day per capita", "tonnes"}

    # Convert kcal/capita/day to kcal/capita/year.
    tb.loc[(tb["element_code"] == "0664pc"), "value"] *= 365

    # Pivot to have kcal and weight in separate columns.
    tb = tb.pivot(
        index=["country", "year", "item", "item_code", "item_description"],
        columns=["element_code"],
        values=["value"],
        join_column_levels_with="_",
    ).rename(columns={"value_0645pc": "food_quantity", "value_0664pc": "food_energy", "value_005511": "production"})

    # For sanity checks later on, keep the "Total" item of food.
    tb_total = tb[tb["item_code"] == "00002901"].reset_index(drop=True)
    error = "'Total' item code or name has changed."
    assert set(tb_total["item"]) == {"Total"}, error
    error = "Expected 'Total' item to be only available for per capita food (kcal)."
    assert tb_total[(tb_total["production"].notnull()) | (tb_total["food_quantity"].notnull())].empty, error
    tb_total = tb_total.drop(columns=["production", "food_quantity"])

    # Remove rows with no production data.
    n_rows_before = len(tb)
    tb = tb.dropna(subset="production").reset_index(drop=True)
    error = "Expected the number of rows to decrease by less than 30% after dropping nans in production."
    assert 100 * (n_rows_before - len(tb)) / n_rows_before < 30, error

    # Sometimes quantity is missing, but calories is informed, and sometimes is the opposite.
    # Check that either way those are edge cases, and then remove those rows.
    error = "Unexpected number of nans in food quantity or food energy."
    assert 100 * len(tb[(tb["food_quantity"].isnull()) & (tb["food_energy"].notnull())]) / len(tb) < 0.1, error
    assert 100 * len(tb[(tb["food_quantity"].notnull()) & (tb["food_energy"].isnull())]) / len(tb) < 0.1, error
    tb = tb.dropna(subset=["food_quantity", "food_energy"], how="any").reset_index(drop=True)

    # Calculate conversion factors, in kcal per 100g.
    tb["conversion"] = (tb["food_energy"] / (tb["food_quantity"] * 10)).fillna(0)
    # Remove spurious conversion factors (which may happen when dividing by very small quantities); assume a maximum conversion factor of 1000 kcal per 100g.
    error = "Unexpected number of rows where conversion factor is zero, but production is not."
    assert 100 * len(tb[(tb["conversion"] == 0) & (tb["production"] > 0)]) / len(tb) < 3, error
    error = "Unexpected number of rows where conversion factor is unreasonably high."
    assert 100 * len(tb[(tb["conversion"] > 1000)]) / len(tb) < 2, error
    # Remove all rows where conversion is zero or unreasonably high.
    tb = tb[(tb["conversion"] > 0) & (tb["conversion"] < 1000)].reset_index(drop=True)
    # px.histogram(tb, x="conversion", title=f"All items (all years and countries)", labels={"conversion": f"Conversion  / kcal per 100g"}, nbins=500, histnorm="percent").show()

    # Apply conversion factor (of kcal per 100g) to production (in tonnes).
    tb["production_energy"] = tb["production"] * 10000 * tb["conversion"]
    # Sanity checks.
    error = "Unexpected zero or nan production energy."
    assert tb[(tb["production_energy"] == 0) & (tb["production"] != 0)].empty, error
    assert tb[(tb["production_energy"].isnull())].empty, error

    # FBS items are grouped together, and the proportions of subitems in each group may differ for different items, then it may be impossible (or quite inaccurate) to translate those groups into energy with a simple conversion factor.
    # for item in ["Wheat", "Apples"]:
    #     px.histogram(tb[tb["item"] == item], x="conversion", title=f"{item} (all years and countries)", labels={"conversion": f"Conversion  / kcal per 100g"}, nbins=500, histnorm="percent", range_x=(0, 1000)).show()
    # So, it seems that FBS has grouped items, e.g. Wheat actually means:
    # 'Default composition: 15 Wheat, 16 Flour, wheat, 17 Bran, wheat, 18 Macaroni, 19 Germ, wheat, 20 Bread, 21 Bulgur, 22 Pastry, 23 Starch, wheat, 24 Gluten, wheat, 41 Cereals, breakfast, 110 Wafers, 114 Mixes and doughs, 115 Food preparations, flour, malt extract'
    # So, the resulting food energy would be a weighted average of all those products (across all years and countries). Maybe, once we combine those items in the right proportion, the conversion factor would follow the resulting distribution above. For example, bread is at 249 kcal. So, maybe the first peak in the histogram (around 260 kcal) could be due to the abundance of bread (shifted up by other items).

    # Convert food quantity and food energy from per capita to totals.
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)
    for column in ["food_quantity", "food_energy"]:
        tb[column] *= tb["population"]

    # Idem for the original "Total" of food (kcal).
    tb_total = geo.add_population_to_table(tb=tb_total, ds_population=ds_population, warn_on_missing_countries=False)
    tb_total["food_energy"] *= tb_total["population"]

    # Get the total from the already aggregated item groups.
    # NOTE: It might be better to get the sum of their subitems (and compare with the item group sum totals).
    ITEM_CODES = sorted(set(sum([items for _, items in FOOD_GROUPS_FBSC.items()], [])))
    tb_grouped = (
        tb[tb["item_code"].isin(ITEM_CODES)]
        .groupby(["country", "year"], as_index=False)
        .agg({column: "sum" for column in ["food_quantity", "food_energy", "production", "production_energy"]})
        .assign(**{"item": "Total"})
    )

    # Sanity check.
    tb_check = tb_grouped[["country", "year", "food_energy"]].merge(
        tb_total[["country", "year", "food_energy"]].rename(columns={"food_energy": "food_energy_original"})
    )
    tb_check["pct"] = (
        100 * abs(tb_check["food_energy"] - tb_check["food_energy_original"]) / tb_check["food_energy_original"]
    )
    error = "Calculated total food energy (kcal) differs from original by more than expected."
    assert tb_check["pct"].mean() < 4, error
    # The only cases where the percentage difference is larger than 50% are for a short list of small countries.
    assert set(tb_check[tb_check["pct"] > 50]["country"]) == {
        "Bahrain",
        "Bermuda",
        "Kiribati",
        "Macao",
        "Nauru",
        "Netherlands Antilles",
        "Saint Kitts and Nevis",
        "Seychelles",
    }

    ####################################################################################################################
    # TODO: Consider moving elsewhere, or otherwise keeping it inside a sanity check function.
    # Compare my estimated totals for production energy with those from the Hong et al. (2021) paper, in terms of production calories.
    # Compare my estimates on caloric supply (from FBSC correcting for historical changes and data availability changes) with the data from https://figshare.com/articles/dataset/Global_and_regional_drivers_of_land-use_emissions_in_1961-2017/12248735?file=26174975
    # Specifically tab "8.1.AgProd".
    from pathlib import Path

    import pandas as pd

    from etl.paths import STEP_DIR

    df = pd.read_excel(Path.home() / "Downloads/LUE_Data_CALUE.xlsx", sheet_name="8.1.AgProd")
    df = df.rename(columns={column: column.replace("Area", "country").replace("Y", "") for column in df.columns})
    df = df.melt(id_vars=["country"], var_name="year", value_name="production_energy_hong")
    df["year"] = df["year"].astype(int)
    countries_file = STEP_DIR / f"data/garden/faostat/{ds_fbsc.metadata.version}/faostat.countries.json"
    excluded_countries_file = (
        STEP_DIR / f"data/garden/faostat/{ds_fbsc.metadata.version}/faostat.excluded_countries.json"
    )
    df = geo.harmonize_countries(
        df,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )
    # Missing mappings:
    from owid.datautils.dataframes import map_series

    df["country"] = map_series(
        df["country"],
        mapping={
            "China- mainland": "China",
            "China- Macao SAR": "Macao",
            "Turkey": "Turkey",
            "United Kingdom": "United Kingdom",
            "Netherlands": "Netherlands",
            "Saint Helena- Ascension and Tristan da Cunha": "Saint Helena",
            "China- Taiwan Province of": "Taiwan",
            "China- Hong Kong SAR": "Hong Kong",
        },
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
    )
    check = pd.DataFrame(tb_grouped)[["country", "year", "production_energy"]].merge(
        df, how="inner", on=["country", "year"]
    )
    check["pct"] = (
        100 * abs(check["production_energy"] - check["production_energy_hong"]) / check["production_energy_hong"]
    )
    error = "Expected percentage difference between our production energy and that from Hong et al. (2021) to agree within ~10%"
    assert check["pct"].median() < 11, error
    # value_name = "production of all food items / kcal"
    # plot = (
    #     check[["country", "year", "production_energy", "production_energy_hong"]]
    #     .rename(columns={"production_energy": "OWID", "production_energy_hong": "Hong et al. (2021)"})
    #     .melt(id_vars=["country", "year"], value_name=value_name)
    # )
    # for country in sorted(set(check["country"])):
    #     _plot = plot[plot["country"] == country]
    #     px.line(_plot, x="year", y=value_name, color="variable", markers=True, title=country, range_y=(0, _plot[value_name].max() * 1.05)).show()
    # Most countries agree reasonably well (better than expected, given that FAOSTAT has probably changed since the publication of Hong et al. in 2021).
    # Cases with significant discrepancies Indonesia, Hong-Kong, or Malaysia. And one where the difference is particularly significant is Iceland.

    ####################################################################################################################

    # TODO: Consider adding the output of this step to the long_term_food_and_agriculture_trends step.

    # Improve table format.
    tb = tb.format(keys=["country", "year", "item"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
