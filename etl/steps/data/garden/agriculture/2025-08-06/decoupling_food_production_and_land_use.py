"""Create a dataset with indicators on food production (in kilocalories) and agricultural land use (in hectares).

The goal is to create a visualization showing which countries have managed to decouple food production and land use.
"""

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


def sanity_check_compare_with_hong_et_al(tb_grouped):
    # Compare my estimated totals for production energy with those from the Hong et al. (2021) paper, in terms of production calories and land use.
    from pathlib import Path

    import pandas as pd
    import plotly.express as px
    from owid.datautils.dataframes import map_series

    from etl.paths import STEP_DIR

    # Extract the version of the fbsc step from the dependency uri.
    fbsc_version = [step.split("/")[4] for step in paths.dependencies if "faostat_fbsc" in step][0]

    # Load data from a local file. The file can be downloaded from:
    # https://figshare.com/articles/dataset/Global_and_regional_drivers_of_land-use_emissions_in_1961-2017/12248735?file=26174975
    # Specifically tab "8.1.AgProd" for agricultural production, and "9.1.AgLand" for agricultural land use.
    df_food = pd.read_excel(Path.home() / "Downloads/LUE_Data_CALUE.xlsx", sheet_name="8.1.AgProd")
    df_land = pd.read_excel(Path.home() / "Downloads/LUE_Data_CALUE.xlsx", sheet_name="9.1.AgLand")
    # Reformat and combine sheets.
    df_food = df_food.rename(
        columns={column: column.replace("Area", "country").replace("Y", "") for column in df_food.columns}
    ).melt(id_vars=["country"], var_name="year", value_name="production_energy_hong")
    df_land = df_land.rename(
        columns={column: column.replace("Area", "country").replace("Y", "") for column in df_land.columns}
    ).melt(id_vars=["country"], var_name="year", value_name="agricultural_land_hong")
    df = df_food.merge(df_land, on=["country", "year"], how="outer")
    df["year"] = df["year"].astype(int)

    # Harmonize country names using the same file as in the FAOSTAT steps.
    countries_file = STEP_DIR / f"data/garden/faostat/{fbsc_version}/faostat.countries.json"
    excluded_countries_file = STEP_DIR / f"data/garden/faostat/{fbsc_version}/faostat.excluded_countries.json"
    df = geo.harmonize_countries(
        df,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )
    # Missing mappings:
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
    check = pd.DataFrame(tb_grouped)[["country", "year", "production_energy", "agricultural_land"]].merge(
        df, how="inner", on=["country", "year"]
    )
    check["pct_food"] = (
        100 * abs(check["production_energy"] - check["production_energy_hong"]) / check["production_energy_hong"]
    )
    check["pct_land"] = (
        100 * abs(check["agricultural_land"] - check["agricultural_land_hong"]) / check["agricultural_land_hong"]
    )
    error = "Expected percentage difference between our production energy and that from Hong et al. (2021) to agree within ~10%"
    assert check["pct_food"].median() < 11, error
    error = "Expected percentage difference between our agricultural land and that from Hong et al. (2021) to agree within ~14%"
    assert check["pct_land"].median() < 14, error
    # Choose to plot food production or land use.
    variable = "production_energy"
    # variable = "agricultural_land"
    value_name = f"{variable.replace('_', ' ')} of all food items / kcal"
    plot = (
        check[["country", "year", variable, f"{variable}_hong"]]
        .rename(columns={variable: "OWID", f"{variable}_hong": "Hong et al. (2021)"})
        .melt(id_vars=["country", "year"], value_name=value_name)
    )
    for country in sorted(set(check["country"])):
        _plot = plot[plot["country"] == country]
        px.line(
            _plot,
            x="year",
            y=value_name,
            color="variable",
            markers=True,
            title=country,
            range_y=(0, _plot[value_name].max() * 1.05),
        ).show()
    # In terms of food production, most countries agree reasonably well with Hong et al (2021); in fact better than expected, given that FAOSTAT has probably changed since the publication of that paper.
    # Cases with significant discrepancies Indonesia, Hong-Kong, or Malaysia. And one where the difference is particularly significant is Iceland.
    # Land use differs significantly more, but I suppose that's also mostly due to changes in FAOSTAT RL data.


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load FAOSTAT land use dataset, and read its main table.
    ds_rl = paths.load_dataset("faostat_rl")
    tb_rl = ds_rl.read("faostat_rl")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Select relevant elements from land use data.
    # Item code "Agricultural land" (00006610).
    # Element code "Area" (005110) in hectares.
    tb_rl = tb_rl[(tb_rl["element_code"] == "005110") & (tb_rl["item_code"] == "00006610")].reset_index(drop=True)
    error = "Units of area have changed."
    assert set(tb_rl["unit"]) == {"hectares"}, error
    tb_rl = tb_rl[["country", "year", "value"]].rename(columns={"value": "agricultural_land"}, errors="raise")

    # Select relevant elements of food production.
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
    )
    # For convenience, add population again to this table.
    tb_grouped = geo.add_population_to_table(
        tb=tb_grouped, ds_population=ds_population, warn_on_missing_countries=False
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

    # Combine data on total food production with agricultural land.
    _n_rows_before = len(tb_grouped)
    tb_grouped = tb_grouped.merge(tb_rl, how="inner", on=["country", "year"])
    error = "Unexpected number of rows lost when merging production and land use."
    assert 100 * (_n_rows_before - len(tb_grouped)) / _n_rows_before < 0.5, error

    # Uncomment to compare the resulting production energy content with the estimate from the Hong et al. (2021) paper.
    # Overall, the estimates on total production (in kcal) per country agree very well (except for a few countries, more significantly Iceland).
    # sanity_check_compare_with_hong_et_al(tb_grouped=tb_grouped)

    # Remove unnecessary columns.
    tb = tb.drop(columns=["conversion", "item_code", "item_description"], errors="raise")

    # Improve table formats.
    tb_grouped = tb_grouped.format(["country", "year"], short_name="decoupling_food_production_and_land_use_total")
    tb = tb.format(keys=["country", "year", "item"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_grouped], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
