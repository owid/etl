"""Create a dataset with indicators on food production (in kilocalories) and agricultural land use (in hectares).

The goal is to create a visualization showing which countries have managed to decouple food production and land use.

NOTE on double-counting correction:
When summing crop and livestock production in calories, crops used as domestic animal feed would be counted twice:
once as crop production, and again as the livestock output they helped produce. To avoid this, we subtract the
estimated domestic feed portion from crop production before converting to calories.

Since FAOSTAT's "feed" element reports total feed use (from both domestic and imported crops), we estimate the
domestic share using a proportional allocation: domestic_feed = feed x ( production / (production + imports) ).
This way, imported feed (which is not in the production count) is not subtracted.

"""

from pathlib import Path

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum percentage increase in production energy for a country to qualify as "decoupled".
PRODUCTION_INCREASE_PCT_MIN = 5
# Minimum percentage decrease in agricultural land for a country to qualify as "decoupled".
LAND_DECREASE_PCT_MIN = 5
# Number of years for the rolling average (1 to not do any rolling average).
# A 3-year window smooths year-to-year variability (e.g. bad harvests, COVID, stock changes).
ROLLING_AVERAGE_YEARS = 3

# Path to local folder where charts will be saved.
# NOTE: Functions that save files will be commented by default; uncomment while doing analysis.
OUTPUT_FOLDER = Path.home() / "Documents/owid/2026-02-20_food_decoupling_analysis/results"

# Columns from food balances dataset.
ELEMENT_CODES = [
    # "Food available for consumption", in kcal/capita/day. It was converted from the original "Food supply (kcal/capita/day)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0664pc",
    # "Food available for consumption", in kg/capita/year. It was converted from the original "Food supply quantity (kg/capita/yr)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0645pc",
    # "Production", in tonnes.
    "005511",
    # "Feed", in tonnes. Used to void double-counting calories (in crops used for feed and then animal products).
    "005521",
    # "Imports", in tonnes. Used to estimate the domestic share of feed.
    "005611",
]


# Food groups (copied from the garden additional_variables step) created by OWID for FBSC (combination of FBS and FBSH).
# This is needed to be able to compute totals for imports (food supply, in kcal, already has a total item).
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


def prepare_land_use_data(tb_rl):
    # Select relevant elements from land use data.
    # Item code "Agricultural land" (00006610).
    # Element code "Area" (005110) in hectares.
    tb_rl = tb_rl[(tb_rl["element_code"] == "005110") & (tb_rl["item_code"] == "00006610")].reset_index(drop=True)
    error = "Units of area have changed."
    assert set(tb_rl["unit"]) == {"hectares"}, error
    tb_rl = tb_rl[["country", "year", "value"]].rename(columns={"value": "agricultural_land"}, errors="raise")

    return tb_rl


def prepare_food_balances_data(tb_fbsc):
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
    assert {"Food available for consumption", "Production"}.issubset(set(check["element"]))
    assert {"kilograms per year per capita", "kilocalories per day per capita", "tonnes"}.issubset(set(check["unit"]))

    # Convert kcal/capita/day to kcal/capita/year.
    # TODO: Check if this is a good idea. It may be more convenient to have numbers in terms of daily calories.
    tb.loc[(tb["element_code"] == "0664pc"), "value"] *= 365

    # Pivot to have kcal and weight in separate columns.
    tb = tb.pivot(
        index=["country", "year", "item", "item_code", "item_description"],
        columns=["element_code"],
        values=["value"],
        join_column_levels_with="_",
    ).rename(
        columns={
            "value_0645pc": "food_quantity",
            "value_0664pc": "food_energy",
            "value_005511": "production",
            "value_005521": "feed",
            "value_005611": "imports",
        }
    )

    return tb


def handle_regions_and_missing_data(tb):
    # Remove regions and FAO aggregates (we only want individual countries).
    tb = tb[
        ~tb["country"].isin(paths.regions.regions_all) & ~tb["country"].str.contains("(FAO)", regex=False)
    ].reset_index(drop=True)

    # NOTE: With the additional element codes (feed, imports), the pivot creates more rows for items that have
    # feed/imports data but no production (e.g., items a country imports but doesn't produce).
    # We keep only rows for which we have production data.
    tb = tb.dropna(subset="production").reset_index(drop=True)

    # Fill missing feed and imports with 0 (assuming that no data means no feed use or no imports for that item).
    tb["feed"] = tb["feed"].fillna(0)
    tb["imports"] = tb["imports"].fillna(0)

    # Sometimes quantity is missing, but calories is informed, and sometimes is the opposite.
    # Check that either way those are edge cases, and then remove those rows.
    error = "Unexpected number of nans in food quantity or food energy."
    assert 100 * len(tb[(tb["food_quantity"].isnull()) & (tb["food_energy"].notnull())]) / len(tb) < 0.1, error
    assert 100 * len(tb[(tb["food_quantity"].notnull()) & (tb["food_energy"].isnull())]) / len(tb) < 0.1, error
    tb = tb.dropna(subset=["food_quantity", "food_energy"], how="any").reset_index(drop=True)

    return tb


def calculate_production_calories_correcting_for_feed(tb):
    # Calculate conversion factors, in kcal per 100g.
    # To do that, we reverse-engineer FAOSTAT data: we divide calories by quantity to obtain the conversion factors.
    tb["conversion"] = (tb["food_energy"] / (tb["food_quantity"] * 10)).fillna(0)

    # Remove spurious conversion factors (which may happen when dividing by very small quantities); assume a maximum conversion factor of 1000 kcal per 100g.
    error = "Unexpected number of rows where conversion factor is zero, but production is not."
    assert 100 * len(tb[(tb["conversion"] == 0) & (tb["production"] > 0)]) / len(tb) < 3, error
    error = "Unexpected number of rows where conversion factor is unreasonably high."
    assert 100 * len(tb[(tb["conversion"] > 1000)]) / len(tb) < 2, error
    # Remove all rows where conversion is zero or unreasonably high.
    tb = tb[(tb["conversion"] > 0) & (tb["conversion"] < 1000)].reset_index(drop=True)
    # Uncomment to visually inspect a histogram of conversions factors.
    # px.histogram(tb, x="conversion", title=f"All items (all years and countries)", labels={"conversion": f"Conversion  / kcal per 100g"}, nbins=500, histnorm="percent").show()

    # Naively, we could simply convert all production to calories, using the conversion factors.
    # But we would be counting calories of crops that were used to feed animals, and then count the calories from those animals as well.
    # We need to remove the calories of domestic production that are used for animal feed.
    # However, FAOSTAT's "feed" element reports total feed use (from both domestic and imported crops).
    # We estimate the domestic share proportionally.
    # For example, imagine a country produces 1000t and imports 500t of maize; it also reports 600t of feed.
    # We assume that 1000/1500=2/3 of the maize used for feed came from domestic production, and 500/1500=1/3 of from imports.
    # This means that "domestic feed" would be 2/3 * 600t = 400t.
    # We then remove the domestic feed from the total domestic production, so:
    # domestic feed = feed x production / (production + imports)
    # Net production = production - domestic feed
    domestic_feed = tb["feed"] * (tb["production"] / (tb["production"] + tb["imports"])).fillna(0)
    # TODO: Investigate the clipping. When is feed larger than production?
    tb["production_net"] = (tb["production"] - domestic_feed).clip(lower=0)

    # Apply conversion factor (of kcal per 100g) to net production (in tonnes).
    tb["production_energy"] = tb["production_net"] * 10000 * tb["conversion"]
    # Also compute the uncorrected version for comparison with Hong et al.
    tb["production_energy_uncorrected"] = tb["production"] * 10000 * tb["conversion"]

    # Sanity checks.
    error = "Unexpected zero or nan production energy."
    assert tb[(tb["production_energy"] == 0) & (tb["production_net"] != 0)].empty, error
    assert tb[(tb["production_energy"].isnull())].empty, error

    # NOTE: FBS items are grouped together, and the proportions of subitems in each group may differ for different items, then it may be impossible (or quite inaccurate) to translate those groups into energy with a simple conversion factor.
    # for item in ["Wheat", "Apples"]:
    #     px.histogram(tb[tb["item"] == item], x="conversion", title=f"{item} (all years and countries)", labels={"conversion": f"Conversion  / kcal per 100g"}, nbins=500, histnorm="percent", range_x=(0, 1000)).show()
    # So, it seems that FBS has grouped items, e.g. Wheat actually means:
    # 'Default composition: 15 Wheat, 16 Flour, wheat, 17 Bran, wheat, 18 Macaroni, 19 Germ, wheat, 20 Bread, 21 Bulgur, 22 Pastry, 23 Starch, wheat, 24 Gluten, wheat, 41 Cereals, breakfast, 110 Wafers, 114 Mixes and doughs, 115 Food preparations, flour, malt extract'
    # So, the resulting food energy would be a weighted average of all those products (across all years and countries). Maybe, once we combine those items in the right proportion, the conversion factor would follow the resulting distribution above. For example, bread is at 249 kcal. So, maybe the first peak in the histogram (around 260 kcal) could be due to the abundance of bread (shifted up by other items).

    return tb


def calculate_totals_for_all_items(tb):
    # Get the total from the already aggregated item groups.
    # NOTE: It might be better to get the sum of their subitems (and compare with the item group sum totals).
    ITEM_CODES = sorted(set(sum([items for _, items in FOOD_GROUPS_FBSC.items()], [])))
    tb_grouped = (
        tb[tb["item_code"].isin(ITEM_CODES)]
        .groupby(["country", "year"], as_index=False)
        .agg(
            {
                column: "sum"
                for column in [
                    "food_quantity",
                    "food_energy",
                    "production",
                    "production_energy",
                    "production_energy_uncorrected",
                ]
            }
        )
    )
    # For convenience, add population again to this table.
    tb_grouped = paths.regions.add_population(tb=tb_grouped, warn_on_missing_countries=False)

    return tb_grouped


def sanity_check_totals(tb_grouped, tb_fbsc):
    # For sanity checks, recover the "Total" item of food from the original data.
    tb_total = tb_fbsc[
        (tb_fbsc["item_code"] == "00002901") & (tb_fbsc["element_code"].isin(ELEMENT_CODES))
    ].reset_index(drop=True)
    error = "'Total' item code or name has changed."
    assert set(tb_total["item"]) == {"Total"}, error
    error = "Expected 'Total' item to be only available for per capita food (kcal)."
    for element in [element for element in ELEMENT_CODES if element != "0664pc"]:
        assert tb_total[(tb_total["element_code"] == element)].empty, error
    tb_total = tb_total[["country", "year", "value"]].rename(columns={"value": "food_energy"})
    # TODO: Remove the following line if we decide it's better to keep food in daily calories.
    tb_total["food_energy"] *= 365
    # Convert to total (instead of per capita).
    tb_total = paths.regions.add_population(tb=tb_total, warn_on_missing_countries=False)
    tb_total["food_energy"] *= tb_total["population"]
    # Check that the aggregated food energy coincides for each country with the original total food energy.
    tb_check = tb_grouped[["country", "year", "food_energy"]].merge(
        tb_total[["country", "year", "food_energy"]].rename(columns={"food_energy": "food_energy_original"})
    )
    tb_check["pct"] = (
        100 * abs(tb_check["food_energy"] - tb_check["food_energy_original"]) / tb_check["food_energy_original"]
    )
    error = "Calculated total food energy (kcal) differs from original by more than expected."
    assert tb_check["pct"].mean() < 5, error
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


def sanity_check_compare_with_hong_et_al(tb_grouped, output_folder=OUTPUT_FOLDER / "comparison-with-hong-et-al-2021"):
    """Compare our production energy (with and without feed correction) with Hong et al. (2021).

    Saves individual country charts to output_folder (or shows interactively if output_folder is None).
    Each chart has 3 lines: "OWID (feed-corrected)", "OWID (uncorrected)", "Hong et al. (2021)".
    Since Hong et al. likely double-counted feed, their values should be closer to our uncorrected values.
    """
    import pandas as pd
    import plotly.express as px
    from owid.datautils.dataframes import map_series

    from etl.paths import STEP_DIR

    # Extract the version of the fbsc step from the dependency uri.
    fbsc_version = [step.split("/")[4] for step in paths.dependencies if "faostat_fbsc" in step][0]

    # Load data from a local file. The file can be downloaded from:
    # https://figshare.com/articles/dataset/Global_and_regional_drivers_of_land-use_emissions_in_1961-2017/12248735?file=26174975
    # Specifically tab "8.1.AgProd" for agricultural production, and "9.1.AgLand" for agricultural land use.
    DATA_FILE = Path.home() / "Documents/owid/2026-02-20_food_decoupling_analysis/data/LUE_Data_CALUE.xlsx"
    df_food = pd.read_excel(DATA_FILE, sheet_name="8.1.AgProd")
    df_land = pd.read_excel(DATA_FILE, sheet_name="9.1.AgLand")
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
    # Harmonize all other country names.
    df = paths.regions.harmonize_names(
        tb=df,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
        warn_on_missing_countries=False,
    )
    # Harmonize missing mappings (that are not in the FAOSTAT country harmonization file):
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
    check = pd.DataFrame(tb_grouped)[
        ["country", "year", "production_energy", "production_energy_uncorrected", "agricultural_land"]
    ].merge(df, how="left", on=["country", "year"])
    check["pct_food"] = (
        100 * abs(check["production_energy"] - check["production_energy_hong"]) / check["production_energy_hong"]
    )
    check["pct_land"] = (
        100 * abs(check["agricultural_land"] - check["agricultural_land_hong"]) / check["agricultural_land_hong"]
    )
    # NOTE: With the feed correction, our production_energy is expected to be lower than Hong et al.'s.
    # The median percentage difference may be larger than in the original step.
    error = "Expected percentage difference between our production energy and that from Hong et al. (2021) to be within a reasonable range."
    assert check["pct_food"].median() < 40, error
    error = "Expected percentage difference between our agricultural land and that from Hong et al. (2021) to agree within ~14%"
    assert check["pct_land"].median() < 14, error

    # Plot food production: corrected, uncorrected, and Hong et al.
    value_name = "Production energy / kcal"
    plot = (
        check[["country", "year", "production_energy", "production_energy_uncorrected", "production_energy_hong"]]
        .rename(
            columns={
                "production_energy": "OWID (feed-corrected)",
                "production_energy_uncorrected": "OWID (uncorrected)",
                "production_energy_hong": "Hong et al. (2021)",
            }
        )
        .melt(id_vars=["country", "year"], value_name=value_name)
    )
    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
    for country in sorted(set(check["country"])):
        _plot = plot[plot["country"] == country].dropna()
        # Only plot if we have at least 2 series (OWID + Hong).
        if len(set(_plot["variable"])) < 2:
            continue
        fig = px.line(
            _plot,
            x="year",
            y=value_name,
            color="variable",
            markers=True,
            title=country,
            range_y=(0, _plot[value_name].max() * 1.05),
        )
        if output_folder is not None:
            safe_name = country.replace("/", "_").replace(" ", "_")
            fig.write_image(Path(output_folder) / f"{safe_name}.png")
        else:
            fig.show()
    # In terms of food production (before correcting for double-counting feed calories), most countries agree reasonably well with Hong et al (2021); in fact better than expected, given that FAOSTAT has probably changed since the publication of that paper.
    # Cases with significant discrepancies Indonesia, Hong-Kong, or Malaysia. And one where the difference is particularly significant is Iceland.
    # Land use differs significantly more, but I suppose that's also mostly due to changes in FAOSTAT RL data.
    # The similarity between our uncorrected caloric series and Hong et al.'s series suggests that they did not correct for feed calories.


def show_decoupled_countries(
    food_column,
    tb_fbsc,
    tb_grouped,
    min_food_pct_change=5,
    min_land_pct_change=5,
    max_net_share_imports=20,
    max_net_share_median_over_years=10,
):
    import plotly.express as px

    year_max = tb_grouped["year"].max()
    year_min = tb_grouped["year"].min()

    # Calculate the share of net annual imports of each country-year.
    # This is, for each country-year (after aggregating all the main food item groups):
    # 100 * | imports - exports | / domestic supply
    # Relevant element codes:
    # 005611: Imports (tonnes)
    # 005911: Exports (tonnes)
    # 005511: Production (tonnes)
    # Instead of production, it may be more appropriate to use domestic supply instead (Domestic supply = Production + Imports − Exports ± Stock changes).
    # 005301: Domestic supply (tonnes)
    imports = (
        tb_fbsc[
            (tb_fbsc["item_code"].isin(sorted(set(sum([items for _, items in FOOD_GROUPS_FBSC.items()], [])))))
            & (tb_fbsc["element_code"].isin(["005611", "005911", "005301"]))
        ]
        .reset_index(drop=True)
        .groupby(["country", "year", "element"], as_index=False)
        .agg({"value": "sum"})
        .pivot(index=["country", "year"], columns=["element"], join_column_levels_with="_")
        .format()
        .reset_index()
    )
    # Calculate the net import share (share of domestic production that is imported).
    imports["net_import_share"] = (
        100 * (imports["value_imports"] - imports["value_exports"]) / imports["value_domestic_supply"]
    )

    check = tb_grouped.copy()
    # Add column of net imports share.
    if max_net_share_median_over_years is None:
        check = check.merge(imports, on=["country", "year"], how="left")
    else:
        # Instead of simply imposing a maximum net import share, we impose a maximum median (over the last 10 years) net import share, for each country.
        imports = (
            imports[(imports["year"] > (year_max - max_net_share_median_over_years))]
            .groupby("country", as_index=False)
            .agg({"net_import_share": "median"})
        )
        check = check.merge(imports, on=["country"], how="left")
    check = check.merge(check[(check["year"] == year_min)], how="left", on=["country"], suffixes=("", "_baseline"))
    check[f"{food_column}_change"] = (
        100 * (check[food_column] - check[f"{food_column}_baseline"]) / check[f"{food_column}_baseline"]
    )
    check["agricultural_land_change"] = (
        100 * (check["agricultural_land"] - check["agricultural_land_baseline"]) / check["agricultural_land_baseline"]
    )
    # List countries that could be added to the chart as examples of decoupling.
    # Sort them by land use relative change.
    _check = check[check["year"] == year_max]
    if food_column == "production_energy":
        decoupled = (
            _check[
                (_check["agricultural_land_change"] <= -min_land_pct_change)
                & (_check[f"{food_column}_change"] >= min_food_pct_change)
            ]
            .dropna(subset=[f"{food_column}_change", "agricultural_land_change"], how="any")
            .sort_values("agricultural_land_change")["country"]
            .unique()
            .tolist()
        )
    else:
        decoupled = (
            _check[
                (_check["net_import_share"] <= max_net_share_imports)
                & (_check["agricultural_land_change"] <= -min_land_pct_change)
                & (_check[f"{food_column}_change"] >= min_food_pct_change)
            ]
            .dropna(subset=[f"{food_column}_change", "agricultural_land_change"], how="any")
            .sort_values("agricultural_land_change")["country"]
            .unique()
            .tolist()
        )
    # Remove regions.
    decoupled = [country for country in decoupled if "(FAO)" not in country if country not in geo.REGIONS]
    # Plot decoupling curves for those countries.
    columns = [f"{food_column}_change", "agricultural_land_change"]
    for country in sorted(decoupled):
        px.line(
            check[check["country"] == country][["year"] + columns].melt(id_vars=["year"]),
            x="year",
            y="value",
            color="variable",
            markers=True,
            title=country,
        ).show()

    # Print resulting list of decoupled countries.
    print(", ".join(decoupled))


def plot_decoupled_countries(
    tb_grouped,
    countries_decoupled,
    year_min,
    year_max,
    y_min=-60,
    y_max=200,
    output_folder=OUTPUT_FOLDER / "decoupled-countries",
):
    """Plot individual line charts showing % change in production energy and agricultural land for each decoupled country."""
    import plotly.express as px
    from owid.catalog import Table

    countries_decoupled = sorted(countries_decoupled)

    # Compute percentage change relative to baseline year for each country.
    # Filter to year >= year_min to exclude years with incomplete rolling averages.
    tb_baseline = tb_grouped[tb_grouped["year"] == year_min][["country", "production_energy", "agricultural_land"]]
    tb_plot = tb_grouped[(tb_grouped["country"].isin(countries_decoupled)) & (tb_grouped["year"] >= year_min)][
        ["country", "year", "production_energy", "agricultural_land"]
    ].merge(tb_baseline, on="country", suffixes=("", "_baseline"))
    tb_plot["Food production (calories)"] = (
        100
        * (tb_plot["production_energy"] - tb_plot["production_energy_baseline"])
        / tb_plot["production_energy_baseline"]
    )
    tb_plot["Agricultural land"] = (
        100
        * (tb_plot["agricultural_land"] - tb_plot["agricultural_land_baseline"])
        / tb_plot["agricultural_land_baseline"]
    )

    # Add baseline point at year_min with 0% change.
    tb_plot = Table(tb_plot)

    # Remove metadata units to avoid spurious warnings.
    for column in ["Food production (calories)", "Agricultural land"]:
        tb_plot[column].metadata.unit = None
        tb_plot[column].metadata.short_unit = None

    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)

    for country in countries_decoupled:
        tb_country = tb_plot[tb_plot["country"] == country].reset_index(drop=True)
        if tb_country.empty:
            continue
        final_row = tb_country[tb_country["year"] == year_max]
        if final_row.empty:
            print(f"Not enough data for this window of years for {country}")
            continue
        food_change = float(final_row["Food production (calories)"].iloc[0])
        land_change = float(final_row["Agricultural land"].iloc[0])
        tb_melted = tb_country.melt(
            id_vars=["country", "year"],
            value_vars=["Food production (calories)", "Agricultural land"],
            var_name="Indicator",
            value_name="value",
        )
        fig = px.line(
            tb_melted,
            x="year",
            y="value",
            color="Indicator",
            title=f"{country} (Food production: {food_change:+.1f}%, Land use: {land_change:+.1f}%)",
        ).update_yaxes(range=[y_min, y_max])
        if output_folder is not None:
            safe_name = country.replace("/", "_").replace(" ", "_")
            fig.write_image(Path(output_folder) / f"{safe_name}.png")
        else:
            fig.show()


def plot_slope_chart_grid(
    tb_changes, countries_decoupled, year_min, year_max, n_cols=6, output_file=OUTPUT_FOLDER / "smooth-grid.png"
):
    """Create a grid of slope charts showing decoupling for each country."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    tb_decoupled = tb_changes[tb_changes["country"].isin(countries_decoupled)].reset_index(drop=True)
    # Remove metadata units to avoid spurious warnings.
    for column in ["production_energy_change", "agricultural_land_change"]:
        tb_decoupled[column].metadata.unit = None
        tb_decoupled[column].metadata.short_unit = None

    # Sort by "decoupling score" (production increase minus land decrease).
    tb_decoupled["decoupling_score"] = (
        tb_decoupled["production_energy_change"] - tb_decoupled["agricultural_land_change"]
    )
    tb_decoupled = tb_decoupled.sort_values("decoupling_score", ascending=False)

    countries_sorted = list(tb_decoupled["country"])
    n_countries = len(countries_sorted)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Calculate global y-axis range for consistent scaling.
    y_max_val = tb_decoupled["production_energy_change"].max()
    y_min_val = tb_decoupled["agricultural_land_change"].min()
    y_range = [y_min_val * 1.15, y_max_val * 1.15]

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=countries_sorted,
        vertical_spacing=0.08,
        horizontal_spacing=0.04,
    )

    food_color = "#3366cc"
    land_color = "#cc3333"

    for i, country in enumerate(countries_sorted):
        row = i // n_cols + 1
        col = i % n_cols + 1

        country_data = tb_decoupled[tb_decoupled["country"] == country].iloc[0]
        food_change = float(country_data["production_energy_change"])
        land_change = float(country_data["agricultural_land_change"])

        # Food production line.
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, food_change],
                mode="lines+text",
                line=dict(color=food_color, width=2),
                text=["", f"{food_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=food_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )

        # Land use line.
        fig.add_trace(
            go.Scatter(
                x=[year_min, year_max],
                y=[0, land_change],
                mode="lines+text",
                line=dict(color=land_color, width=2),
                text=["", f"{land_change:+.0f}%"],
                textposition="middle right",
                textfont=dict(color=land_color, size=10),
                showlegend=False,
                cliponaxis=False,
            ),
            row=row,
            col=col,
        )

    fig.update_layout(
        height=200 * n_rows,
        width=180 * n_cols,
        title_text=f"Countries that increased food production while reducing land use, {year_min}-{year_max}",
        showlegend=False,
    )

    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=True, zerolinecolor="lightgray", range=y_range)

    if output_file is not None:
        fig.write_image(output_file)
    else:
        fig.show()


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

    #
    # Process data.
    #
    # Prepare land use data.
    tb_rl = prepare_land_use_data(tb_rl=tb_rl)

    # Prepare food balances data.
    tb = prepare_food_balances_data(tb_fbsc=tb_fbsc)

    # Some basic processing:
    # * Remove regions (we only care about individual countries for this dataset).
    # * Remove rows with no production, food, or food calories data.
    # * Fill missing feed and imports with zeros.
    tb = handle_regions_and_missing_data(tb=tb)

    # Calculate production in terms of caloric content, and correct to avoid double-counting calories of crops used for animal feed.
    # NOTE: There are various important assumptions in this calculation, see function for more details.
    tb = calculate_production_calories_correcting_for_feed(tb=tb)

    # Convert food quantity and food energy from per capita to totals.
    # TODO: Check how much things change when using per capita.
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    for column in ["food_quantity", "food_energy"]:
        tb[column] *= tb["population"]

    # Calculate totals for each country-year.
    # NOTE: tb_fbsc is used for sanity checks.
    tb_grouped = calculate_totals_for_all_items(tb=tb)

    # Run sanity checks on the aggregated amounts (summed over all items).
    sanity_check_totals(tb_grouped=tb_grouped, tb_fbsc=tb_fbsc)

    # Combine data on total food production with agricultural land.
    _n_rows_before = len(tb_grouped)
    tb_grouped = tb_grouped.merge(tb_rl, how="inner", on=["country", "year"])
    error = "Unexpected percentage of rows lost when merging production and land use."
    assert 100 * (_n_rows_before - len(tb_grouped)) / _n_rows_before < 1, error

    # Uncomment to compare the resulting production energy content with the estimate from the Hong et al. (2021) paper.
    sanity_check_compare_with_hong_et_al(tb_grouped=tb_grouped)

    # Apply rolling averages to smooth year-to-year variability.
    if ROLLING_AVERAGE_YEARS > 1:
        tb_grouped = tb_grouped.sort_values(["country", "year"]).reset_index(drop=True)
        for column in tb_grouped.drop(columns=["country", "year"]).columns:
            tb_grouped[column] = tb_grouped.groupby("country", sort=False)[column].transform(
                lambda s: s.rolling(ROLLING_AVERAGE_YEARS, min_periods=1).mean()
            )

    # Select countries that achieved decoupling: production up and land down over the full time window.
    # With a 3-year rolling average, the effective first year is 1963 (average of 1961-1963).
    year_min = tb_grouped["year"].min() + max(1, ROLLING_AVERAGE_YEARS) - 1
    year_max = tb_grouped["year"].max()

    # Remove years with incomplete rolling averages from both tables.
    tb_grouped = tb_grouped[tb_grouped["year"] >= year_min].reset_index(drop=True)
    tb = tb[tb["year"] >= year_min].reset_index(drop=True)

    tb_start = tb_grouped[tb_grouped["year"] == year_min][["country", "production_energy", "agricultural_land"]]
    tb_end = tb_grouped[tb_grouped["year"] == year_max][["country", "production_energy", "agricultural_land"]]
    tb_changes = tb_start.merge(tb_end, on="country", suffixes=("_start", "_end"))
    tb_changes["production_energy_change"] = (
        100
        * (tb_changes["production_energy_end"] - tb_changes["production_energy_start"])
        / tb_changes["production_energy_start"]
    )
    tb_changes["agricultural_land_change"] = (
        100
        * (tb_changes["agricultural_land_end"] - tb_changes["agricultural_land_start"])
        / tb_changes["agricultural_land_start"]
    )

    countries_decoupled = set(
        tb_changes[
            (tb_changes["production_energy_change"] >= PRODUCTION_INCREASE_PCT_MIN)
            & (tb_changes["agricultural_land_change"] <= -LAND_DECREASE_PCT_MIN)
        ]["country"]
    )

    # Uncomment to plot individual charts for each decoupled country.
    # plot_decoupled_countries(tb_grouped, countries_decoupled, year_min, year_max)
    # Uncomment to plot a slope chart grid PNG with all decoupled countries.
    # plot_slope_chart_grid(tb_changes, countries_decoupled, year_min, year_max)

    # Filter both tables to only include decoupled countries.
    tb_grouped = tb_grouped[tb_grouped["country"].isin(countries_decoupled)].reset_index(drop=True)
    tb = tb[tb["country"].isin(countries_decoupled)].reset_index(drop=True)

    # Remove unnecessary columns.
    tb = tb.drop(
        columns=[
            "conversion",
            "item_code",
            "item_description",
            "feed",
            "imports",
            "production_net",
            "production_energy_uncorrected",
        ],
        errors="raise",
    )
    tb_grouped = tb_grouped.drop(columns=["production_energy_uncorrected"], errors="raise")

    # Improve table formats.
    tb_grouped = tb_grouped.format(["country", "year"], short_name="decoupling_food_production_and_land_use_total")
    tb = tb.format(keys=["country", "year", "item"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_grouped], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
