"""Create a dataset with indicators on production and supply (in kilocalories) and agricultural land use (in hectares).

The goal is to create a visualization showing which countries have managed to decouple food production and land use.

"""

from pathlib import Path

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum percentage increase in total domestically produced food energy for a country to qualify as "decoupled".
DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN = 5
# Minimum percentage increase in per-capita domestically produced food energy for a country to qualify as "decoupled".
# Set to a very negative number (e.g. -1000) to effectively disable this criterion.
# Setting it to zero means that at least it didn't decrease.
DOMESTIC_FOOD_PC_INCREASE_PCT_MIN = 0
# Minimum percentage decrease in agricultural land for a country to qualify as "decoupled".
LAND_DECREASE_PCT_MIN = 5
# Maximum percentage increase in imported feed energy for a country to qualify as "decoupled".
# A value of 0 means imported feed must not increase at all.
IMPORTED_FEED_INCREASE_PCT_MAX = 0
# Number of years for the rolling average (1 to not do any rolling average).
# A 3-year window smooths year-to-year variability (e.g. bad harvests, COVID, stock changes).
ROLLING_AVERAGE_YEARS = 3

# Path to local folder where charts will be saved.
# NOTE: Functions that save files will be commented by default; uncomment while doing analysis.
OUTPUT_FOLDER = Path.home() / "Documents/owid/2026-02-27_food_decoupling_analysis/results"

# Columns from food balances dataset.
ELEMENT_CODES = [
    # "Food available for consumption", in kcal/capita/day. It was converted from the original "Food supply (kcal/capita/day)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0664pc",
    # "Food available for consumption", in kg/capita/year. It was converted from the original "Food supply quantity (kg/capita/yr)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    "0645pc",
    # "Production", in tonnes.
    "005511",
    # "Feed", in tonnes. Used to avoid double-counting calories (in crops used for feed and then animal products).
    "005521",
    # "Imports", in tonnes. Used to estimate the domestic share of feed.
    "005611",
    # "Food", in tonnes. Amount allocated to human consumption (excludes feed, seed, processing, waste, other non-food uses).
    "005142",
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
    assert {"Food available for consumption", "Production", "Food"}.issubset(set(check["element"]))
    assert {"kilograms per year per capita", "kilocalories per day per capita", "tonnes"}.issubset(set(check["unit"]))

    # Convert kcal/capita/day to kcal/capita/year.
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
            "value_005142": "food",
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

    # Fill missing feed, imports, and food with 0 (assuming that no data means no use for that item).
    # Check that the food element is not missing too often before filling with 0.
    error = "Unexpectedly high percentage of rows where food element is missing."
    assert 100 * len(tb[tb["food"].isnull()]) / len(tb) < 10, error
    tb["feed"] = tb["feed"].fillna(0)
    tb["imports"] = tb["imports"].fillna(0)
    tb["food"] = tb["food"].fillna(0)

    # Sometimes quantity is missing, but calories is informed, and sometimes is the opposite.
    # Check that either way those are edge cases, and then remove those rows.
    error = "Unexpected number of nans in food quantity or food energy."
    assert 100 * len(tb[(tb["food_quantity"].isnull()) & (tb["food_energy"].notnull())]) / len(tb) < 1, error
    assert 100 * len(tb[(tb["food_quantity"].notnull()) & (tb["food_energy"].isnull())]) / len(tb) < 1, error
    tb = tb.dropna(subset=["food_quantity", "food_energy"], how="any").reset_index(drop=True)

    return tb


def calculate_food_and_feed_energy(tb):
    # Calculate conversion factors, in kcal per 100g.
    # To do that, we reverse-engineer FAOSTAT data: we divide calories by quantity to obtain the conversion factors.
    # NOTE: Conversion factors are needed to convert imported feed (in tonnes) to calories.
    tb["conversion"] = (tb["food_energy"] / (tb["food_quantity"] * 10)).fillna(0)

    # Remove spurious conversion factors (which may happen when dividing by very small quantities); assume a maximum conversion factor of 1000 kcal per 100g.
    error = "Unexpected number of rows where conversion factor is zero, but production is not."
    assert 100 * len(tb[(tb["conversion"] == 0) & (tb["production"] > 0)]) / len(tb) < 3, error
    error = "Unexpected number of rows where conversion factor is unreasonably high."
    assert 100 * len(tb[(tb["conversion"] > 1000)]) / len(tb) < 2, error
    # Remove all rows where conversion is zero or unreasonably high.
    tb = tb[(tb["conversion"] > 0) & (tb["conversion"] < 1000)].reset_index(drop=True)

    # Estimate the domestic share of each item's supply: the proportion of domestic supply that comes from
    # domestic production (as opposed to imports).
    # For example, if a country produces 1000t and imports 500t of maize, the domestic share is 1000/1500 = 2/3.
    # Note that some country-year-items have zero production and imports, but non-zero food and feed.
    # For those odd cases, fill domestic share with zero.
    error = "Unexpectedly large percentage of rows where production and imports are zero but feed is not zero."
    assert (100 * len(tb[(tb["production"] == 0) & (tb["imports"] == 0) & (tb["feed"] > 0)]) / len(tb)) < 0.1, error
    domestic_share = (tb["production"] / (tb["production"] + tb["imports"])).fillna(0)

    # Domestically produced food energy, per item.
    # The "food" element (005142) gives the total tonnes allocated to human consumption, already excluding
    # feed, seed, processing, waste, and other non-food uses. We estimate the domestic share of food
    # using the same proportional allocation: food_domestic = food * domestic_share.
    # Total (kcal): use the "food" element (tonnes) with conversion factors.
    food_domestic = (tb["food"] * domestic_share).clip(upper=tb["production"])
    tb["food_domestic_energy"] = food_domestic * 10000 * tb["conversion"]
    # Per-capita (kcal/capita/year): food_energy (0664pc) is already per capita, multiply by domestic_share.
    tb["food_domestic_energy_pc"] = tb["food_energy"] * domestic_share

    # Imported feed energy (kcal, per item).
    # Imported feed = feed * (1 - domestic_share) = feed * imports / (production + imports).
    # This requires conversion factors to go from tonnes to calories.
    imported_feed = tb["feed"] * (1 - domestic_share)
    tb["imported_feed_energy"] = imported_feed * 10000 * tb["conversion"]

    # Also compute production_energy (feed-corrected) and uncorrected, for backward compatibility and
    # comparison with Hong et al.
    domestic_feed = tb["feed"] * domestic_share
    error = "Unexpectedly large percentage of rows where feed is larger than production."
    assert 100 * len(tb[tb["feed"] > tb["production"]]) / len(tb) < 3, error
    tb["production_net"] = (tb["production"] - domestic_feed).clip(lower=0)
    tb["production_energy"] = tb["production_net"] * 10000 * tb["conversion"]
    tb["production_energy_uncorrected"] = tb["production"] * 10000 * tb["conversion"]

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
                    "food_domestic_energy",
                    "food_domestic_energy_pc",
                    "imported_feed_energy",
                ]
            }
        )
    )

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
    # Convert to kcal/capita/year.
    tb_total["food_energy"] *= 365
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
        "Marshall Islands",
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

    # Plot production: corrected, uncorrected, and Hong et al.
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


def apply_rolling_average(tb_grouped):
    # Apply rolling averages to smooth year-to-year variability.
    if ROLLING_AVERAGE_YEARS > 1:
        tb_grouped = tb_grouped.sort_values(["country", "year"]).reset_index(drop=True)
        for column in tb_grouped.drop(columns=["country", "year"]).columns:
            tb_grouped[column] = tb_grouped.groupby("country", sort=False)[column].transform(
                lambda s: s.rolling(ROLLING_AVERAGE_YEARS, min_periods=1).mean()
            )

        # With a 3-year rolling average, the effective first year is 1963 (average of 1961-1963).
        # Remove years with incomplete rolling averages from both tables.
        year_min = tb_grouped["year"].min() + ROLLING_AVERAGE_YEARS - 1
        tb_grouped = tb_grouped[tb_grouped["year"] >= year_min].reset_index(drop=True)

    return tb_grouped


def detect_decoupled_countries(tb_grouped, plot=False):
    year_min = tb_grouped["year"].min()
    year_max = tb_grouped["year"].max()

    metrics = ["food_domestic_energy", "food_domestic_energy_pc", "imported_feed_energy", "agricultural_land"]
    tb_start = tb_grouped[tb_grouped["year"] == year_min][["country"] + metrics]
    tb_end = tb_grouped[tb_grouped["year"] == year_max][["country"] + metrics]
    tb_changes = tb_start.merge(tb_end, on="country", suffixes=("_start", "_end"))

    tb_changes["food_domestic_energy_change"] = (
        100
        * (tb_changes["food_domestic_energy_end"] - tb_changes["food_domestic_energy_start"])
        / tb_changes["food_domestic_energy_start"]
    )
    tb_changes["food_domestic_energy_pc_change"] = (
        100
        * (tb_changes["food_domestic_energy_pc_end"] - tb_changes["food_domestic_energy_pc_start"])
        / tb_changes["food_domestic_energy_pc_start"]
    )
    tb_changes["imported_feed_energy_change"] = (
        100
        * (tb_changes["imported_feed_energy_end"] - tb_changes["imported_feed_energy_start"])
        / tb_changes["imported_feed_energy_start"]
    )
    tb_changes["agricultural_land_change"] = (
        100
        * (tb_changes["agricultural_land_end"] - tb_changes["agricultural_land_start"])
        / tb_changes["agricultural_land_start"]
    )

    # Handle edge cases where imported feed at baseline is zero.
    # If feed was zero at start and is still zero (or decreased), that's fine (change = 0).
    # If feed was zero at start but increased, treat as infinite increase (disqualify).
    zero_start_mask = tb_changes["imported_feed_energy_start"] == 0
    tb_changes.loc[zero_start_mask & (tb_changes["imported_feed_energy_end"] == 0), "imported_feed_energy_change"] = 0
    tb_changes.loc[zero_start_mask & (tb_changes["imported_feed_energy_end"] > 0), "imported_feed_energy_change"] = (
        float("inf")
    )

    # Diagnostic: print how many countries pass each criterion individually.
    cond_land = tb_changes["agricultural_land_change"] <= -LAND_DECREASE_PCT_MIN
    cond_food_total = tb_changes["food_domestic_energy_change"] >= DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN
    cond_food_pc = tb_changes["food_domestic_energy_pc_change"] >= DOMESTIC_FOOD_PC_INCREASE_PCT_MIN
    cond_feed = tb_changes["imported_feed_energy_change"] <= IMPORTED_FEED_INCREASE_PCT_MAX
    n_total = len(tb_changes)
    print(f"Total countries with data: {n_total}")
    print(f"  Land decrease >= {LAND_DECREASE_PCT_MIN}%: {cond_land.sum()}")
    print(f"  Domestic food (total) increase >= {DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN}%: {cond_food_total.sum()}")
    print(f"  Domestic food (per capita) increase >= {DOMESTIC_FOOD_PC_INCREASE_PCT_MIN}%: {cond_food_pc.sum()}")
    print(f"  Imported feed change <= {IMPORTED_FEED_INCREASE_PCT_MAX}%: {cond_feed.sum()}")
    print(f"  Land + food (total) + food (pc): {(cond_land & cond_food_total & cond_food_pc).sum()}")
    print(f"  All criteria: {(cond_land & cond_food_total & cond_food_pc & cond_feed).sum()}")

    # Detailed view: countries passing land + food (total + pc), sorted by imported feed change.
    both = tb_changes[cond_land & cond_food_total & cond_food_pc].sort_values("imported_feed_energy_change")
    print(f"\n  {'Country':30s}  {'Food tot':>9s}  {'Food pc':>8s}  {'Land':>8s}  {'Feed':>10s}")
    for _, row in both.iterrows():
        feed_val = row["imported_feed_energy_change"]
        feed_str = f"{feed_val:+.0f}%" if feed_val != float("inf") else "+inf"
        print(
            f"  {str(row['country']):30s}  {row['food_domestic_energy_change']:+8.1f}%  {row['food_domestic_energy_pc_change']:+7.1f}%  {row['agricultural_land_change']:+7.1f}%  {feed_str:>10s}"
        )

    countries_decoupled = set(tb_changes[cond_land & cond_food_total & cond_food_pc & cond_feed]["country"])

    if plot:
        plot_decoupled_countries(tb_grouped, countries_decoupled, year_min, year_max)
        plot_slope_chart_grid(tb_changes, countries_decoupled, year_min, year_max)

    return countries_decoupled


def plot_decoupled_countries(
    tb_grouped,
    countries_decoupled,
    year_min,
    year_max,
    y_min=-60,
    y_max=200,
    output_folder=OUTPUT_FOLDER / "decoupled-countries",
):
    """Plot individual line charts showing % change in domestic food, imported feed, and agricultural land for each decoupled country."""
    import plotly.express as px
    from owid.catalog import Table

    if not countries_decoupled:
        print("No decoupled countries to plot.")
        return

    countries_decoupled = sorted(countries_decoupled)

    # Compute percentage change relative to baseline year for each country.
    metrics = ["food_domestic_energy", "imported_feed_energy", "agricultural_land"]
    tb_baseline = tb_grouped[tb_grouped["year"] == year_min][["country"] + metrics]
    tb_plot = tb_grouped[(tb_grouped["country"].isin(countries_decoupled)) & (tb_grouped["year"] >= year_min)][
        ["country", "year"] + metrics
    ].merge(tb_baseline, on="country", suffixes=("", "_baseline"))
    tb_plot["Domestically produced food (calories)"] = (
        100
        * (tb_plot["food_domestic_energy"] - tb_plot["food_domestic_energy_baseline"])
        / tb_plot["food_domestic_energy_baseline"]
    )
    # Handle division by zero when baseline imported feed is zero.
    tb_plot["Imported feed (calories)"] = (
        100
        * (tb_plot["imported_feed_energy"] - tb_plot["imported_feed_energy_baseline"])
        / tb_plot["imported_feed_energy_baseline"].replace(0, float("nan"))
    )
    tb_plot["Agricultural land"] = (
        100
        * (tb_plot["agricultural_land"] - tb_plot["agricultural_land_baseline"])
        / tb_plot["agricultural_land_baseline"]
    )

    tb_plot = Table(tb_plot)

    # Remove metadata units to avoid spurious warnings.
    for column in ["Domestically produced food (calories)", "Imported feed (calories)", "Agricultural land"]:
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
        food_change = float(final_row["Domestically produced food (calories)"].iloc[0])
        feed_change = float(final_row["Imported feed (calories)"].iloc[0])
        land_change = float(final_row["Agricultural land"].iloc[0])
        tb_melted = tb_country.melt(
            id_vars=["country", "year"],
            value_vars=["Domestically produced food (calories)", "Imported feed (calories)", "Agricultural land"],
            var_name="Indicator",
            value_name="Change from baseline (%)",
        )
        fig = px.line(
            tb_melted,
            x="year",
            y="Change from baseline (%)",
            color="Indicator",
            color_discrete_map={
                "Domestically produced food (calories)": "blue",
                "Imported feed (calories)": "orange",
                "Agricultural land": "red",
            },
            title=f"{country} (Food: {food_change:+.1f}%, Feed: {feed_change:+.1f}%, Land: {land_change:+.1f}%)",
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

    if not countries_decoupled:
        print("No decoupled countries to plot in slope chart.")
        return

    tb_decoupled = tb_changes[tb_changes["country"].isin(countries_decoupled)].reset_index(drop=True)
    # Remove metadata units to avoid spurious warnings.
    for column in ["food_domestic_energy_change", "agricultural_land_change"]:
        tb_decoupled[column].metadata.unit = None
        tb_decoupled[column].metadata.short_unit = None

    # Sort by "decoupling score" (domestic food increase minus land decrease).
    tb_decoupled["decoupling_score"] = (
        tb_decoupled["food_domestic_energy_change"] - tb_decoupled["agricultural_land_change"]
    )
    tb_decoupled = tb_decoupled.sort_values("decoupling_score", ascending=False)

    countries_sorted = list(tb_decoupled["country"])
    n_countries = len(countries_sorted)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Calculate global y-axis range for consistent scaling.
    y_max_val = tb_decoupled["food_domestic_energy_change"].max()
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
        food_change = float(country_data["food_domestic_energy_change"])
        land_change = float(country_data["agricultural_land_change"])

        # Total production line.
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
        title_text=f"Countries that increased domestic food production while reducing land use, {year_min}-{year_max}",
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

    # Calculate domestically produced food energy and imported feed energy.
    # Also computes production_energy (feed-corrected) for backward compatibility.
    # NOTE: There are various important assumptions in this calculation, see function for more details.
    tb = calculate_food_and_feed_energy(tb=tb)

    # Calculate totals for each country-year.
    tb_grouped = calculate_totals_for_all_items(tb=tb)

    # Run sanity checks on the aggregated amounts (summed over all items).
    sanity_check_totals(tb_grouped=tb_grouped, tb_fbsc=tb_fbsc)

    # Combine data on total production with agricultural land.
    _n_rows_before = len(tb_grouped)
    tb_grouped = tb_grouped.merge(tb_rl, how="inner", on=["country", "year"])
    error = "Unexpected percentage of rows lost when merging production and land use."
    assert 100 * (_n_rows_before - len(tb_grouped)) / _n_rows_before < 1, error

    # Uncomment to compare the resulting production energy content with the estimate from the Hong et al. (2021) paper.
    # sanity_check_compare_with_hong_et_al(tb_grouped=tb_grouped)

    # Remove unnecessary columns.
    tb_grouped = tb_grouped.drop(columns=["production_energy_uncorrected", "food_quantity"], errors="raise")

    # Apply a rolling average of ROLLING_AVERAGE_YEARS (defined above) on all indicators.
    tb_grouped = apply_rolling_average(tb_grouped=tb_grouped)

    # Select countries that achieved decoupling: production increased and land use decreased over the full time window.
    # Set plot=True to generate individual country charts and a slope chart grid.
    countries_decoupled = detect_decoupled_countries(tb_grouped, plot=False)

    # Filter to only include decoupled countries.
    tb_grouped = tb_grouped[tb_grouped["country"].isin(countries_decoupled)].reset_index(drop=True)

    # Improve table format.
    tb_grouped = tb_grouped.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_grouped], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
