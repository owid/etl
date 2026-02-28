"""Create a dataset with indicators on production and supply (in kilocalories) and agricultural land use (in hectares).

The goal is to create a visualization showing which countries have managed to decouple food production and land use.

TODO: This is still work in progress, I'm trying various approaches to find a good measurable criterion for decoupling, and selecting which countries fulfil them, if any.

"""

from pathlib import Path

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum percentage increase in total domestically produced food energy for a country to qualify as "decoupled".
DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN = 0
# Minimum percentage increase in total per-capita food supply (kcal/capita/day) for a country to qualify as "decoupled".
# This uses total food available (domestic + imported), not just domestically produced.
# Setting it to zero means that at least it didn't decrease.
FOOD_PC_INCREASE_PCT_MIN = 0
# Minimum percentage decrease in agricultural land for a country to qualify as "decoupled".
LAND_DECREASE_PCT_MIN = 0
# Minimum percentage decrease in total land footprint (domestic + offshored) for a country to qualify as "decoupled".
# Offshored land is estimated from imported feed using global average feed crop yields.
TOTAL_LAND_DECREASE_PCT_MIN = 0
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


# Top feed crops used to estimate a fallback average yield (for unmapped items).
FEED_CROP_NAMES = ["Maize", "Wheat", "Barley", "Soybeans", "Sorghum"]

# Mapping from SCL item codes to (QCL parent crop code, extraction rate).
# The extraction rate is the fraction of the parent crop's weight that becomes this product.
# E.g., 1 tonne of soybeans → ~0.79 tonnes of soybean cake, so extraction_rate = 0.79.
# To estimate the parent crop tonnage needed: parent_tonnes = product_tonnes / extraction_rate.
# For primary crops that appear directly in QCL, extraction_rate = 1.0.
# Items mapped to None have no meaningful crop yield (dairy, fish, meat, etc.).
FEED_SCL_TO_QCL_MAPPING = {
    # Primary crops (directly in QCL, extraction_rate = 1.0)
    # SCL "Maize (corn)" (34.1% of global feed) → QCL "Maize"
    "00000056": ("00000056", 1.0),
    # SCL "Wheat" (7.3%) → QCL "Wheat"
    "00000015": ("00000015", 1.0),
    # SCL "Barley" (4.0%) → QCL "Barley"
    "00000044": ("00000044", 1.0),
    # SCL "Sugar cane" (3.4%) → QCL "Sugar cane"
    "00000156": ("00000156", 1.0),
    # SCL "Rice" (1.5%) → QCL "Rice"
    "00000027": ("00000027", 1.0),
    # SCL "Soya beans" (1.4%) → QCL "Soybeans"
    "00000236": ("00000236", 1.0),
    # SCL "Sweet potatoes" (1.4%) → QCL "Sweet potatoes"
    "00000122": ("00000122", 1.0),
    # SCL "Potatoes" (1.4%) → QCL "Potatoes"
    "00000116": ("00000116", 1.0),
    # SCL "Other vegetables, fresh n.e.c." (1.3%) → QCL same
    "00000463": ("00000463", 1.0),
    # SCL "Sorghum" (1.0%) → QCL "Sorghum"
    "00000083": ("00000083", 1.0),
    # SCL "Cassava, fresh" (0.8%) → QCL "Cassava"
    "00000125": ("00000125", 1.0),
    # SCL "Oats" (0.7%) → QCL "Oats"
    "00000075": ("00000075", 1.0),
    # SCL "Triticale" (0.6%) → QCL "Triticale"
    "00000097": ("00000097", 1.0),
    # SCL "Cotton seed" (0.5%) → QCL "Seed cotton"
    "00000329": ("00000328", 1.0),
    # SCL "Yams" (0.4%) → QCL "Yams"
    "00000137": ("00000137", 1.0),
    # SCL "Watermelons" (0.4%) → QCL "Watermelons"
    "00000567": ("00000567", 1.0),
    # SCL "Sugar beet" (0.3%) → QCL "Sugar beet"
    "00000157": ("00000157", 1.0),
    # SCL "Rape or colza seed" (0.3%) → QCL "Rapeseed"
    "00000270": ("00000270", 1.0),
    # SCL "Rye" (0.3%) → QCL "Rye"
    "00000071": ("00000071", 1.0),
    # SCL "Peas, dry" (0.3%) → QCL "Peas, dry"
    "00000187": ("00000187", 1.0),
    # SCL "Millet" (0.2%) → QCL "Millet"
    "00000079": ("00000079", 1.0),
    # SCL "Cabbages" (0.2%) → QCL "Cabbages"
    "00000358": ("00000358", 1.0),
    # SCL "Edible roots and tubers n.e.c." (0.2%) → QCL same
    "00000149": ("00000149", 1.0),
    # SCL "Sunflower seed" (0.2%) → QCL "Sunflower seed"
    "00000267": ("00000267", 1.0),
    # SCL "Beans, dry" (0.2%) → QCL "Beans, dry"
    "00000176": ("00000176", 1.0),
    # SCL "Chick peas, dry" (0.1%) → QCL "Chick peas, dry"
    "00000191": ("00000191", 1.0),
    # SCL "Cucumbers and gherkins" (0.1%) → QCL same
    "00000397": ("00000397", 1.0),
    # SCL "Carrots and turnips" (0.1%) → QCL same
    "00000426": ("00000426", 1.0),
    # SCL "Broad beans and horse beans, dry" (0.1%) → QCL same
    "00000181": ("00000181", 1.0),
    # SCL "Mixed grain" (0.1%) → QCL "Mixed grain"
    "00000103": ("00000103", 1.0),
    # Oilseed cakes (by-product → parent crop)
    # SCL "Cake of soya beans" (12.1%) → QCL "Soybeans"; ~79% extraction rate
    "00000238": ("00000236", 0.79),
    # SCL "Cake of rapeseed" (1.8%) → QCL "Rapeseed"; ~60% extraction rate
    "00000272": ("00000270", 0.60),
    # SCL "Cake of sunflower seed" (0.9%) → QCL "Sunflower seed"; ~55% extraction rate
    "00000269": ("00000267", 0.55),
    # SCL "Cake of cottonseed" (0.7%) → QCL "Seed cotton"; ~45% extraction rate
    "00000332": ("00000328", 0.45),
    # SCL "Cake of palm kernel" (0.6%) → QCL "Palm fruit oil" (proxy); ~50% extraction rate
    "00000259": ("00000254", 0.50),
    # SCL "Cake of groundnuts" (0.4%) → QCL "Groundnuts"; ~55% extraction rate
    "00000245": ("00000242", 0.55),
    # SCL "Cake of rice bran" (0.3%) → QCL "Rice"; ~5% of paddy rice weight
    "00000037": ("00000027", 0.05),
    # SCL "Cake of maize" (0.2%) → QCL "Maize"; ~5% extraction rate
    "00000061": ("00000056", 0.05),
    # SCL "Cake, oilseeds n.e.c." (0.1%) → None (no single parent crop)
    "00000341": None,
    # SCL "Cake of linseed" (0.06%) → QCL "Linseed"; ~60% extraction rate
    "00000335": ("00000333", 0.60),
    # SCL "Cake of copra" (0.08%) → QCL "Coconuts"; ~35% extraction rate
    "00000253": ("00000249", 0.35),
    # SCL "Cake of sesame seed" (0.06%) → QCL "Sesame seed"; ~50% extraction rate
    "00000291": ("00000289", 0.50),
    # SCL "Cake of mustard seed" (<0.05%) → QCL "Mustard seed"; ~60% extraction rate
    "00000294": ("00000292", 0.60),
    # Cereal brans (by-product → parent cereal)
    # SCL "Bran of wheat" (3.5%) → QCL "Wheat"; ~25% extraction rate
    "00000017": ("00000015", 0.25),
    # SCL "Bran of rice" (2.1%) → QCL "Rice"; ~10% of paddy weight
    "00000035": ("00000027", 0.10),
    # SCL "Bran of maize" (0.7%) → QCL "Maize"; ~8% extraction rate
    "00000059": ("00000056", 0.08),
    # SCL "Bran of sorghum" (0.1%) → QCL "Sorghum"; ~15% extraction rate
    "00000085": ("00000083", 0.15),
    # SCL "Bran of millet" (0.09%) → QCL "Millet"; ~15% extraction rate
    "00000081": ("00000079", 0.15),
    # SCL "Bran of cereals n.e.c." (0.08%) → QCL "Cereals n.e.c."; ~20% extraction rate
    "00000112": ("00000108", 0.20),
    # SCL "Bran of barley" (0.07%) → QCL "Barley"; ~20% extraction rate
    "00000047": ("00000044", 0.20),
    # SCL "Bran of oats" (0.06%) → QCL "Oats"; ~25% extraction rate
    "00000077": ("00000075", 0.25),
    # Other processed crops (by-product → parent crop)
    # SCL "Cassava, dry" (1.0%) → QCL "Cassava"; ~30% of fresh weight after drying
    "00000128": ("00000125", 0.30),
    # SCL "Molasses" (0.8%) → QCL "Sugar cane"; ~3% extraction rate
    "00000165": ("00000156", 0.03),
    # SCL "Rice, broken" (0.8%) → QCL "Rice"; ~10% extraction rate
    "00000032": ("00000027", 0.10),
    # SCL "Gluten feed and meal" (0.5%) → QCL "Maize" (mainly from corn wet milling); ~25%
    "00000846": ("00000056", 0.25),
    # SCL "Germ of wheat" (0.4%) → QCL "Wheat"; ~3% extraction rate
    "00000019": ("00000015", 0.03),
    # SCL "Germ of maize" (0.2%) → QCL "Maize"; ~8% extraction rate
    "00000057": ("00000056", 0.08),
    # SCL "Maize gluten" (0.07%) → QCL "Maize"; ~6% extraction rate
    "00000063": ("00000056", 0.06),
    # SCL "Raw cane or beet sugar" (0.07%) → QCL "Sugar cane" (proxy); ~12% extraction rate
    "00000162": ("00000156", 0.12),
    # SCL "Cereal preparations" (0.07%) → None (heterogeneous)
    "00000113": None,
    # SCL "Mango pulp" (0.08%) → None (negligible, no clear QCL match)
    "00000584": None,
    # SCL "Flours and meals of oilseeds" (0.07%) → None (heterogeneous)
    "00000343": None,
    # Dairy and animal products (no cropland equivalent)
    # SCL "Whey, fresh" (2.5%) — dairy by-product
    "00000903": None,
    # SCL "Skim milk of buffalo" (1.7%) — dairy
    "00000954": None,
    # SCL "Skim milk of cows" (1.3%) — dairy
    "00000888": None,
    # SCL "Raw milk of cattle" (1.1%) — dairy
    "00000882": None,
    # SCL "Raw milk of buffalo" (0.3%) — dairy
    "00000951": None,
    # SCL "Whey, dry" (0.1%) — dairy
    "00000900": None,
    # SCL "Whey, condensed" (0.08%) — dairy
    "00000890": None,
    # SCL "Buttermilk" (0.06%) — dairy
    "00000893": None,
    # SCL "Skim milk and whey powder" (0.05%) — dairy
    "00000898": None,
    # SCL "Raw milk of camel" (0.05%) — dairy
    "00001130": None,
    # SCL "Tallow" (0.07%) — animal fat
    "00001225": None,
}


def calculate_global_feed_yield(tb_qcl):
    """Compute the global production-weighted average yield (tonnes/hectare) for feed crops, per year.

    Uses World-level Production (005510) and Area harvested (005312) for the top 5 feed crops.
    Returns a table with columns ["year", "global_feed_yield"].
    """
    tb = tb_qcl[
        (tb_qcl["country"] == "World")
        & (tb_qcl["item"].isin(FEED_CROP_NAMES))
        & (tb_qcl["element_code"].isin(["005510", "005312"]))
    ].reset_index(drop=True)

    error = f"Expected all {len(FEED_CROP_NAMES)} feed crops at World level in QCL."
    assert set(tb["item"]) == set(FEED_CROP_NAMES), error

    # Pivot to get production and area as separate columns.
    tb_pivot = tb.pivot(index=["year", "item"], columns="element_code", values="value").reset_index()
    tb_pivot.columns.name = None
    tb_pivot = tb_pivot.rename(columns={"005510": "production_qcl", "005312": "area_harvested"})

    # Global weighted average yield = total production / total area, per year.
    tb_yearly = tb_pivot.groupby("year", as_index=False).agg({"production_qcl": "sum", "area_harvested": "sum"})
    tb_yearly["global_feed_yield"] = tb_yearly["production_qcl"] / tb_yearly["area_harvested"]

    # Print some diagnostics.
    print(
        f"Global feed yield range: {tb_yearly['global_feed_yield'].min():.2f} - {tb_yearly['global_feed_yield'].max():.2f} t/ha"
    )
    print(f"Global feed yield (latest year): {tb_yearly['global_feed_yield'].iloc[-1]:.2f} t/ha")

    return tb_yearly[["year", "global_feed_yield"]]


def calculate_export_weighted_yields(tb_qcl, tb_scl):
    """Compute export-weighted global yield per QCL crop item and year.

    For each crop, the yield is the weighted average of country-level yields,
    weighted by each country's export volume (from SCL). This better reflects
    the yield of crops entering international trade.

    Returns a DataFrame with columns ["qcl_item_code", "year", "export_weighted_yield"].
    """
    import pandas as pd

    # Ensure we work with plain DataFrames (SCL is loaded from feather, QCL from catalog).
    tb_qcl = pd.DataFrame(tb_qcl)
    tb_scl = pd.DataFrame(tb_scl)
    # Get the unique QCL crop codes we need yields for.
    qcl_codes_needed = set()
    for mapping in FEED_SCL_TO_QCL_MAPPING.values():
        if mapping is not None:
            qcl_codes_needed.add(mapping[0])

    # QCL: country-level production and area harvested
    tb_yields = tb_qcl[
        (tb_qcl["item_code"].isin(qcl_codes_needed))
        & (tb_qcl["element_code"].isin(["005510", "005312"]))
        & (tb_qcl["country"] != "World")
    ].reset_index(drop=True)

    tb_yields = tb_yields.pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value"
    ).reset_index()
    tb_yields.columns.name = None
    tb_yields = tb_yields.rename(columns={"005510": "production", "005312": "area_harvested"})

    # Compute country-level yield (tonnes per hectare).
    tb_yields = tb_yields[(tb_yields["area_harvested"] > 0) & (tb_yields["production"] > 0)].copy()
    tb_yields["yield"] = tb_yields["production"] / tb_yields["area_harvested"]

    # SCL: country-level exports for the same items
    # We need to map from QCL item codes back to SCL item codes for exports.
    # For primary crops, the codes are the same. For by-products, we want the parent crop exports.
    # Simplification: use exports of the QCL item code from SCL (works for primary crops).
    tb_exports = (
        tb_scl[
            (tb_scl["item_code"].isin(qcl_codes_needed))
            & (tb_scl["element_code"] == "005910")  # Export quantity
            & (tb_scl["country"] != "World")
        ][["country", "year", "item_code", "value"]]
        .rename(columns={"value": "exports"})
        .reset_index(drop=True)
    )

    # Merge yields with exports.
    tb_merged = tb_yields.merge(tb_exports, on=["country", "year", "item_code"], how="left")
    tb_merged["exports"] = tb_merged["exports"].fillna(0)

    # For items with no export data, fall back to production-weighted yield.
    # This handles cases where SCL doesn't have the same item code as QCL.
    tb_merged["weight"] = tb_merged["exports"].where(tb_merged["exports"] > 0, tb_merged["production"])

    # Compute weighted average yield per item and year.
    tb_merged["yield_x_weight"] = tb_merged["yield"] * tb_merged["weight"]
    tb_weighted = tb_merged.groupby(["item_code", "year"], as_index=False).agg(
        {"yield_x_weight": "sum", "weight": "sum"}
    )
    tb_weighted["export_weighted_yield"] = tb_weighted["yield_x_weight"] / tb_weighted["weight"]

    return tb_weighted[["item_code", "year", "export_weighted_yield"]].rename(columns={"item_code": "qcl_item_code"})


def calculate_offshored_land_from_scl(tb_scl, tb_qcl):
    """Compute offshored land per country-year from SCL feed/import/production data.

    For each country, year, and feed item:
      1. Estimate imported feed: feed × imports / (production + imports)
      2. Convert by-products to parent crop equivalent using extraction rates
      3. Divide by the export-weighted global yield of the parent crop

    Returns a DataFrame with columns ["country", "year", "offshored_land"].
    """
    import pandas as pd

    # Ensure we work with plain DataFrames.
    tb_scl = pd.DataFrame(tb_scl)
    tb_qcl = pd.DataFrame(tb_qcl)

    # Get export-weighted yields per QCL item and year.
    tb_yields = calculate_export_weighted_yields(tb_qcl, tb_scl)

    # Get SCL data for mapped items: feed (005520), production (005510), imports (005610).
    mapped_scl_codes = set(FEED_SCL_TO_QCL_MAPPING.keys())
    tb_feed = tb_scl[
        (tb_scl["item_code"].isin(mapped_scl_codes))
        & (tb_scl["element_code"].isin(["005520", "005510", "005610"]))
        & (tb_scl["country"] != "World")
    ].reset_index(drop=True)

    # Pivot elements into columns.
    tb_feed = tb_feed.pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value"
    ).reset_index()
    tb_feed.columns.name = None
    tb_feed = tb_feed.rename(columns={"005520": "feed", "005510": "production", "005610": "imports"})
    for col in ["feed", "production", "imports"]:
        if col in tb_feed.columns:
            tb_feed[col] = tb_feed[col].fillna(0)

    # Skip items mapped to None (dairy, etc.) — they have no cropland equivalent.
    tb_feed["mapping"] = tb_feed["item_code"].map(FEED_SCL_TO_QCL_MAPPING)
    tb_feed = tb_feed[tb_feed["mapping"].notna()].copy()

    # Extract QCL parent code and extraction rate.
    tb_feed["qcl_item_code"] = tb_feed["mapping"].apply(lambda x: x[0])
    tb_feed["extraction_rate"] = tb_feed["mapping"].apply(lambda x: x[1])
    tb_feed = tb_feed.drop(columns=["mapping"])

    # Estimate imported feed per item.
    total_supply = tb_feed["production"] + tb_feed["imports"]
    import_share = (tb_feed["imports"] / total_supply).where(total_supply > 0, 0)
    tb_feed["imported_feed"] = tb_feed["feed"] * import_share

    # Convert by-product tonnage to parent crop equivalent.
    tb_feed["parent_crop_equivalent"] = tb_feed["imported_feed"] / tb_feed["extraction_rate"]

    # Merge with export-weighted yields.
    tb_feed = tb_feed.merge(tb_yields, on=["qcl_item_code", "year"], how="left")

    # Compute offshored land per item (hectares).
    tb_feed["offshored_land_item"] = (tb_feed["parent_crop_equivalent"] / tb_feed["export_weighted_yield"]).fillna(0)

    # For items without yield data, use fallback (will be handled by the caller).
    # Sum across all items per country-year.
    tb_offshored = tb_feed.groupby(["country", "year"], as_index=False).agg(
        offshored_land=("offshored_land_item", "sum"),
        imported_feed_total=("imported_feed", "sum"),
    )

    return tb_offshored


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

    # Imported food energy (kcal, per item): food from imports for domestic consumption.
    # food_domestic_energy + food_imported_energy = total food energy (from our per-item calculation).
    food_imported = tb["food"] * (1 - domestic_share)
    tb["food_imported_energy"] = food_imported * 10000 * tb["conversion"]

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
                    "food_imported_energy",
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

    metrics = [
        "food_domestic_energy",
        "food_energy",
        "agricultural_land",
        "offshored_land",
        "total_land",
    ]
    tb_start = tb_grouped[tb_grouped["year"] == year_min][["country"] + metrics]
    tb_end = tb_grouped[tb_grouped["year"] == year_max][["country"] + metrics]
    tb_changes = tb_start.merge(tb_end, on="country", suffixes=("_start", "_end"))

    tb_changes["food_domestic_energy_change"] = (
        100
        * (tb_changes["food_domestic_energy_end"] - tb_changes["food_domestic_energy_start"])
        / tb_changes["food_domestic_energy_start"]
    )
    tb_changes["food_energy_pc_change"] = (
        100 * (tb_changes["food_energy_end"] - tb_changes["food_energy_start"]) / tb_changes["food_energy_start"]
    )
    tb_changes["agricultural_land_change"] = (
        100
        * (tb_changes["agricultural_land_end"] - tb_changes["agricultural_land_start"])
        / tb_changes["agricultural_land_start"]
    )
    tb_changes["total_land_change"] = (
        100 * (tb_changes["total_land_end"] - tb_changes["total_land_start"]) / tb_changes["total_land_start"]
    )
    tb_changes["offshored_land_change"] = (
        100
        * (tb_changes["offshored_land_end"] - tb_changes["offshored_land_start"])
        / tb_changes["offshored_land_start"].replace(0, float("nan"))
    )

    # Diagnostic: print how many countries pass each criterion individually.
    cond_total_land = tb_changes["total_land_change"] <= -TOTAL_LAND_DECREASE_PCT_MIN
    cond_domestic_land = tb_changes["agricultural_land_change"] <= -LAND_DECREASE_PCT_MIN
    cond_food_total = tb_changes["food_domestic_energy_change"] >= DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN
    cond_food_pc = tb_changes["food_energy_pc_change"] >= FOOD_PC_INCREASE_PCT_MIN
    n_total = len(tb_changes)
    print(f"Total countries with data: {n_total}")
    print(f"  Total land decrease >= {TOTAL_LAND_DECREASE_PCT_MIN}%: {cond_total_land.sum()}")
    print(f"  Domestic land decrease >= {LAND_DECREASE_PCT_MIN}% (info): {cond_domestic_land.sum()}")
    print(f"  Domestic food (total) increase >= {DOMESTIC_FOOD_TOTAL_INCREASE_PCT_MIN}%: {cond_food_total.sum()}")
    print(f"  Total food (per capita) increase >= {FOOD_PC_INCREASE_PCT_MIN}%: {cond_food_pc.sum()}")
    print(f"  Total land + food (total) + food (pc): {(cond_total_land & cond_food_total & cond_food_pc).sum()}")

    # Detailed view: countries passing food (total + pc), sorted by total land change.
    both = tb_changes[cond_food_total & cond_food_pc].sort_values("total_land_change")
    print(f"\n  {'Country':30s}  {'Food tot':>9s}  {'Food pc':>8s}  {'Dom land':>9s}  {'Total land':>11s}")
    for _, row in both.iterrows():
        print(
            f"  {str(row['country']):30s}  {row['food_domestic_energy_change']:+8.1f}%  {row['food_energy_pc_change']:+7.1f}%  {row['agricultural_land_change']:+8.1f}%  {row['total_land_change']:+10.1f}%"
        )

    countries_decoupled = set(tb_changes[cond_total_land & cond_food_total & cond_food_pc]["country"])

    if plot:
        plot_countries_stacked(
            tb_grouped,
            countries_decoupled,
            year_min,
            year_max,
            output_folder=OUTPUT_FOLDER / "decoupled-countries-stacked",
        )
        plot_slope_chart_grid(tb_changes, countries_decoupled, year_min, year_max)

    return countries_decoupled


def plot_countries_stacked(
    tb_grouped,
    countries,
    year_min,
    year_max,
    output_folder=OUTPUT_FOLDER / "stacked",
):
    """Two-panel stacked charts for a given list of countries."""
    countries = sorted(countries)
    if not countries:
        print("No countries to plot (stacked).")
        return

    print(f"Plotting {len(countries)} stacked charts to {output_folder} ...")
    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)

    for country in countries:
        plot_country_stacked(tb_grouped, country, year_min, year_max, output_folder=output_folder)

    print(f"Done. {len(countries)} stacked charts saved.")


def plot_country_stacked(
    tb_grouped,
    country,
    year_min,
    year_max,
    output_folder=None,
):
    """Two-panel line chart for a single country.

    Top panel: Domestic food production, domestic food per capita, food imports, total food per capita.
    Bottom panel: Domestic land, total land (domestic + offshored).
    All shown as % change from baseline (baseline = 0).
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    cols = [
        "food_domestic_energy",
        "food_imported_energy",
        "food_energy",
        "food_domestic_energy_pc",
        "agricultural_land",
        "offshored_land",
        "total_land",
    ]
    tb_c = tb_grouped[(tb_grouped["country"] == country) & (tb_grouped["year"] >= year_min)][
        ["country", "year"] + cols
    ].reset_index(drop=True)
    if tb_c.empty:
        return

    baseline = tb_c[tb_c["year"] == year_min]
    if baseline.empty:
        return

    # Baseline values for computing % change (baseline = 0).
    food_dom_base = float(baseline["food_domestic_energy"].iloc[0])
    food_imp_base = float(baseline["food_imported_energy"].iloc[0])
    food_pc_base = float(baseline["food_energy"].iloc[0])
    food_dom_pc_base = float(baseline["food_domestic_energy_pc"].iloc[0])
    land_dom_base = float(baseline["agricultural_land"].iloc[0])
    land_total_base = float(baseline["total_land"].iloc[0])
    if food_dom_base == 0 or food_pc_base == 0 or food_dom_pc_base == 0 or land_dom_base == 0:
        return

    # Each metric as % change from baseline (baseline = 0).
    food_dom = 100 * (tb_c["food_domestic_energy"] / food_dom_base - 1)
    food_imp = (
        100 * (tb_c["food_imported_energy"] / food_imp_base - 1)
        if food_imp_base > 0
        else tb_c["food_imported_energy"] * 0
    )
    food_pc_total = 100 * (tb_c["food_energy"] / food_pc_base - 1)
    food_pc_dom = 100 * (tb_c["food_domestic_energy_pc"] / food_dom_pc_base - 1)
    land_dom = 100 * (tb_c["agricultural_land"] / land_dom_base - 1)
    land_total = 100 * (tb_c["total_land"] / land_total_base - 1)
    years = tb_c["year"]

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=["Food (% change from baseline)", "Land use (% change from baseline)"],
    )

    # Top panel: food lines.
    fig.add_trace(
        go.Scatter(
            x=years,
            y=food_dom,
            name="Domestic food production",
            mode="lines",
            line=dict(color="rgba(0,100,200,0.6)", width=2),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=food_pc_dom,
            name="Domestic food per capita",
            mode="lines",
            line=dict(color="rgb(0,50,150)", width=2.5),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=food_imp,
            name="Food imports",
            mode="lines",
            line=dict(color="gray", width=1.5),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=food_pc_total,
            name="Total food per capita",
            mode="lines",
            line=dict(color="gray", width=1.5, dash="dash"),
        ),
        row=1,
        col=1,
    )

    # Bottom panel: land lines.
    fig.add_trace(
        go.Scatter(
            x=years,
            y=land_dom,
            name="Domestic land",
            mode="lines",
            line=dict(color="rgb(200,50,50)", width=2),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=years,
            y=land_total,
            name="Total land (incl. offshored feed)",
            mode="lines",
            line=dict(color="rgb(140,30,30)", width=2.5),
        ),
        row=2,
        col=1,
    )

    # Horizontal baseline at 0 and vertical line at 2010 (SCL data start) on both panels.
    for row in [1, 2]:
        fig.add_hline(y=0, line_dash="solid", line_color="gray", line_width=1, opacity=0.5, row=row, col=1)
        fig.add_vline(x=2010, line_dash="dash", line_color="gray", line_width=1, opacity=0.5, row=row, col=1)

    fig.update_layout(
        height=600,
        width=800,
        title_text=country,
        title_font_size=20,
        legend=dict(x=1.02, y=0.5, xanchor="left", yanchor="middle", traceorder="normal"),
        margin=dict(r=250),
    )

    if output_folder is not None:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
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
    # Remove metadata units to avoid spurious warnings (only if using owid catalog Tables).
    for column in ["food_domestic_energy_change", "total_land_change"]:
        if hasattr(tb_decoupled[column], "metadata"):
            tb_decoupled[column].metadata.unit = None
            tb_decoupled[column].metadata.short_unit = None

    # Sort by "decoupling score" (domestic food increase minus total land change).
    tb_decoupled["decoupling_score"] = tb_decoupled["food_domestic_energy_change"] - tb_decoupled["total_land_change"]
    tb_decoupled = tb_decoupled.sort_values("decoupling_score", ascending=False)

    countries_sorted = list(tb_decoupled["country"])
    n_countries = len(countries_sorted)
    n_rows = (n_countries + n_cols - 1) // n_cols

    # Calculate global y-axis range for consistent scaling.
    y_max_val = tb_decoupled["food_domestic_energy_change"].max()
    y_min_val = tb_decoupled["total_land_change"].min()
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
        land_change = float(country_data["total_land_change"])

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
        title_text=f"Countries that increased domestic food production while reducing total land footprint, {year_min}-{year_max}",
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

    # Load FAOSTAT production dataset (for feed crop yields).
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl").reset_index()

    # Load FAOSTAT Supply Utilization Accounts (for comprehensive feed data including oilseed cakes, bran, etc.).
    # TODO: Replace with paths.load_dataset("faostat_scl") once a new SCL version is imported.
    import pyarrow.feather as feather

    _SCL_PATH = Path(__file__).parents[6] / "data/garden/faostat/2025-03-10/faostat_scl/faostat_scl.feather"
    # Load only the columns we need to save memory (the full SCL feather is ~10M rows).
    tb_scl = feather.read_table(
        str(_SCL_PATH), columns=["country", "year", "item_code", "element_code", "value"]
    ).to_pandas()
    # Convert categorical columns to plain types to avoid issues with groupby/fillna.
    for col in tb_scl.select_dtypes(include=["category"]).columns:
        tb_scl[col] = tb_scl[col].astype(str)

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

    # Remove unnecessary columns.
    tb_grouped = tb_grouped.drop(columns=["production_energy_uncorrected", "food_quantity"], errors="raise")

    # Compute offshored land from SCL data (comprehensive feed including oilseed cakes, brans, etc.).
    # SCL covers 2010-2022; for earlier years, offshored land is assumed to be zero
    # (international feed trade was negligible before the 1970s-80s).
    tb_offshored = calculate_offshored_land_from_scl(tb_scl=tb_scl, tb_qcl=tb_qcl)

    # Merge offshored land into tb_grouped. Use pandas merge since tb_offshored is a plain DataFrame.
    # Store metadata-bearing columns, merge as plain DataFrames, then restore.
    import pandas as pd
    from owid.catalog import Table

    _tb = pd.DataFrame(tb_grouped).merge(
        tb_offshored[["country", "year", "offshored_land"]], on=["country", "year"], how="left"
    )
    tb_grouped = Table(_tb)
    # Forward-fill offshored land per country so that years beyond SCL range (2023+) use the last
    # known value instead of 0. Then fill remaining NaNs (years before SCL, i.e. pre-2010) with 0.
    tb_grouped["offshored_land"] = tb_grouped.groupby("country")["offshored_land"].ffill().fillna(0)

    # Total land footprint = domestic agricultural land + offshored land.
    tb_grouped["total_land"] = tb_grouped["agricultural_land"] + tb_grouped["offshored_land"]

    # Apply a rolling average of ROLLING_AVERAGE_YEARS (defined above) on all indicators.
    tb_grouped = apply_rolling_average(tb_grouped=tb_grouped)

    # Select countries that achieved decoupling: production increased and land use decreased over the full time window.
    # Set plot=True to generate individual country charts and a slope chart grid.
    countries_decoupled = detect_decoupled_countries(tb_grouped, plot=True)

    # Filter to decoupled countries.
    tb_grouped = tb_grouped[tb_grouped["country"].isin(countries_decoupled)].reset_index(drop=True)

    # Improve table format.
    tb_grouped = tb_grouped.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_grouped], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
