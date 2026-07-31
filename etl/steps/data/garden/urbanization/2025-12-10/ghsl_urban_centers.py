"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

START_OF_PROJECTIONS = 2025

# The source rates the plausibility of every city's population figure (0 = low, 1 = moderate,
# 2 = high, 3 = very high) and the meadow step carries that rating through. We do not publish the
# figures it rates "low", because in those cases the population is sometimes placed in the wrong
# settlement: in Angola the source puts ~1 million people in the small town of Uíge (1.01M in 29 km²)
# while Luanda's urban centre is absent until 1995, and it flags exactly those figures as low. The
# same pattern shows up in Eritrea, South Sudan, Comoros and Cabo Verde.
#
# Two limits on that, both deliberate:
#
# 1. Only indicators built from ONE named city are filtered (capital, largest city, top 100). Every
#    aggregate is left alone -- the city-size buckets, and the regional sums of capital population.
#    An aggregate sums many cities, so dropping a misplaced one would delete people who really do
#    live in that country. Filtering the city-size aggregates would also be far more destructive than
#    it looks: 38 country-years would lose every one of their cities, and Bangladesh, Yemen, Sudan and
#    Iran would each lose a fifth to two-thirds of their urban population.
# 2. Only the historical estimates are filtered, never the projections. From 2025 on the rating mostly
#    marks ordinary forecast uncertainty for small capitals rather than misplacement -- Vientiane is
#    rated low from 2025, but it really is the largest city in Laos.
LOW_PLAUSIBILITY = 0

# Regions for which aggregates will be created.
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]

# City size cutoffs (in population).
CITY_SIZE_CUTOFFS = {
    "50k_300k": (50000, 300000),
    "50k_1m": (50000, 1000000),
    "1m_5m": (1000000, 5000000),
    "5m_10m": (5000000, 10000000),
    "above_10m": (10000000, float("inf")),
}


def blank_low_plausibility(tb, value_columns, flag_column):
    """Blank the values the source rates as low plausibility, in the historical estimates only.

    Blanks rather than drops, and never re-ranks: for the largest-city indicator the meadow step has
    already picked the winning city, so removing the row here would let a later step fall through to
    the second-biggest city and publish that as the largest.

    Rows without a flag (regional and income-group aggregates, which carry NaN) are always left alone.
    """
    unreliable = (tb[flag_column] == LOW_PLAUSIBILITY) & (tb["year"] < START_OF_PROJECTIONS)
    tb.loc[unreliable.fillna(False).astype(bool), value_columns] = float("nan")
    return tb.drop(columns=[flag_column])


def calculate_population_shares(tb_data, tb_total_pop, pop_column, share_prefix):
    """Calculate share of urban/total population for a given population column.

    Args:
        tb_data: Table with country, year, and population column
        tb_total_pop: Table with urban_population and total_population
        pop_column: Name of the population column to calculate shares for
        share_prefix: Prefix for the output share column names (e.g., "capital", "largest_city")

    Returns:
        Table with two new columns:
        - urban_pop_share_{share_prefix}: population as % of urban population
        - total_pop_share_{share_prefix}: population as % of total population
    """
    tb_share = pr.merge(
        tb_data[["country", "year", pop_column]],
        tb_total_pop[["country", "year", "urban_population", "total_population"]],
        on=["country", "year"],
        how="left",
    )

    tb_share[f"urban_pop_share_{share_prefix}"] = (tb_share[pop_column] / tb_share["urban_population"]) * 100
    tb_share[f"total_pop_share_{share_prefix}"] = (tb_share[pop_column] / tb_share["total_population"]) * 100

    return tb_share[["country", "year", f"urban_pop_share_{share_prefix}", f"total_pop_share_{share_prefix}"]]


def calculate_citysize_growth_rates(tb, data_type):
    """Calculate annualized exponential growth rate of population share in cities >= 300k.

    For each country, compute the annualized rate of the share between observations:
        rate_t = 100 * ln(share_t / share_{t-prev}) / (year_t - year_prev)

    This produces UN F04-style rates. For 5-year intervals, year_t - year_prev = 5.
    The formula assumes constant exponential growth between observations.

    Args:
        tb: Table with popshare_citysize_above_300k_{data_type} column
        data_type: Either "estimates" or "projections"

    Notes:
    - Returns NaN when previous is missing/zero, current is missing, or ratio <= 0.
    - Handles irregular year gaps automatically.
    """
    col_name = f"popshare_citysize_above_300k_{data_type}"

    if col_name not in tb.columns:
        return tb

    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)

    # Compute the year step per country (handles 5-year data and irregular gaps)
    year_prev = tb.groupby("country")["year"].shift(1)
    year_step = tb["year"] - year_prev  # e.g. 5 for five-year intervals

    current = tb[col_name]
    previous = tb.groupby("country")[col_name].shift(1)

    with np.errstate(divide="ignore", invalid="ignore"):
        # Valid only when we have a positive previous, a non-missing current, and a positive year step
        valid = previous.notna() & current.notna() & (previous > 0) & (year_step.notna()) & (year_step > 0)

        ratio = np.where(valid, current / previous, np.nan)
        log_ratio = np.where((ratio > 0) & np.isfinite(ratio), np.log(ratio), np.nan)

        # Annualized percent log growth rate
        tb[f"popshare_citysize_above_300k_growth_{data_type}"] = 100 * (log_ratio / year_step)

    return tb


def run() -> None:
    """Process GHSL urban centers data: capitals, largest cities, top 100 cities, and city size distributions.

    Data Flow:
    1. Load merged meadow table (capitals + largest cities + top 100 + city sizes)
    2. Separate and process each data type:
       - Capitals: Add regional aggregates, calculate shares
       - Largest cities: Calculate shares (country-level only)
       - Top 100 cities: Keep as-is
       - City sizes: Add regional aggregates, calculate shares
    3. Merge all tables back together
    4. Split into historical estimates and future projections
    5. Calculate growth rates
    6. Validate and save

    Key Concepts:
    - Capital: Official administrative capital (may not be the largest city)
    - Largest city: City with highest population (determined by sorting)
    - Regional aggregates: Sum of values across countries (where meaningful)
    - Shares: Percentage of urban or total population
    """
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_urban_centers")
    # Read table from meadow dataset.
    tb = ds_meadow.read("ghsl_urban_centers")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load total and urban population from ghsl_countries for share calculations.
    ds_countries = paths.load_dataset("ghsl_countries")
    tb_countries = ds_countries.read("ghsl_countries")
    # Get total population by combining all three location types.
    tb_total_pop = tb_countries[
        (tb_countries["metric"] == "population")
        & (tb_countries["location_type"].isin(["urban_centre", "urban_cluster", "rural_total"]))
        & (tb_countries["data_type"].isin(["estimates", "projections"]))
    ].copy()
    tb_total_pop = tb_total_pop.pivot_table(
        index=["country", "year"], columns="location_type", values="value"
    ).reset_index()
    tb_total_pop["total_population"] = tb_total_pop[["urban_centre", "urban_cluster", "rural_total"]].sum(axis=1)
    tb_total_pop["urban_population"] = tb_total_pop[["urban_centre", "urban_cluster"]].sum(axis=1)
    tb_total_pop = tb_total_pop[["country", "year", "total_population", "urban_population", "urban_cluster"]]

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Identify different data types in the merged meadow table.
    # The meadow step merges: capitals, largest cities, top 100 cities, and city size aggregates.
    # We need to separate them to process each type appropriately.
    has_capital_data = tb["urban_pop"].notna()
    has_largest_city_data = tb["largest_city_pop"].notna()
    has_top_100_data = tb["urban_pop_top_100"].notna()

    # Identify city size columns.
    city_size_cols = [f"pop_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()]
    city_size_cols.extend(["pop_above_300k", "pop_above_1m"])
    has_city_size_data = tb[city_size_cols[0]].notna()

    ####################################################################################################################
    # SECTION 1: Process capital city data
    ####################################################################################################################
    # Extract capital data (exclude top 100 city rows which have different country names like "Paris (France)").
    tb_capitals = tb[has_capital_data & ~has_top_100_data].copy()
    tb_capitals = tb_capitals[["country", "year", "urban_pop", "urban_density", "urban_pop_plausibility"]]

    # Add regional aggregates (sum of capital populations across countries in each region).
    # Deliberately done BEFORE blanking the low-plausibility capitals below, so that regional and
    # income-group totals still count every capital the source reports. Those sums are aggregates, and
    # aggregates keep the people throughout this step. Blanking first would silently lower Africa's
    # total for 1995-2005 by up to 1.6%.
    tb_capitals = geo.add_regions_to_table(
        tb_capitals,
        aggregations={"urban_pop": "sum"},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Now blank the individual capitals rated low plausibility. Region rows carry no flag, so they and
    # their totals are untouched.
    tb_capitals = blank_low_plausibility(tb_capitals, ["urban_pop", "urban_density"], "urban_pop_plausibility")

    # Calculate capital city shares (% of urban/total population living in capital cities).
    # This works for both individual countries and regional aggregates.
    tb_capitals_share = calculate_population_shares(tb_capitals, tb_total_pop, "urban_pop", "capital")
    tb_capitals = pr.merge(tb_capitals, tb_capitals_share, on=["country", "year"], how="left")

    ####################################################################################################################
    # SECTION 2: Process largest city data
    ####################################################################################################################
    # Extract largest city data (individual countries only, no regional aggregates).
    # Note: "Largest city" is determined purely by population, which may differ from the capital.
    tb_largest_city = tb[has_largest_city_data & ~has_top_100_data].copy()
    tb_largest_city = tb_largest_city[
        ["country", "year", "largest_city_pop", "largest_city_density", "largest_city_plausibility"]
    ]

    # Blank the largest cities rated low plausibility, before the shares are derived from them, so the
    # shares go blank with their numerator instead of being computed from a figure we won't publish.
    tb_largest_city = blank_low_plausibility(
        tb_largest_city, ["largest_city_pop", "largest_city_density"], "largest_city_plausibility"
    )

    # Calculate largest city shares (% of urban/total population living in the largest city).
    # No regional aggregates - can't meaningfully sum "largest city" across countries.
    # We only keep the shares, not the raw population/density values.
    tb_largest_city = calculate_population_shares(tb_largest_city, tb_total_pop, "largest_city_pop", "largest_city")

    ####################################################################################################################
    # SECTION 3: Process top 100 cities data
    ####################################################################################################################
    # Extract top 100 cities (these have country names like "Paris (France)").
    tb_top_100 = tb[has_top_100_data].copy()
    tb_top_100 = tb_top_100[
        ["country", "year", "urban_pop_top_100", "urban_density_top_100", "urban_pop_top_100_plausibility"]
    ]

    # Blank the city-years rated low plausibility. Each row is one named city in one year, so this
    # touches nothing else; the city itself stays in the top 100, since membership is fixed by 2020
    # population and should not depend on the rating of a single year's figure.
    tb_top_100 = blank_low_plausibility(
        tb_top_100, ["urban_pop_top_100", "urban_density_top_100"], "urban_pop_top_100_plausibility"
    )

    ####################################################################################################################
    # SECTION 4: Process city size aggregates
    ####################################################################################################################
    # Extract city size data (population living in cities of various size categories).
    tb_city_sizes = tb[has_city_size_data].copy()
    tb_city_sizes = tb_city_sizes[["country", "year"] + city_size_cols]

    # Add regional aggregates for city sizes (sum populations across countries).
    city_size_agg = {col: "sum" for col in city_size_cols}
    tb_city_sizes = geo.add_regions_to_table(
        tb_city_sizes,
        aggregations=city_size_agg,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Merge with total and urban population to calculate shares.
    tb_city_sizes = pr.merge(
        tb_city_sizes,
        tb_total_pop[["country", "year", "total_population", "urban_population", "urban_cluster"]],
        on=["country", "year"],
        how="left",
    )

    # Rename columns from meadow naming (pop_X) to garden naming (pop_citysize_X).
    rename_dict = {f"pop_{size_name}": f"pop_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()}
    rename_dict.update(
        {
            "pop_above_300k": "pop_citysize_above_300k",
            "pop_above_1m": "pop_citysize_above_1m",
            "urban_cluster": "pop_citysize_below_50k",  # Small cities/towns (below 50k).
        }
    )
    tb_city_sizes = tb_city_sizes.rename(columns=rename_dict)

    # Propagate metadata for below_50k column (comes from ghsl_countries dataset).
    tb_city_sizes["pop_citysize_below_50k"].metadata.origins = tb_countries["value"].metadata.origins

    # Calculate shares as percentage of urban population (for all city size categories).
    for size_name in CITY_SIZE_CUTOFFS.keys():
        tb_city_sizes[f"popshare_citysize_{size_name}"] = (
            tb_city_sizes[f"pop_citysize_{size_name}"] / tb_city_sizes["urban_population"]
        ) * 100

    # Calculate shares for aggregate categories (above_300k, above_1m, below_50k).
    for col in ["above_300k", "above_1m", "below_50k"]:
        tb_city_sizes[f"popshare_citysize_{col}"] = (
            tb_city_sizes[f"pop_citysize_{col}"] / tb_city_sizes["urban_population"]
        ) * 100

    # Calculate share as percentage of total population (only for above_1m).
    tb_city_sizes["totalshare_citysize_above_1m"] = (
        tb_city_sizes["pop_citysize_above_1m"] / tb_city_sizes["total_population"]
    ) * 100

    # Drop temporary columns.
    tb_city_sizes = tb_city_sizes.drop(columns=["total_population", "urban_population"])

    ####################################################################################################################
    # SECTION 5: Merge all processed tables
    ####################################################################################################################
    # Combine all the separate tables back together (city sizes kept for all countries here
    # so that growth rates can be calculated correctly; non-region rows will be nulled out later).
    tb = pr.merge(tb_capitals, tb_largest_city, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_top_100, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_city_sizes, on=["country", "year"], how="outer")

    ####################################################################################################################
    # SECTION 6: Handle cross-border cities and split estimates/projections
    ####################################################################################################################
    # Exclude share columns for cross-border cities (Gibraltar, Macao, Monaco).
    # These cities span multiple countries, so shares relative to country population are misleading.
    CROSS_BORDER_CITIES = ["Gibraltar", "Macao", "Monaco"]
    share_cols_all = [col for col in tb.columns if "share" in col and "growth" not in col]
    tb.loc[tb["country"].isin(CROSS_BORDER_CITIES), share_cols_all] = None

    # Split all metrics into "_estimates" (historical) and "_projections" (future) columns.
    # This allows users to distinguish between observed data and model projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()  # Include overlap year.

    # Define all columns that need to be split.
    columns_to_split = [
        # Capital city metrics.
        "urban_pop",
        "urban_density",
        "urban_pop_share_capital",
        "total_pop_share_capital",
        # Largest city metrics (only shares, not raw pop/density).
        "urban_pop_share_largest_city",
        "total_pop_share_largest_city",
        # Top 100 cities metrics.
        "urban_density_top_100",
        "urban_pop_top_100",
    ]
    # City size distribution columns (regions only, but split same way).
    columns_to_split.extend([f"pop_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    columns_to_split.extend([f"popshare_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    columns_to_split.extend(
        [
            "pop_citysize_above_300k",
            "popshare_citysize_above_300k",
            "pop_citysize_above_1m",
            "popshare_citysize_above_1m",
            "pop_citysize_below_50k",
            "popshare_citysize_below_50k",
            "totalshare_citysize_above_1m",
        ]
    )

    # Split each column into _estimates and _projections.
    for col in columns_to_split:
        if col in tb.columns:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
            past_estimates = past_estimates.drop(columns=[col], errors="ignore")
            future_projections = future_projections.drop(columns=[col], errors="ignore")

    # Merge estimates and projections back together.
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    ####################################################################################################################
    # SECTION 7: Calculate growth rates
    ####################################################################################################################
    tb_for_proj_growth = tb.copy()
    tb_for_proj_growth["popshare_citysize_above_300k_projections"] = tb_for_proj_growth[
        "popshare_citysize_above_300k_projections"
    ].fillna(tb_for_proj_growth["popshare_citysize_above_300k_estimates"])

    tb = calculate_citysize_growth_rates(tb, "estimates")
    tb_for_proj_growth = calculate_citysize_growth_rates(tb_for_proj_growth, "projections")
    tb["popshare_citysize_above_300k_growth_projections"] = tb_for_proj_growth[
        "popshare_citysize_above_300k_growth_projections"
    ]

    # Null out fine-grained city size breakdown for non-region countries.
    # Broad aggregates (above_300k, above_1m, below_50k, totalshare_above_1m) are kept for all countries.
    fine_grained_cols = (
        [f"pop_citysize_{s}_estimates" for s in CITY_SIZE_CUTOFFS.keys()]
        + [f"pop_citysize_{s}_projections" for s in CITY_SIZE_CUTOFFS.keys()]
        + [f"popshare_citysize_{s}_estimates" for s in CITY_SIZE_CUTOFFS.keys()]
        + [f"popshare_citysize_{s}_projections" for s in CITY_SIZE_CUTOFFS.keys()]
    )
    non_region_mask = ~tb["country"].isin(REGIONS)
    for col in fine_grained_cols:
        if col in tb.columns:
            tb.loc[non_region_mask, col] = None

    ####################################################################################################################
    # SECTION 8: Format and validate
    ####################################################################################################################
    # Set index and ensure proper formatting.
    tb = tb.format(["country", "year"])

    # Sanity check: Ensure no share values exceed 100% (would indicate data quality issues).
    share_columns = [col for col in tb.columns if "share" in col and "growth" not in col and col != "country"]
    for col in share_columns:
        max_value = tb[col].max()
        if max_value > 100:
            problematic = tb[tb[col] > 100][["country", "year", col]].reset_index(drop=True)
            raise AssertionError(
                f"Column {col} has values exceeding 100%. This indicates data quality issues.\n"
                f"Problematic rows:\n{problematic.head(10)}"
            )

    ####################################################################################################################
    # SECTION 9: Save outputs
    ####################################################################################################################
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
