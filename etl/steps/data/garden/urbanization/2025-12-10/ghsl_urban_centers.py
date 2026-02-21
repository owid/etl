"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

START_OF_PROJECTIONS = 2025

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
    "below_300k": (0, 300000),
    "300k_1m": (300000, 1000000),
    "1m_3m": (1000000, 3000000),
    "3m_5m": (3000000, 5000000),
    "above_5m": (5000000, float("inf")),
}


def calculate_citysize_growth_rates(tb, data_type):
    """Calculate annualized exponential growth rate of population in cities >= 300k.

    For each country, compute the annualized rate between observations:
        rate_t = 100 * ln(P_t / P_{t-prev}) / (year_t - year_prev)

    This produces UN F04-style rates. For 5-year intervals, year_t - year_prev = 5.
    The formula assumes constant exponential growth between observations.

    Args:
        tb: Table with pop_citysize_above_300k_{data_type} column
        data_type: Either "estimates" or "projections"

    Notes:
    - Returns NaN when previous is missing/zero, current is missing, or ratio <= 0.
    - Handles irregular year gaps automatically.
    """
    col_name = f"pop_citysize_above_300k_{data_type}"

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
        tb[f"pop_citysize_above_300k_growth_{data_type}"] = 100 * (log_ratio / year_step)

    return tb


def run() -> None:
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
    tb_countries = ds_countries.read("ghsl_countries").reset_index()
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
    tb_total_pop = tb_total_pop[["country", "year", "total_population", "urban_population"]]

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # The meadow step already has capitals, top 100 cities, and city size data merged.
    # We need to separate them to add regional aggregates for capitals only and city sizes.

    # Extract rows with capital data (have urban_pop but no urban_pop_top_100).
    has_capital_data = tb["urban_pop"].notna()
    has_top_100_data = tb["urban_pop_top_100"].notna()

    # Identify city size columns.
    city_size_cols = [f"pop_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()]
    has_city_size_data = tb[city_size_cols[0]].notna()  # Check first column

    # Create capital-only table for regional aggregates.
    tb_capitals = tb[has_capital_data & ~has_top_100_data].copy()
    tb_capitals = tb_capitals[["country", "year", "urban_pop", "urban_density"]]

    # Add region aggregates for capitals only.
    tb_capitals = geo.add_regions_to_table(
        tb_capitals,
        aggregations={"urban_pop": "sum"},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Calculate share of urban population living in largest city (capital).
    # Merge with urban population data.
    tb_capitals_share = pr.merge(
        tb_capitals[["country", "year", "urban_pop"]],
        tb_total_pop[["country", "year", "urban_population"]],
        on=["country", "year"],
        how="left",
    )
    tb_capitals_share["urban_pop_share_largest_city"] = (
        tb_capitals_share["urban_pop"] / tb_capitals_share["urban_population"]
    ) * 100
    # Keep only the share column.
    tb_capitals_share = tb_capitals_share[["country", "year", "urban_pop_share_largest_city"]]

    # Merge share back to capitals table.
    tb_capitals = pr.merge(tb_capitals, tb_capitals_share, on=["country", "year"], how="left")

    # Create top 100 only table.
    tb_top_100 = tb[has_top_100_data].copy()
    tb_top_100 = tb_top_100[["country", "year", "urban_pop_top_100", "urban_density_top_100"]]

    # Create city size table and add regional aggregates.
    tb_city_sizes = tb[has_city_size_data].copy()
    city_size_cols_with_meta = ["country", "year"] + city_size_cols
    tb_city_sizes = tb_city_sizes[city_size_cols_with_meta]

    # Add regional aggregates for city sizes.
    city_size_agg = {col: "sum" for col in city_size_cols}
    tb_city_sizes = geo.add_regions_to_table(
        tb_city_sizes,
        aggregations=city_size_agg,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Merge with total population to calculate shares.
    tb_city_sizes = pr.merge(
        tb_city_sizes, tb_total_pop[["country", "year", "total_population"]], on=["country", "year"], how="left"
    )

    # Calculate shares as percentage of total population.
    for size_name in CITY_SIZE_CUTOFFS.keys():
        tb_city_sizes[f"popshare_citysize_{size_name}"] = (
            tb_city_sizes[f"pop_{size_name}"] / tb_city_sizes["total_population"]
        ) * 100

    # Rename absolute population columns to be more descriptive.
    for size_name in CITY_SIZE_CUTOFFS.keys():
        tb_city_sizes = tb_city_sizes.rename(columns={f"pop_{size_name}": f"pop_citysize_{size_name}"})

    # Calculate population in cities >= 300k (300k_1m + 1m_3m + 3m_5m + above_5m).
    tb_city_sizes["pop_citysize_above_300k"] = (
        tb_city_sizes["pop_citysize_300k_1m"]
        + tb_city_sizes["pop_citysize_1m_3m"]
        + tb_city_sizes["pop_citysize_3m_5m"]
        + tb_city_sizes["pop_citysize_above_5m"]
    )

    # Calculate share of population in cities >= 300k.
    tb_city_sizes["popshare_citysize_above_300k"] = (
        tb_city_sizes["pop_citysize_above_300k"] / tb_city_sizes["total_population"]
    ) * 100

    # Drop total population.
    tb_city_sizes = tb_city_sizes.drop(columns=["total_population"])

    # Merge capitals, top 100, and city sizes.
    tb = pr.merge(tb_capitals, tb_top_100, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_city_sizes, on=["country", "year"], how="outer")

    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()

    # For each column, split it into two (projections and estimates).
    columns_to_split = [
        "urban_pop",
        "urban_density",
        "urban_density_top_100",
        "urban_pop_top_100",
        "urban_pop_share_largest_city",
    ]
    # Add city size population and share columns.
    columns_to_split.extend([f"pop_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    columns_to_split.extend([f"popshare_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    # Add aggregate columns for cities >= 300k.
    columns_to_split.extend(["pop_citysize_above_300k", "popshare_citysize_above_300k"])

    for col in columns_to_split:
        if col in tb.columns:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
            past_estimates = past_estimates.drop(columns=[col], errors="ignore")
            future_projections = future_projections.drop(columns=[col], errors="ignore")

    # Merge past estimates and future projections.
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    # Calculate growth rates for estimates and projections separately.
    tb = calculate_citysize_growth_rates(tb, "estimates")
    tb = calculate_citysize_growth_rates(tb, "projections")

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
