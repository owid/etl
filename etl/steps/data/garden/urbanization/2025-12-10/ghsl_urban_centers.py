"""Load a meadow dataset and create a garden dataset."""

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
    "1m_5m": (1000000, 5000000),
    "above_5m": (5000000, float("inf")),
}


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

    # Load total population from ghsl_countries for share calculations.
    ds_countries = paths.load_dataset("ghsl_countries")
    tb_countries = ds_countries.read("ghsl_countries").reset_index()
    # Get total population by combining all three location types.
    tb_total_pop = tb_countries[
        (tb_countries["metric"] == "population")
        & (tb_countries["location_type"].isin(["urban_centre", "urban_cluster", "rural_total"]))
        & (tb_countries["data_type"].isin(["estimates", "projections"]))
    ].copy()
    tb_total_pop = tb_total_pop.pivot_table(index=["country", "year"], columns="location_type", values="value").reset_index()
    tb_total_pop["total_population"] = tb_total_pop[["urban_centre", "urban_cluster", "rural_total"]].sum(axis=1)
    tb_total_pop = tb_total_pop[["country", "year", "total_population"]]

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
    tb_city_sizes = pr.merge(tb_city_sizes, tb_total_pop, on=["country", "year"], how="left")

    # Calculate shares as percentage of total population.
    for size_name in CITY_SIZE_CUTOFFS.keys():
        tb_city_sizes[f"popshare_citysize_{size_name}"] = (
            tb_city_sizes[f"pop_{size_name}"] / tb_city_sizes["total_population"]
        ) * 100

    # Rename absolute population columns to be more descriptive.
    for size_name in CITY_SIZE_CUTOFFS.keys():
        tb_city_sizes = tb_city_sizes.rename(columns={f"pop_{size_name}": f"pop_citysize_{size_name}"})

    # Drop total population.
    tb_city_sizes = tb_city_sizes.drop(columns=["total_population"])

    # Merge capitals, top 100, and city sizes.
    tb = pr.merge(tb_capitals, tb_top_100, on=["country", "year"], how="outer")
    tb = pr.merge(tb, tb_city_sizes, on=["country", "year"], how="outer")

    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()

    # For each column, split it into two (projections and estimates).
    columns_to_split = ["urban_pop", "urban_density", "urban_density_top_100", "urban_pop_top_100"]
    # Add city size population and share columns.
    columns_to_split.extend([f"pop_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])
    columns_to_split.extend([f"popshare_citysize_{size_name}" for size_name in CITY_SIZE_CUTOFFS.keys()])

    for col in columns_to_split:
        if col in tb.columns:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
            past_estimates = past_estimates.drop(columns=[col], errors="ignore")
            future_projections = future_projections.drop(columns=[col], errors="ignore")

    # Merge past estimates and future projections.
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
