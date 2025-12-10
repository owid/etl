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
]


def run(dest_dir: str) -> None:
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

    #
    # Process data.
    #

    tb = tb.reset_index()
    tb = paths.regions.harmonize_names(tb)

    # The meadow step already has capitals and top 100 cities merged.
    # We need to separate them to add regional aggregates for capitals only.

    # Extract rows with capital data (have urban_pop but no urban_pop_top_100).
    has_capital_data = tb["urban_pop"].notna()
    has_top_100_data = tb["urban_pop_top_100"].notna()

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

    # Merge capitals and top 100 cities.
    tb = pr.merge(tb_capitals, tb_top_100, on=["country", "year"], how="outer")

    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()

    # For each column, split it into two (projections and estimates).
    for col in ["urban_pop", "urban_density", "urban_density_top_100", "urban_pop_top_100"]:
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
