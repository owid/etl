"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("monthly_burned_area")

    # Read table from meadow dataset.
    tb = ds_meadow["monthly_burned_area"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    area_types = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]

    # Sum the burned area by country, year, and month
    grouped_tb = tb.groupby(["country", "year", "month"], observed=True)[area_types].sum().reset_index()

    # Create a date column
    grouped_tb["date"] = pd.to_datetime(grouped_tb["year"].astype(str) + "-" + grouped_tb["month"].astype(str) + "-01")

    aggregations = {emission: "sum" for emission in area_types}

    # Add region aggregates.
    grouped_tb = geo.add_regions_to_table(
        grouped_tb,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        year_col="date",
    )

    # Create a variable with days since column
    grouped_tb["days_since_2000"] = (grouped_tb["date"] - pd.to_datetime("2000-01-01")).dt.days
    # Make sure there are no NaN values in 'year' and 'month' columns after aggregations
    grouped_tb["year"] = grouped_tb["date"].dt.year
    grouped_tb["month"] = grouped_tb["date"].dt.month

    grouped_tb = grouped_tb.drop(columns=["date"])

    grouped_tb["all"] = grouped_tb[area_types].sum(axis=1)

    grouped_tb = grouped_tb.format(["country", "year", "month", "days_since_2000"])

    #
    # Sanity checks.
    #
    area_cols = area_types + ["all"]

    # No negative values
    assert (grouped_tb[area_cols] >= 0).all().all(), "Negative burned area values found."

    # 2024 should be entirely zero (placeholder year in source data)
    if 2024 in grouped_tb.index.get_level_values("year"):
        assert (grouped_tb.loc[grouped_tb.index.get_level_values("year") == 2024, area_cols] == 0).all().all(), (
            "Expected 2024 data to be all zeros (placeholder), but found non-zero values."
        )

    # Global yearly total (World) should be in a plausible range (200M–600M ha/year)
    world_yearly = (
        grouped_tb.loc[grouped_tb.index.get_level_values("country") == "World"].groupby("year")[area_cols].sum()
    )
    plausible_years = world_yearly[world_yearly.index < 2024]
    assert (plausible_years["all"] > 200_000_000).all(), (
        "World yearly burned area unexpectedly low (< 200M ha) for some year."
    )
    assert (plausible_years["all"] < 600_000_000).all(), (
        "World yearly burned area unexpectedly high (> 600M ha) for some year."
    )

    # Savannas should be the dominant land cover globally each year
    assert (plausible_years["savannas"] > plausible_years["forest"]).all(), (
        "Savannas should exceed forests in global yearly burned area."
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[grouped_tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
