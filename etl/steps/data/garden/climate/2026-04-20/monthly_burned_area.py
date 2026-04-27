"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("monthly_burned_area")
    tb = ds_meadow["monthly_burned_area"].reset_index()

    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    area_types = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]

    # Sum burned area by country and year
    tb = tb.groupby(["country", "year"], observed=True)[area_types].sum().reset_index()

    aggregations = {col: "sum" for col in area_types}

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    tb["all"] = tb[area_types].sum(axis=1)

    tb = tb.format(["country", "year"])

    #
    # Sanity checks.
    #
    area_cols = area_types + ["all"]

    # No negative values
    assert (tb[area_cols] >= 0).all().all(), "Negative burned area values found."

    # Global yearly total (World) should be in a plausible range (200M–600M ha/year)
    world_yearly = tb.loc[tb.index.get_level_values("country") == "World"][area_cols]
    assert (world_yearly["all"] > 200_000_000).all(), (
        "World yearly burned area unexpectedly low (< 200M ha) for some year."
    )
    assert (world_yearly["all"] < 600_000_000).all(), (
        "World yearly burned area unexpectedly high (> 600M ha) for some year."
    )

    # Savannas should be the dominant land cover globally each year
    assert (world_yearly["savannas"] > world_yearly["forest"]).all(), (
        "Savannas should exceed forests in global yearly burned area."
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
