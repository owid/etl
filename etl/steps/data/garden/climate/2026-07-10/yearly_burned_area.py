"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("yearly_burned_area")
    tb = ds_meadow.read("yearly_burned_area")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    area_types = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]

    aggregations = {col: "sum" for col in area_types}

    # Add region aggregates.
    tb = paths.regions.add_aggregates(
        tb,
        aggregations=aggregations,
        regions=REGIONS,
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

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
