"""Load a meadow dataset and create a garden dataset."""

from shared import add_regional_aggregates

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
AGE_GROUPS_RANGES = {
    "All ages": [0, None],
    "<5 years": [0, 4],
    "5-14 years": [5, 14],
    "15-49 years": [15, 49],
    "50-69 years": [50, 69],
    "70+ years": [70, None],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_cause_dalys")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_cause_dalys"].reset_index()
    # Drop population_group_name as it only contains 'All Population'
    tb = tb.drop(columns=["population_group_name"])
    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb=tb)
    # Add regional aggregates
    tb = add_regional_aggregates(
        tb=tb,
        ds_regions=ds_regions,
        index_cols=["country", "year", "metric", "measure", "cause", "age"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
        run_percent=True,
    )

    # Split into two tables: one for deaths, one for DALYs
    tb_dalys = tb[tb["measure"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    # Shorten the metric name for DALYs
    tb_dalys["measure"] = "DALYs"

    # Drop the measure column
    tb_dalys = tb_dalys.drop(columns="measure")

    # Format the tables
    tb_dalys = tb_dalys.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_deaths")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_dalys],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
