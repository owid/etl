"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_historical = paths.load_dataset("measles_historical")
    ds_meadow_current = paths.load_dataset("measles_cases")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb_hist = ds_meadow_historical.read("measles_historical")
    tb_curr = ds_meadow_current.read("measles_cases")
    tb_curr = tb_curr[["country", "year", "cases"]]

    tb = pr.concat([tb_hist, tb_curr], short_name="measles_long_run")
    # Save outputs.

    tb = pr.merge(
        tb,
        ds_population.read("population"),
        on=["country", "year"],
        how="left",
    )
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000
    tb = tb.drop(columns=["population", "source", "world_pop_share"])
    tb = tb.format(["country", "year"])
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
