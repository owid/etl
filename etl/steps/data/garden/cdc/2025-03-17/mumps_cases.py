"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mumps_cases")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb = ds_meadow.read("mumps_cases")
    tb = tb.drop(columns=["source"])

    tb_pop = ds_population.read("population", reset_metadata="keep_origins")
    tb_pop = tb_pop.drop(columns=["source"])

    tb = tb.merge(tb_pop, on=["country", "year"], how="left")
    tb["case_rate"] = tb["cases"] / tb["population"] * 100000
    tb = tb.drop(columns=["population", "world_pop_share"])
    #
    # Process data.
    #
    # Harmonize country names.

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
