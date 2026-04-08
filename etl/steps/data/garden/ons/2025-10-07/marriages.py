"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("marriages")

    # Read table from meadow dataset.
    tb = ds_meadow.read("marriages")
    tb["cumulative_percentage_per_100"] = tb["cumulative_percentage_per_1000"] / 10
    tb = tb.drop(columns=["cumulative_percentage_per_1000"])

    tb = tb.format(["year", "age", "gender"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
