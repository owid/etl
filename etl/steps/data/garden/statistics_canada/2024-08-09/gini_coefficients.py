"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gini_coefficients")

    # Read table from meadow dataset.
    tb = ds_meadow["gini_coefficients"].reset_index()

    #
    # Process data.
    #
    # Multiply gini by 100
    tb["gini"] *= 100

    # Make data wide
    tb = tb.pivot(index=["country", "year"], columns="income_concept", values="gini").reset_index()

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()