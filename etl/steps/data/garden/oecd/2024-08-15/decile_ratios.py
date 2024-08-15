"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define new names for indicators
INDICATOR_NAMES = {
    "Decile 9/1": "p90_p10_ratio",
    "Decile 9/5": "p90_p50_ratio",
    "Decile 5/1": "p50_p10_ratio",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("decile_ratios")

    # Read table from meadow dataset.
    tb = ds_meadow["decile_ratios"].reset_index()

    #
    # Process data.
    #

    # Multiply by 100 to get percentages.
    tb["value"] *= 100

    # Make table wide
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    # Replace names of columns
    tb = tb.rename(columns=INDICATOR_NAMES, errors="raise")

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
