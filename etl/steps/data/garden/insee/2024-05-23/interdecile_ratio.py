"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep
COLUMNS_TO_KEEP = ["p90_p50_ratio"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("interdecile_ratio")

    # Read table from meadow dataset.
    tb = ds_meadow["interdecile_ratio"].reset_index()

    #
    # Process data.
    tb = tb.format(["country", "year"])

    # Keep relevant columns
    tb = tb[COLUMNS_TO_KEEP]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
