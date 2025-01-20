"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define index columns
INDEX_COLUMNS = ["country", "year", "povertyline", "scenario"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("poverty_projections")

    # Read table from garden dataset.
    tb = ds_garden.read("poverty_projections")

    # Round povertyline to 2 decimal places
    tb["povertyline"] = tb["povertyline"].round(2)

    tb = tb.format(INDEX_COLUMNS)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset..
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
