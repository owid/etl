"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Year with incomplete data
INCOMPLETE_YEAR = 2025


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("snowfall")
    tb = ds_garden.read("snowfall")
    #
    # Process data.
    #

    tb = tb.format(["country", "time"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    ds_grapher.save()
