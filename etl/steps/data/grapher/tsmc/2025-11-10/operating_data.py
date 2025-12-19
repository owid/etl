"""Load garden dataset and create grapher views."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create grapher dataset."""
    # Load garden dataset.
    ds_garden = paths.load_dataset("operating_data")

    # Read table from garden dataset.
    tb = ds_garden.read("operating_data", reset_index=False)

    # Save outputs.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
