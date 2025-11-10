"""Load garden dataset and create grapher views."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Create grapher dataset."""
    # Load garden dataset.
    ds_garden = paths.load_dataset("operating_data")

    # Read table from garden dataset.
    tb = ds_garden.read("operating_data", reset_index=False)

    # Save outputs.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
