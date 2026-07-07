"""Grapher step for Ireland Data Centers Metered Electricity Consumption.

This step loads the garden dataset and saves it for upload to the MySQL grapher database.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run() -> None:
    """Create grapher dataset from garden dataset."""
    # Load inputs
    ds_garden = paths.load_dataset("ireland_metered_consumption")

    # Read table from garden dataset
    tb = ds_garden.read("ireland_metered_consumption", reset_index=False)

    # Save outputs
    ds_grapher = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )
    ds_grapher.save()
