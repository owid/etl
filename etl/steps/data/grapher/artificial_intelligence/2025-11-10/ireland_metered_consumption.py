"""Grapher step for Ireland Data Centres Metered Electricity Consumption.

This step loads the garden dataset and saves it for upload to the MySQL grapher database.
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Create grapher dataset from garden dataset."""
    # Load inputs
    ds_garden = paths.load_dataset("ireland_metered_consumption")

    # Read table from garden dataset
    tb = ds_garden.read("ireland_metered_consumption", reset_index=False)

    # Save outputs
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )
    ds_grapher.save()
