"""Dummy file created for consistency."""
from etl.helpers import create_dataset


def run(dest_dir: str):
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[], check_variables_metadata=True)
    ds_garden.save()
