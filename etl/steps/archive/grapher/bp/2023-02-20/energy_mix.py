"""Grapher step for BP's energy mix dataset.
"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("energy_mix")

    # Read table from garden dataset.
    tb_garden = ds_garden["energy_mix"].reset_index()

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb_garden = tb_garden.drop(columns=["country_code"])

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)
    ds_grapher.save()
