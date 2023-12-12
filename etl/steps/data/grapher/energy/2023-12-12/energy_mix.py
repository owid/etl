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
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("energy_mix")
    tb_garden = ds_garden["energy_mix"]

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
