"""Grapher step for the global primary energy dataset.
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
    ds_garden: Dataset = paths.load_dependency("global_primary_energy")
    tb_garden = ds_garden["global_primary_energy"]

    #
    # Process data.
    #
    # Drop unnecessary columns from table.
    tb = tb_garden.drop(columns=["data_source"])

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
