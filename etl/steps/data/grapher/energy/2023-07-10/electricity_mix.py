"""Grapher step for the Electricity Mix (BP & Ember) dataset.
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
    ds_garden: Dataset = paths.load_dependency("electricity_mix")
    tb_garden = ds_garden["electricity_mix"]

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb_garden.drop(columns=["population"])

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
