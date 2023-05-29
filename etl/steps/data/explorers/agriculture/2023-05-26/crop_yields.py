"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Crop Yields explorer:
https://ourworldindata.org/explorers/crop-yields
"""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("attainable_yields"))

    # Read table from garden dataset.
    tb_garden = ds_garden["attainable_yields"]

    # Rename table to have the same name as the current step, for consistency.
    tb_garden.metadata.short_name = paths.short_name

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, formats=["csv"])
    ds_explorer.save()
