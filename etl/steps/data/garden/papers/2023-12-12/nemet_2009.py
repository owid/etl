"""Harmonize data from Nemet (2009) paper on cost and capacity of photovoltaic energy.

"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow: Dataset = paths.load_dependency("nemet_2009")
    tb_meadow = ds_meadow["nemet_2009"]

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_meadow], default_metadata=ds_meadow.metadata)
    ds_garden.save()
