"""Harmonize data from Nemet (2009) paper on cost and capacity of photovoltaic energy.

"""

from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow.
    ds_meadow: catalog.Dataset = paths.load_dependency("nemet_2009")
    tb_meadow = ds_meadow["nemet_2009"]

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = catalog.Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Create a new table and add it to the dataset.
    tb_garden = tb_meadow.copy()
    ds_garden.add(tb_garden)

    # Update dataset metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()
