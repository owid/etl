"""Harmonize data from Farmer & Lafond (2016) paper on the evolution of the cost of different technologies.

"""
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow.
    ds_meadow = paths.load_dependency("farmer_lafond_2016")
    tb_meadow = ds_meadow["farmer_lafond_2016"]

    #
    # Process data.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Create a new table with the same metadata as meadow and add it to the dataset.
    tb_garden = tb_meadow
    ds_garden.add(tb_garden)
    
    # Update dataset metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()
