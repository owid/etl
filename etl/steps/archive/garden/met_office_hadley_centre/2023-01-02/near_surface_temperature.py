"""Curate near surface temperature dataset by Met Office Hadley Centre.

"""

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

# Get naming conventions.
N = PathFinder(__file__)

# Meadow and garden dataset versions.
MEADOW_VERSION = "2023-01-02"
GARDEN_VERSION = MEADOW_VERSION


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = Dataset(DATA_DIR / f"meadow/met_office_hadley_centre/{MEADOW_VERSION}/near_surface_temperature")
    tb_meadow = ds_meadow["near_surface_temperature"]
    df = pd.DataFrame(tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as in meadow.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Create a new table with the same metadata as in meadow and add it to the dataset.
    tb_garden = Table(df, like=tb_meadow)
    ds_garden.add(tb_garden)

    # Update dataset metadata and save dataset.
    ds_garden.update_metadata(N.metadata_path)
    ds_garden.save()
