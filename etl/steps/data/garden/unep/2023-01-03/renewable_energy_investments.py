"""Create a garden dataset on renewable energy investments based on UNEP data.

"""

import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = paths.load_dependency("renewable_energy_investments")
    tb_meadow = ds_meadow["renewable_energy_investments"]

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Create new table with metadata and add it to the new dataset.
    tb_garden = tb_meadow
    ds_garden.add(tb_garden)

    # Update metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()
