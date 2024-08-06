"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load the garden gender statistics datasets.
    ds_garden = paths.load_dataset("gender_statistics")
    tb = ds_garden["gender_statistics"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
