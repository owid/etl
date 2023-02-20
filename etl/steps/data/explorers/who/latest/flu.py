"""Load a garden dataset and create an explorers dataset."""

import pandas as pd
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    flunet_garden: Dataset = paths.load_dependency("flunet")
    fluid_garden: Dataset = paths.load_dependency("fluid")

    # Read table from garden dataset.
    tb_flunet = flunet_garden["flunet"]
    tb_fluid = fluid_garden["fluid"]

    tb_flu = pd.merge(tb_fluid, tb_flunet, on=["country", "date"], how="outer")

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_flu], default_metadata=flunet_garden.metadata, formats=["csv"])
    ds_explorer.save()
