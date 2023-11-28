"""Load a garden dataset and create a grapher dataset.

Some auxiliary variables will be added (where nans are filled with zeros, to avoid missing data in stacked area charts).

"""

import numpy as np
import pandas as pd
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("global_carbon_budget")
    tb_garden = ds_garden["global_carbon_budget"]

    #
    # Process data.
    #
    # Ensure all countries span all years (from 1750 to the latest observation), even if many of those rows are empty.
    # This will increase the size of the dataset, but we do this so that stacked area charts span the maximum possible
    # range of years.
    countries = tb_garden.reset_index()["country"].unique()
    years = np.arange(tb_garden.reset_index()["year"].min(), tb_garden.reset_index()["year"].max() + 1, dtype=int)
    tb_garden = tb_garden.reindex(pd.MultiIndex.from_product([countries, years], names=["country", "year"]))

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_garden],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
    )

    # Sanity checks.
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
