"""Load a garden dataset and create a grapher dataset.

Some auxiliary variables will be added (where nans are filled with zeros, to avoid missing data in stacked area charts).

"""
from copy import deepcopy

import numpy as np
import pandas as pd
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# For two stacked area charts (namely "CO₂ emissions by fuel type" and "Cumulative CO₂ emissions by source") having
# nans in the data causes the chart to show only years where all sources have data.
# To avoid this, create additional variables that have nans filled with zeros.
VARIABLES_TO_FILL_WITH_ZEROS = [
    "emissions_total",
    "emissions_from_cement",
    "emissions_from_coal",
    "emissions_from_flaring",
    "emissions_from_gas",
    "emissions_from_land_use_change",
    "emissions_from_oil",
    "emissions_from_other_industry",
    "cumulative_emissions_total",
    "cumulative_emissions_from_cement",
    "cumulative_emissions_from_coal",
    "cumulative_emissions_from_flaring",
    "cumulative_emissions_from_gas",
    "cumulative_emissions_from_land_use_change",
    "cumulative_emissions_from_oil",
    "cumulative_emissions_from_other_industry",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("global_carbon_budget")

    # Read table from garden dataset.
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

    # Create additional variables in the table that have nans filled with zeros (for two specific stacked area charts).
    for variable in VARIABLES_TO_FILL_WITH_ZEROS:
        new_variable_name = variable + "_zero_filled"
        tb_garden[new_variable_name] = tb_garden[variable].fillna(0)
        tb_garden[new_variable_name].metadata = deepcopy(tb_garden[variable].metadata)
        tb_garden[new_variable_name].metadata.title = tb_garden[variable].metadata.title + " (zero filled)"
        tb_garden[new_variable_name].metadata.description = (
            tb_garden[variable].metadata.description + " Missing data has been filled with zeros for the purposes of "
            "data visualization."
        )

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)

    # Sanity checks.
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
