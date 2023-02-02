"""Grapher step for Global Carbon Budget dataset.

Some auxiliary variables will be added (where nans are filled with zeros, to avoid missing data in stacked area charts).

"""

from copy import deepcopy

import numpy as np
import pandas as pd
from owid import catalog

from etl.helpers import PathFinder

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

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create a new Grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # Load table from Garden dataset.
    table = N.garden_dataset["global_carbon_budget"]

    # Ensure all countries span all years (from 1750 to the latest observation), even if many of those rows are empty.
    # This will increase the size of the dataset, but we do this so that stacked area charts span the maximum possible
    # range of years.
    countries = table.reset_index()["country"].unique()
    years = np.arange(table.reset_index()["year"].min(), table.reset_index()["year"].max() + 1, dtype=int)
    table = table.reindex(pd.MultiIndex.from_product([countries, years], names=["country", "year"]))

    # Create additional variables in the table that have nans filled with zeros (for two specific stacked area charts).
    for variable in VARIABLES_TO_FILL_WITH_ZEROS:
        new_variable_name = variable + "_zero_filled"
        table[new_variable_name] = table[variable].fillna(0)
        table[new_variable_name].metadata = deepcopy(table[variable].metadata)
        table[new_variable_name].metadata.title = table[variable].metadata.title + " (zero filled)"
        table[new_variable_name].metadata.description = (
            table[variable].metadata.description + " Missing data has been filled with zeros for the purposes of data "
            "visualization."
        )

    # Add table to Grapher dataset and save dataset.
    dataset.add(table)
    dataset.save()
