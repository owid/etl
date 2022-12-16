"""Prepare Global Carbon Budget Fossil CO2 data.

The resulting dataset will have one table of national fossil CO2 emissions (that does not include land-use change
emissions). Bunker emissions are included as a separate country, called "International Transport".

"""

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Get naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load fossil CO2 data from Walden.
    emissions_ds = Snapshot("gcp/2022-11-11/global_carbon_budget_fossil_co2_emissions.csv")
    # Create a dataframe with the data.
    emissions_df = pd.read_csv(emissions_ds.path)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    emissions_df = emissions_df.set_index(["Country", "Year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create new dataset and reuse walden metadata (from any of the raw files).
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(emissions_ds)

    # Create a new table with metadata.
    emissions_tb = Table(emissions_df, metadata=TableMeta(short_name=N.short_name))

    # Ensure all columns are lower snake case.
    emissions_tb = underscore_table(emissions_tb)

    # Add table to new dataset and save dataset.
    ds.add(emissions_tb)
    ds.save()
