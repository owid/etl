"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation.

"""

import pandas as pd
from owid import catalog

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow.
    ds_meadow: catalog.Dataset = paths.load_dependency("renewable_electricity_capacity_and_generation")
    # Load main table from dataset.
    tb_meadow = ds_meadow["renewable_electricity_capacity_and_generation"]
    # Create a dataframe out of the main table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Reshape dataframe to have each technology as a separate column, and sort conveniently.
    df = (
        df.pivot(index=["country", "year"], columns=["technology"], values="capacity")
        .rename_axis(None, axis=1)
        .sort_index()
        .sort_index(axis=1)
    )

    # For convenience, remove parentheses from column names.
    df = df.rename(columns={column: column.replace("(", "").replace(")", "") for column in df.columns})

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)
    ds_garden.metadata.version = paths.version

    # Create a new table.
    tb_garden = catalog.Table(df, underscore=True, short_name=paths.short_name)

    # Add table to dataset.
    ds_garden.add(tb_garden)

    # Update metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()
