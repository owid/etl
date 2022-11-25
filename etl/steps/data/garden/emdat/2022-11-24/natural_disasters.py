"""Process and harmonize EM-DAT natural disasters dataset.

"""

import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from shared import CURRENT_DIR, add_region_aggregates, add_population

from etl.helpers import Names
from etl.paths import DATA_DIR

# Define inputs.
MEADOW_VERSION = "2022-11-24"
# Define outputs.
VERSION = MEADOW_VERSION

# Get naming conventions.
N = Names(str(CURRENT_DIR / "natural_disasters"))


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = Dataset(DATA_DIR / f"meadow/emdat/{MEADOW_VERSION}/natural_disasters")
    # Get table from dataset.
    tb_meadow = ds_meadow["natural_disasters"]
    # Create a dataframe from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Sanity check.
    error = "Expected only 'Natural' in 'group' column."
    assert df["group"].unique().tolist() == ["Natural"], error

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=N.country_mapping_path, warn_on_missing_countries=True,
                            warn_on_unused_countries=True)

    # TODO: Check that, when loading all countries from the countries file, and removing the historical regions,
    #  that aggregation coincides with the World aggregate.

    # Remove spurious spaces in entities.
    df["type"] = df["type"].str.strip()

    # We are not interested in each individual event, but the number of events of each kind and damages.
    df = df.groupby(["country", "year", "type"]).sum(numeric_only=True).reset_index()

    # Add Azores Islands to Portugal (so that we can attach a population to it).
    new_portugal_df = pd.concat([df[df["country"] == "Portugal"], df[df["country"] == "Azores Islands"]]).\
        groupby(["year", "type"]).\
        sum(numeric_only=True).reset_index().assign(**{"country": "Portugal"})
    df = pd.concat([df[~df["country"].isin(["Azores Islands", "Portugal"])], new_portugal_df]).reset_index(drop=True)

    # Idem for Canary Islands.
    new_spain_df = pd.concat([df[df["country"] == "Spain"], df[df["country"] == "Canary Islands"]]).\
        groupby(["year", "type"]).\
        sum(numeric_only=True).reset_index().assign(**{"country": "Spain"})
    df = pd.concat([df[~df["country"].isin(["Canary Islands", "Spain"])], new_spain_df]).reset_index(drop=True)

    # Add a new category (or "type") corresponding to the total of all natural disasters.
    all_disasters = df.groupby(["country", "year"]).sum(numeric_only=True).reset_index().assign(**{"type": "All disasters"})
    df = pd.concat([df, all_disasters], ignore_index=True).sort_values(["country", "year", "type"]).reset_index(drop=True)

    # TODO: Complete HISTORIC_TO_CURRENT_REGION.
    # In fact, add all possible countries, even if they are not in the data.

    # TODO:
    # * Create a table for yearly and another for decadal data.
    # * In the grapher step, create a dataset that selects the world and treats "type" as "country",
    #  and another dataset for national data.

    df = add_region_aggregates(data=df, index_columns=["country", "year", "type"])

    # Add population including historical countries.
    df = add_population(df=df, warn_on_missing_countries=True)

    for column in df.drop(columns=["country", "year", "type"]).columns:
        df[f"{column}_per_100k_people"] = df[column] * 1e5 / df["population"]

    # TODO: Create a new table for decadal data. What's the best way?
    decadal = df.copy()
    decadal["year"] = pd.to_datetime(decadal["year"], format="%Y")
    decadal = decadal.set_index(["country", "type", "year"]).reset_index(level=[0, 1])
    decadal.groupby(['country','type']).resample('10A', origin="end_day").sum(numeric_only=True).reset_index()

    # Alternative way:
    decadal = df.copy()
    decadal["year"] = pd.to_datetime(decadal["year"], format="%Y")
    decadal = decadal.set_index(["country", "type", "year"])
    level_values = decadal.index.get_level_values
    decadal.groupby([level_values(i) for i in [0,1]]+[pd.Grouper(freq='10A', level=-1)]).sum(numeric_only=True)

    #
    # Save outputs.
    #
    # Create new Garden dataset.
    ds_garden = Dataset.create_empty(dest_dir)
    # Ensure all column names are snake, lower case.
    tb_garden = underscore_table(Table(df))
    # Get dataset metadata from yaml file.
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    # Get table metadata from yaml file.
    tb_garden.update_metadata_from_yaml(N.metadata_path, "natural_disasters")
    # Add table to dataset and save dataset.
    ds_garden.add(tb_garden)
    ds_garden.save()
