"""Load a meadow dataset and create a garden dataset."""

import os
import zipfile
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education"))

    # Read table from meadow dataset.
    tb = ds_meadow["education"]
    tb.reset_index(inplace=True)
    # Add region aggregates.
    # tb = add_data_for_regions(tb=tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    #
    # Process data.
    #
    # Combine the directory and filename to get the full file path
    base_directory = os.path.dirname(paths.country_mapping_path)

    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=os.path.join(base_directory, "education.excluded_countries.json"),
        countries_file=os.path.join(base_directory, "education.countries.json"),
    )

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb.reset_index(inplace=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    tb = Table(tb, short_name=paths.short_name, underscore=True)

    # Add metadata based on a csv file located within the snapshot directory
    snap: Snapshot = paths.load_dependency("education.zip")
    df_metadata = read_metadata(snap)
    tb = add_metadata(tb, df_metadata)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()


def read_metadata(snap: Snapshot) -> pd.DataFrame:
    """
    Read metadata from a Snapshot and return it as a DataFrame.

    Parameters:
        snap (Snapshot): The Snapshot object containing metadata to be read.

    Returns:
        pd.DataFrame: A DataFrame containing the metadata from the Snapshot.

    This function reads metadata from the given Snapshot object, which is a zip archive
    containing the required metadata CSV file named 'EdStatsSeries.csv'. The function follows
    the following steps:

    Step 1: Unzip the file - The function unzips the contents of the Snapshot object, extracting
    the 'EdStatsSeries.csv' file into the destination directory.

    Step 2: Read the CSV file - The function reads the extracted CSV file using pandas and
    creates a DataFrame 'df_metadata' containing the metadata.

    The DataFrame 'df_metadata' is then returned, containing the metadata extracted from the
    given Snapshot.
    """
    # Step 1: Unzip the file
    with zipfile.ZipFile(snap.path, "r") as zip_ref:
        # Replace 'data.csv' with the name of the CSV file in the zip archive
        csv_file_name = "EdStatsSeries.csv"
        destination_directory = os.path.dirname(snap.path)
        zip_ref.extract(csv_file_name, destination_directory)

    # Step 2: Read the CSV file
    df_metadata = pd.read_csv(os.path.join(destination_directory, csv_file_name))
    return df_metadata


def add_metadata(tb, df_metadata):
    """
    Add metadata to the columns of the given DataFrame 'tb' based on the 'df_metadata'.

    Parameters:
        tb (DataFrame): The DataFrame to which metadata will be added.
        df_metadata (DataFrame): The DataFrame containing metadata information.

    Returns:
        DataFrame: The modified DataFrame 'tb' with added metadata.

    This function takes two DataFrames, 'tb' and 'df_metadata', and adds metadata information
    to each column of 'tb' based on the 'Series Code' found in 'df_metadata'. The metadata
    includes column title, description, unit, short unit, and display properties.
    """
    df_metadata = df_metadata[["Series Code", "Indicator Name", "Source", "Long definition"]]

    # Convert 'Series Code' to upper case and remove spaces from it to avoid matching errors.
    df_metadata["Series Code"] = df_metadata["Series Code"].str.replace(" ", "").str.upper()

    # Iterate through each column in the DataFrame 'tb' and update its metadata.
    for column in tb.columns:
        # Extract indicator id (WB definition)
        indicator_to_find = tb[column].metadata.title

        # Convert 'indicator_to_find' to upper case and remove spaces from it to avoid matching errors.
        indicator_to_find = indicator_to_find.replace(" ", "").upper()

        # Extract long definition of the variable from 'df_metadata'.
        filtered_definition = df_metadata["Long definition"][df_metadata["Series Code"] == indicator_to_find].values

        # Extract source information from 'df_metadata' (to be used in future metadata format).
        filtered_source = df_metadata["Source"][df_metadata["Series Code"] == indicator_to_find].values

        # Find the matching 'Indicator Name' (informative name) from 'df_metadata' for the given 'indicator_to_find' (WB variable id).
        filtered_indicator = df_metadata.loc[df_metadata["Series Code"] == indicator_to_find, "Indicator Name"].values

        if (len(filtered_indicator) + len(filtered_definition) + len(filtered_source)) > 0:
            definition = filtered_definition[0]
            source = " Provided by " + filtered_source[0]
            variable_name = filtered_indicator[0]
        else:
            raise ValueError(
                "No matching 'Indicator Name', 'Source' or 'Long definition' found for the given 'indicator_to_find' (indicator id)."
            )

        # Update the metadata for the current column in 'tb'.
        tb[column].metadata.title = variable_name

        # We were using indicator id for matching metadata as this is less prone to problems.
        # Now we should use make the variable names more informative - so use the value from the Indicator Name as a new column name
        new_column_name = underscore(variable_name)
        # Rename the column in 'tb' to 'new_column_name'.
        tb.rename(columns={column: new_column_name}, inplace=True)

        # Set the description, unit, short_unit, and display properties based on column name patterns.
        tb[new_column_name].metadata.description = definition + source
        tb[new_column_name].metadata.display = {}

        # Now update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains.
        #
        if (
            "%" in variable_name
            or "Percentage" in variable_name
            or "percentage" in variable_name
            or "share of" in variable_name
            or "rate" in variable_name
        ):
            tb[new_column_name].metadata.unit = "%"
            tb[new_column_name].metadata.short_unit = "%"
            tb[new_column_name].metadata.display["numDecimalPlaces"] = 0
        elif "ratio" in variable_name:
            tb[new_column_name].metadata.display["numDecimalPlaces"] = 1
            tb[new_column_name].metadata.unit = "ratio"
            tb[new_column_name].metadata.short_unit = " "
        elif "(years)" in variable_name or "years" in variable_name:
            tb[new_column_name].metadata.display["numDecimalPlaces"] = 1
            tb[new_column_name].metadata.unit = "years"
            tb[new_column_name].metadata.short_unit = " "
        elif "number of pupils" in variable_name or "number" in variable_name:
            tb[new_column_name].metadata.display["numDecimalPlaces"] = 0
            tb[new_column_name].metadata.unit = "pupils"
            tb[new_column_name].metadata.short_unit = " "
        else:
            tb[new_column_name].metadata.unit = " "
            tb[new_column_name].metadata.short_unit = " "
    return tb
