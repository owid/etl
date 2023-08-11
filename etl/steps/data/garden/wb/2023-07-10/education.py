"""Load a meadow dataset and create a garden dataset."""

from time import sleep
from typing import cast

import requests
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education"))

    # Read table from meadow dataset.
    tb = ds_meadow["education"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=paths.excluded_countries_path,
        countries_file=paths.country_mapping_path,
    )

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb.reset_index(inplace=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    tb = Table(tb, short_name=paths.short_name, underscore=True)
    tb = add_metadata(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_metadata(tb):
    """
    Adds metadata to the columns of a DataFrame by fetching details from the World Bank API.

    Args:
        tb (DataFrame): The DataFrame containing columns to which metadata needs to be added.

    Returns:
        DataFrame: The DataFrame with updated metadata.

    Note:
        Each column's metadata must contain a title attribute that corresponds to a World Bank indicator.
    """
    # Loop through the DataFrame columns
    for column in tqdm(tb.columns, desc="Processing metadata for indicators"):
        retries = 3
        delay = 1  # Initial delay in seconds

        # Attempt to process each column up to `retries` times
        for attempt in range(retries):
            try:
                # Extract the title from the metadata to find the corresponding World Bank indicator
                indicator_to_find = tb[column].metadata.title

                # Construct the URL to fetch metadata from the World Bank API
                url = f"https://api.worldbank.org/v2/indicator/{indicator_to_find}?format=json"

                # Send GET request to the World Bank API with a timeout (in seconds)
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Check for unsuccessful status codes

                data = response.json()

                # Validate the expected data format received from the World Bank API
                if len(data) < 2 or not isinstance(data[1], list) or len(data[1]) < 1:
                    raise ValueError("Unexpected data format received from the World Bank API")

                # Extract relevant metadata from the API response
                nested_data = data[1][0]
                name = nested_data["name"].replace("‚", "")
                new_column_name = underscore(name)  # Convert name to underscore format

                # Update the DataFrame column name and metadata
                tb.rename(columns={column: new_column_name}, inplace=True)
                tb[new_column_name].metadata.description = " ".join(
                    [
                        nested_data["sourceNote"],
                        "World Bank variable id: " + nested_data["id"],
                        nested_data["sourceOrganization"],  # change this when the new metadata format is ready
                    ]
                )
                tb[new_column_name].metadata.title = nested_data["name"]
                tb[new_column_name].metadata.display = {}

                # Now update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains.
                #
                def update_metadata(table, column, display_decimals, unit, short_unit=" "):
                    table[column].metadata.display["numDecimalPlaces"] = display_decimals
                    table[column].metadata.unit = unit
                    table[column].metadata.short_unit = short_unit

                name_lower = name.lower()

                percentage_unit = ["%", "percentage", "share of", "rate"]
                # Checking the most specific conditions first to avoid redundancy
                if any(keyword in name_lower for keyword in percentage_unit) and not ("number" in name_lower):
                    update_metadata(tb, new_column_name, 1, "%", "%")
                elif "ratio" in name_lower:
                    update_metadata(tb, new_column_name, 1, "ratio", " ")
                elif "number of pupils" in name_lower:
                    update_metadata(tb, new_column_name, 0, "pupils", " ")
                elif "number" in name_lower and not ("rate" in name_lower) and not ("pasec" in name_lower):
                    update_metadata(tb, new_column_name, 0, "people", " ")
                elif "(years)" in name_lower or "years" in name_lower:
                    update_metadata(tb, new_column_name, 1, "years", " ")
                elif "index" in name_lower:
                    update_metadata(tb, new_column_name, 1, "index", " ")
                else:
                    update_metadata(tb, new_column_name, 0, " ", " ")

                break  # Success, exit the retry loop

            except requests.exceptions.ReadTimeout:
                print(f"Timeout while processing column {column}. Retrying in {delay} seconds...")
                sleep(delay)
                delay *= 2  # Double the delay for the next attempt
    return tb
