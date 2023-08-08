"""Load a meadow dataset and create a garden dataset."""


from typing import cast

import requests
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
new_line = "<br><br>"


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
    Add metadata to the DataFrame 'tb' by fetching information from the World Bank API.

    :param tb: DataFrame containing the indicators.
    :return: Updated DataFrame with the added metadata.
    """

    for column in tqdm(tb.columns, desc="Processing metadata for indicators"):
        # Extract the title from the metadata to find the corresponding World Bank indicator.
        indicator_to_find = tb[column].metadata.title
        try:
            # Construct the URL to fetch metadata from the World Bank API.
            url = f"https://api.worldbank.org/v2/indicator/{indicator_to_find}?format=json"

            # Send GET request to the World Bank API.
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

            data = response.json()

            # Check if the expected data format is received
            if len(data) < 2 or not isinstance(data[1], list) or len(data[1]) < 1:
                raise ValueError("Unexpected data format received from the World Bank API")

            # Extract relevant data from the API response.
            nested_data = data[1][0]
            sourceNote = nested_data["sourceNote"]
            sourceOrganization = nested_data["sourceOrganization"]
            name = nested_data["name"]
            variable_id = nested_data["id"]

            # Update the metadata for the current column.
            tb[column].metadata.title = name
            new_column_name = underscore(name)
            tb.rename(columns={column: new_column_name}, inplace=True)
            tb[new_column_name].metadata.description = new_line.join(
                [
                    sourceNote,
                    sourceOrganization,
                    "World Bank variable id: " + variable_id,
                ]
            )
            tb[new_column_name].metadata.display = {}
            # Now update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains.
            #
            if "%" in name or "Percentage" in name or "percentage" in name or "share of" in name or "rate" in name:
                tb[new_column_name].metadata.unit = "%"
                tb[new_column_name].metadata.short_unit = "%"
                tb[new_column_name].metadata.display["numDecimalPlaces"] = 1
            elif "ratio" in name:
                tb[new_column_name].metadata.display["numDecimalPlaces"] = 1
                tb[new_column_name].metadata.unit = "ratio"
                tb[new_column_name].metadata.short_unit = " "
            elif "(years)" in name or "years" in name:
                tb[new_column_name].metadata.display["numDecimalPlaces"] = 1
                tb[new_column_name].metadata.unit = "years"
                tb[new_column_name].metadata.short_unit = " "
            elif "number of pupils" in name:
                tb[new_column_name].metadata.display["numDecimalPlaces"] = 0
                tb[new_column_name].metadata.unit = "pupils"
                tb[new_column_name].metadata.short_unit = " "
            else:
                tb[new_column_name].metadata.unit = " "
                tb[new_column_name].metadata.short_unit = " "

        except requests.RequestException as e:
            print(f"An error occurred while fetching data for {indicator_to_find}: {e}")
        except ValueError as e:
            print(f"An error occurred while processing data for {indicator_to_find}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {indicator_to_find}: {e}")

    return tb
