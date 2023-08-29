"""Load a meadow dataset and create a garden dataset."""

from typing import cast

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

    #
    # Process data.
    #

    tb.reset_index(inplace=True)
    # Save the metadata df
    metadata_tb = tb.loc[:, ["indicator_code", "indicator_name", "description", "source"]]
    # Drop metadata columns from the dataset table
    tb.drop(["indicator_name", "description", "source"], axis=1, inplace=True)

    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=paths.excluded_countries_path,
        countries_file=paths.country_mapping_path,
    )
    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb.reset_index(inplace=True)

    # Set an appropriate index and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    tb = Table(tb, short_name=paths.short_name, underscore=True)

    # Add metadata by finding the descriptions and sources using the indicator codes.
    tb = add_metadata(tb, metadata_tb)

    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()


def add_metadata(tb: Table, metadata_tb: Table) -> None:
    """
    Adds metadata by fetching details from the table with descriptions and sources originally retrieved in snapshot using the World Bank API.

    Args:
        tb (Table): Table containing columns to which metadata needs to be added.
        metadata_tb (Table): Table containing descriptions and sources for indicators (matched by indicator_code)

    Returns:
        Table: The table with updated metadata.
    """
    # Loop through the DataFrame columns
    for column in tqdm(tb.columns, desc="Processing metadata for indicators"):
        # Extract the title from the default metadata to find the corresponding World Bank indicator
        indicator_to_find = tb[column].metadata.title

        # Extract relevant name, description and source from the metadata table using the WB code
        name = (
            metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "indicator_name"]
            .str.replace("‚", "")  # commas caused problems when renaming variables later on
            .iloc[0]
        )
        description = metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "description"].iloc[0]
        source = metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "source"].iloc[0]
        new_column_name = underscore(name)  # Convert extracted name to underscore format

        # If more detailed description is currently missing in the API --> use the long title as a description
        if str(description) == "nan":
            description = name
            source = " "

        # Update the column names and metadata
        tb.rename(columns={column: new_column_name}, inplace=True)
        description_string = " ".join(
            [
                description + "." "World Bank variable id: " + indicator_to_find + ".",
                source,
            ]
        )

        # Replace any occurrences of '..' with '.'
        description_string = description_string.replace("..", ".")
        description_string = description_string.replace(".W", ". W")

        tb[new_column_name].metadata.description = description_string
        tb[new_column_name].metadata.title = name

        # Conver Witthgenstein projections to %
        if "wittgenstein_projection__percentage" in new_column_name:
            tb[new_column_name] *= 100

        tb[new_column_name].metadata.display = {}

        #
        # Update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains
        #

        def update_metadata(table, column, display_decimals, unit, short_unit=" "):
            """
            Update metadata attributes of a specified column in the given table.

            Args:
            table (obj): The table object containing the column.
            column (str): Name of the column whose metadata is to be updated.
            display_decimals (int): Number of decimal places to display.
            unit (str): The full name of the unit of measurement for the column data.
            short_unit (str, optional): The abbreviated form of the unit. Defaults to an empty space.

            Returns:
            None: The function updates the table in-place.
            """
            table[column].metadata.display["numDecimalPlaces"] = display_decimals
            table[column].metadata.unit = unit
            table[column].metadata.short_unit = short_unit

        # Convert the 'name' variable to lowercase for easier text matching.
        name_lower = name.lower()

        # Define a list of keywords associated with percentages.
        percentage_unit = ["%", "percentage", "share of", "rate"]
        other_list = ["ratio", "index", "years", "USD"]
        # Check if any keyword from the percentage_unit list is present in 'name_lower' and ensure "number" is not in 'name_lower'.
        if any(keyword in name_lower for keyword in percentage_unit) and (name_lower not in other_list):
            update_metadata(tb, new_column_name, 1, "%", "%")
        elif "ratio" in name_lower and not ("duration" in name_lower):
            update_metadata(tb, new_column_name, 1, "ratio", " ")
        elif "number of pupils" in name_lower:
            update_metadata(tb, new_column_name, 0, "pupils", " ")
        # Check if the column name contains "number", but not "rate" or "pasec".
        elif "number" in name_lower and not ("rate" in name_lower) and not ("pasec" in name_lower):
            update_metadata(tb, new_column_name, 0, "people", " ")
        elif "years" in name_lower:
            update_metadata(tb, new_column_name, 1, "years", " ")
        elif "index" in name_lower:
            update_metadata(tb, new_column_name, 1, "index", " ")
        # Check for the presence of currency-related keywords in 'name_lower'.
        elif "usd" in name_lower or "$" in name_lower:
            update_metadata(tb, new_column_name, 1, "US dollars", "$")
        elif "score" in name_lower:
            update_metadata(tb, new_column_name, 1, "score", " ")

        else:
            # Default metadata update when no other conditions are met.
            update_metadata(tb, new_column_name, 0, " ", " ")

    return tb
