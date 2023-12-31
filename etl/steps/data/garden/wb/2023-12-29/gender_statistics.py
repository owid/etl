"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
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

    # Load meadow datasets.
    ds_meadow = paths.load_dataset("gender_statistics")
    tb = ds_meadow["gender_statistics"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    # Columns containing metadata
    metadata_columns = [
        "indicator_name",
        "long_definition",
        "source",
        "topic",
        "license_type",
        "statistical_concept_and_methodology",
        "development_relevance",
        "limitations_and_exceptions",
        "general_comments",
        "notes_from_original_source",
    ]

    # Save the table with just metadata
    metadata_tb = tb.loc[
        :,
        ["wb_seriescode"] + metadata_columns,
    ]

    # Drop metadata columns from the original table
    tb = tb.drop(metadata_columns, axis=1)

    # Harmonize countries
    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=paths.excluded_countries_path,
        countries_file=paths.country_mapping_path,
    )
    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="wb_seriescode", values="value")
    tb = tb.reset_index()

    # Add metadata by finding the descriptions and sources using indicator codes
    tb = add_metadata(tb, metadata_tb)

    # Set an appropriate index and sort.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Combine maternity and paternity leave indicators as described in the issue here: https://github.com/owid/owid-issues/issues/1274
    tb["total_maternity_leave"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"]
    tb["total_paternity_leave"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"]

    # Add relevant metadata to the newly created columns
    tb["total_maternity_leave"].metadata.description_from_producer = (
        "**This indicator is a sum of two different maternity leave indicators provided by World Bank:**"
        + "\n\nThe first indicator is called "
        + tb["sh_mmr_leve"].metadata.title
        + ". The revelavant description from World Bank of this indicator is:\n\n"
        + tb["sh_mmr_leve"].metadata.description_from_producer
        + "\n\nThe first indicator is called "
        + tb["sh_par_leve_fe"].metadata.title
        + ". The revelavant description from World Bank of this indicator is:\n\n"
        + tb["sh_par_leve_fe"].metadata.description_from_producer
    )

    tb["total_paternity_leave"].metadata.description_from_producer = (
        "**This indicator is a sum of two different paternity leave indicators provided by World Bank:**"
        + "\n\nThe first indicator is called "
        + tb["sh_par_leve_ma"].metadata.title
        + ". The revelavant description from World Bank of this indicator is:\n\n"
        + tb["sh_par_leve_ma"].metadata.description_from_producer
        + "\n\nThe second indicator is called "
        + tb["sh_ptr_leve"].metadata.title
        + ". The revelavant description from World Bank of this indicator is:\n\n"
        + tb["sh_ptr_leve"].metadata.description_from_producer
    )

    tb["total_paternity_leave"].metadata.title = "Total days of leave available for the father"
    tb["total_maternity_leave"].metadata.title = "Total days of leave available for the mother"

    tb[
        "total_maternity_leave"
    ].metadata.description_processing = "The total days of leave available for the mother indicator was created by OWID by combining maternity leave and parental leave mother quota indicators. For more details on each indicator see the section on **How producer describes this data**."
    tb[
        "total_paternity_leave"
    ].metadata.description_processing = "The total days of leave available for the father indicator was created by OWID by combining paternity leave and parental leave father quota indicators. For more details on each indicator see **How producer describes this data**."

    for leave_col in ["total_paternity_leave", "total_maternity_leave"]:
        tb[leave_col].metadata.display["numDecimalPlaces"] = 0
        tb[leave_col].metadata.unit = "days"
        tb[leave_col].metadata.short_unit = ""

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def add_metadata(tb: Table, metadata_tb: Table):
    """
    Adds metadata by fetching details from the table with descriptions and sources originally retrieved in snapshot using the World Bank API.

    Args:
        tb (Table): Table containing columns to which metadata needs to be added.
        metadata_tb (Table): Table containing descriptions and sources for indicators (matched by wb_seriescode)

    Returns:
        Table: The table with updated metadata.
    """

    # Loop through the Table columns
    for column in tqdm(tb.columns, desc="Processing metadata for indicators"):
        if column not in ["country", "year"]:
            name = metadata_tb.loc[metadata_tb["wb_seriescode"] == column, "indicator_name"].iloc[0]

            # Define the columns to extract from metadata_tb
            metadata_fields = [
                "long_definition",
                "source",
                "statistical_concept_and_methodology",
                "limitations_and_exceptions",
                "license_type",
                "development_relevance",
                "general_comments",
                "notes_from_original_source",
            ]

            # Extract the metadata for the current column
            metadata = metadata_tb.loc[metadata_tb["wb_seriescode"] == column, metadata_fields].iloc[0]

            # Replace NaN values with empty strings and handle specific cases
            metadata = metadata.where(pd.notna(metadata), "")

            # Handle the case where the detailed description is missing
            if metadata["long_definition"] == "":
                metadata["long_definition"] = name
                metadata["source"] = ""

            # Construct the components list
            components = [f"{metadata[field]}" if metadata[field] else "" for field in metadata_fields]
            components.append(f"World Bank variable id: {column}")

            # Create the metadata description from producer string
            description_string = "\n\n".join(filter(None, components))
            if not description_string:
                description_string = "No detailed metadata available from World Bank."
            tb[column].metadata.description_from_producer = description_string
            tb[column].metadata.title = name
            tb[column].metadata.processing = "minor"
            tb[column].metadata.display = {}

            #
            # Update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains
            #

            def update_metadata(table, column, display_decimals, unit, short_unit):
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

            # Convert the 'name' variable to lowercase to make text matching easier
            name_lower = tb[column].title.lower()

            # Fill out units and decimal places
            if "%" in name_lower:
                update_metadata(tb, column, 1, unit="%", short_unit="%")
            elif "(days)" in name_lower:
                update_metadata(tb, column, 1, unit="days", short_unit="")
            elif "index" in name_lower:
                update_metadata(tb, column, 1, unit="index", short_unit="")
            elif "index" in name_lower:
                update_metadata(tb, column, 1, unit="index", short_unit="")
            elif "(current us$)" in name_lower:
                update_metadata(tb, column, 1, unit="current US$", short_unit="current $")
            elif "number" in name_lower:
                update_metadata(tb, column, 0, unit="number", short_unit="")

            else:
                # Default metadata update when no other conditions are met.
                update_metadata(tb, column, 0, " ", " ")
    return tb
