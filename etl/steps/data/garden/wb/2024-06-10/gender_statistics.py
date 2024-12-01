"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
PARENTAL_LEAVE = (
    "**Assumptions**\n\n"
    "It is assumed that the woman in question:\n\n"
    "- Resides in the economy's main business city.\n"
    "- Has reached the legal age of majority and is capable of making decisions as an adult, is in good health and has no criminal record.\n"
    "- Is a lawful citizen of the economy being examined.\n"
    "- Is a cashier in the food retail sector in a supermarket or grocery store that has 60 employees.\n"
    "- Is a cisgender, heterosexual woman in a monogamous first marriage registered with the appropriate authorities (de facto marriages and customary unions are not measured).\n"
    "- Is of the same religion as her husband.\n"
    "- Is in a marriage under the rules of the default marital property regime, or the most common regime for that jurisdiction, which will not change during the course of the marriage.\n"
    "- Is not a member of a union, unless membership is mandatory. Membership is considered mandatory when collective bargaining agreements cover more than 50 percent of the workforce in the food retail sector and when they apply to individuals who were not party to the original collective bargaining agreement.\n\n"
    "For the questions on maternity, paternity, and parental leave, it is assumed that:\n\n"
    "- The woman gave birth to her first child without complications on October 1, 2023, and her child is in good health. Answers will therefore correspond to legislation in force as of October 1, 2023, even if the law provides for changes over time.\n"
    "- Both parents have been working long enough to accrue any maternity, paternity, and parental benefits.\n"
    "- If maternity benefit systems are not mandatory or they were not in force as of October 1, 2023, they are not measured."
)


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
    tb = tb.pivot(index=["country", "year"], columns="wb_seriescode", values="value").reset_index()

    # Filter for Slovakia and the specific year range, then update the value as it was not correctly recorded (the source will change it in their next update)
    tb.loc[(tb["country"] == "Slovakia") & (tb["year"].between(1971, 1993)), "sg_law_eqrm_wk"] = 1

    # Add metadata by finding the descriptions and sources using indicator codes
    tb = add_metadata(tb, metadata_tb)

    # Set an appropriate index and sort.
    tb = tb.format(["country", "year"])

    # Combine maternity and paternity leave indicators (time only available TO each parent)
    tb["total_maternity_leave_to"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"]
    tb["total_paternity_leave_to"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"]

    # Combine maternity and paternity leave indicators (time available days FOR each parent)
    tb["total_maternity_leave_for"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"] + tb["sh_par_leve"]
    tb["total_paternity_leave_for"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"] + tb["sh_par_leve"]

    # Add relevant metadata to the newly created columns
    add_metadata_description(tb, "total_maternity_leave_for", ["sh_mmr_leve", "sh_par_leve_fe", "sh_par_leve"])
    add_metadata_description(tb, "total_paternity_leave_for", ["sh_par_leve_ma", "sh_ptr_leve", "sh_par_leve"])

    add_metadata_description(tb, "total_maternity_leave_to", ["sh_mmr_leve", "sh_par_leve_fe"])
    add_metadata_description(tb, "total_paternity_leave_to", ["sh_par_leve_ma", "sh_ptr_leve"])

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

            # Construct the components list with field names
            components = []
            for field in metadata_fields:
                value = metadata[field]
                if value:
                    component = f"**{field.replace('_', ' ').capitalize()} from World Bank:** {value}"
                    components.append(component)
            components.append(f"**World Bank variable id:** {column}")

            # Create the metadata description from producer string
            description_string = "\n\n".join(filter(None, components))
            if not description_string:
                description_string = "No detailed metadata available from World Bank."

            # Add specific assumptions for parental leave indicators
            if "SH." in column and "LEVE" in column:
                description_string = f"{PARENTAL_LEAVE}\n\n" + description_string

            meta = tb[column].metadata
            meta.description_from_producer = description_string
            meta.title = name
            meta.processing = "minor"
            meta.display = {}

            # Changing the producer name for the variables that come fron the WBL as requested by the source
            if metadata["source"] == "World Bank: Women, Business and the Law. https://wbl.worldbank.org/":
                meta.origins[0].producer = "World Bank - Women, Business and the Law"

            # Convert the 'name' variable to lowercase to make text matching easier
            name_lower = tb[column].title.lower()

            #
            # Update metadata units, short_units and number of decimal places to display depending on what keywords the variable name contains
            #
            # Fill out units and decimal places
            if "%" in name_lower:
                update_metadata(meta, display_decimals=1, unit="%", short_unit="%")
            elif "(days)" in name_lower:
                update_metadata(meta, display_decimals=1, unit="days", short_unit="")
            elif "index" in name_lower:
                update_metadata(meta, display_decimals=1, unit="index", short_unit="")
            elif "index" in name_lower:
                update_metadata(meta, display_decimals=1, unit="index", short_unit="")
            elif "(current us$)" in name_lower:
                update_metadata(meta, display_decimals=1, unit="current US$", short_unit="current $")
            elif "number" in name_lower:
                update_metadata(meta, display_decimals=0, unit="number", short_unit="")

            else:
                # Default metadata update when no other conditions are met.
                update_metadata(meta, 0, " ", " ")
    return tb


def update_metadata(meta, display_decimals, unit, short_unit):
    """
    Update metadata attributes of a specified column in the given table.

    Args:
    meta (obj): Metadata object of the column to update.
    display_decimals (int): Number of decimal places to display.
    unit (str): The full name of the unit of measurement for the column data.
    short_unit (str, optional): The abbreviated form of the unit. Defaults to an empty space.

    Returns:
    None: The function updates the table in-place.
    """
    meta.display["numDecimalPlaces"] = display_decimals
    meta.unit = unit
    meta.short_unit = short_unit


def add_metadata_description(tb, column_name, indicators):
    """Adds metadata description to a given column in tb."""
    description = (
        f"**This indicator is a sum of {len(indicators)} different leave indicators provided by World Bank:**\n\n"
    )
    for indicator in indicators:
        if indicator in tb and hasattr(tb[indicator], "metadata"):
            description += (
                f"The indicator '{tb[indicator].metadata.title}' is described by World Bank as:\n\n"
                f"{tb[indicator].metadata.description_from_producer}\n\n"
            )
    tb[column_name].metadata.description_from_producer = description
