"""Load a meadow dataset and create a garden dataset."""

import re

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
    ds_meadow = paths.load_dataset("education_opri")

    # Read table from meadow dataset.
    tb = ds_meadow["education_opri"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    # TO DO: Add metadata columns to the table
    summary_dict = create_metadata_dictionary(tb)
    tb_pivoted = add_metadata_description(tb_pivoted, summary_dict)
    # Add metadata columns to the table
    tb_pivoted = tb_pivoted.reset_index()
    tb_pivoted = tb_pivoted.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_metadata_dictionary(tb):
    # Drop columns that have metadata that's too detailed and won't work with the current datapage layout
    tb = tb.drop(["magnitude", "qualifier", "source__data_sources", "change__data_reporting"], axis=1)

    # Generate the summary string for metadata columns for each indicator
    metadata_columns = [
        "under_coverage__students_or_individuals",
    ]
    summary_dict = {}

    for indicator in tqdm(tb["indicator_label_en"].unique(), desc="Processing indicators"):
        tb_indicator = tb[tb["indicator_label_en"] == indicator]
        summary_strings = {}
        for col in metadata_columns:
            if col == "change__data_reporting":
                meta = "Change in data reporting:"
            elif col == "under_coverage__students_or_individuals":
                meta = "Under coverage:"
            unique_values = tb_indicator[col].astype(str).dropna().unique()
            if not all(value == "nan" for value in unique_values):
                summary_per_unique_value = {}
                for value in unique_values:
                    if value != "nan":
                        countries_years = (
                            tb_indicator[tb_indicator[col] == value].groupby(["country", "year"], observed=False).size()
                        )
                        unique_countries = (
                            countries_years[countries_years.values == 1].index.get_level_values(0).unique().tolist()
                        )
                        summary_per_unique_country = {}
                        for country in unique_countries:
                            years = countries_years.loc[country]
                            years_list = years[years.values == 1].index.get_level_values(0).tolist()
                            summary_per_unique_country[country] = years_list
                        summary_per_unique_value[value] = summary_per_unique_country
                if summary_per_unique_value:
                    summary_strings[meta] = summary_per_unique_value
        summary_dict[indicator] = summary_strings
    return summary_dict


def add_metadata_description(tb, summary_dict):
    for column in tb.columns:
        meta = tb[column].metadata

        # Use the function
        md_table = dict_to_string(summary_dict[column])
        meta.description_from_producer = md_table
        meta.title = column
        meta.processing = "minor"
        meta.display = {}

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
        elif "male" in name_lower:
            update_metadata(meta, display_decimals=0, unit="boys", short_unit="")
        elif "female" in name_lower:
            update_metadata(meta, display_decimals=0, unit="girls", short_unit="")
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


def dict_to_string(d):
    if not d:
        return "No metadata."
    else:
        paragraphs = []
        for key, nested_dict in d.items():
            paragraph = f"**{key}**\n"
            paragraph += "\n".join(
                f"**{nested_key}** - {nested_value}+\n" for nested_key, nested_value in nested_dict.items()
            )
            paragraph = paragraph.replace("(", "").replace(")", "") + "\n\n"
            paragraph = re.sub(r"[\[\]']+", "", paragraph)
            paragraph = paragraph.replace("{", "").replace("}", "")

            paragraphs.append(paragraph)
        return "\n\n".join(paragraphs)
