"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import owid.catalog.processing as pr
import pandas as pd
import shared
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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


def add_data_for_regions(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()
    # Aggregates for adjusted years of schooling and harmonized learning scores
    aggregations = {
        column: "mean"
        for column in tb_with_regions.columns
        if column
        in [
            "HD.HCI.LAYS",
            "HD.HCI.LAYS.FE",
            "HD.HCI.LAYS.MA",
            "HD.HCI.HLOS",
            "HD.HCI.HLOS.FE",
            "HD.HCI.HLOS.MA",
        ]
    }

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )
        tb_with_regions = shared.add_region_aggregates_education(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.5,
            aggregations=aggregations,
        )
    return tb_with_regions


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #

    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education"))

    # Read table from meadow dataset.
    tb = ds_meadow["education"]
    # Save metadata for later use
    metadata = tb.metadata

    #
    # Process data.
    #
    tb.reset_index(inplace=True)

    # Columns containing metadata
    metadata_columns = [
        "indicator_name",
        "short_definition",
        "long_definition",
        "source",
        "aggregation_method",
        "statistical_concept_and_methodology",
        "limitations_and_exceptions",
        "general_comments",
    ]

    # Save the table with just metadata
    metadata_tb = tb.loc[
        :,
        ["indicator_code"] + metadata_columns,
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
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb = tb.reset_index()

    # Find the maximum value in the 'HD.HCI.HLOS' column
    max_value = tb["HD.HCI.HLOS"].max()

    # Normalize every value in the 'HD.HCI.HLOS' column by the maximum value (How many years of effective learning do you get for every year of education)
    tb["normalized_hci"] = tb["HD.HCI.HLOS"] / max_value

    # Combine recent literacy estimates and expenditure data with historical estimates from a migrated dataset
    tb, combined_literacy_description, combined_expenditure_description = combine_historical_literacy_expenditure(tb)

    # Load additional datasets for region and income group information for regional aggregates
    ds_regions = paths.load_dependency("regions")
    ds_income_groups = paths.load_dependency("income_groups")
    tb = add_data_for_regions(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Set an appropriate index and sort.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Add the metadata back to the table
    tb.metadata = metadata
    # Add metadata by finding the descriptions and sources using the indicator codes.
    tb = add_metadata(tb, metadata_tb, combined_literacy_description, combined_expenditure_description)
    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_historical_literacy_expenditure(tb):
    """
    Combines historical and recent literacy and expenditure data into a single DataFrame.

    This function loads historical literacy and expenditure data, merges them with the recent data in
    the input DataFrame 'tb' based on 'year' and 'country' columns. It then creates two new columns:
    'combined_literacy' and 'combined_expenditure', which hold the respective values, preferring recent
    data when available.
    """
    # Load historical literacy
    ds_literacy = cast(Dataset, paths.load_dependency("literacy_rates"))
    tb_literacy = ds_literacy["literacy_rates"]

    # Load historical literacy expenditure data
    ds_expenditure = cast(Dataset, paths.load_dependency("public_expenditure"))
    tb_expenditure = ds_expenditure["public_expenditure"]

    historic_literacy = (
        tb_literacy[["literacy_rates__world_bank__cia_world_factbook__and_other_sources"]].reset_index().copy()
    )
    historic_expenditure = (
        tb_expenditure[["public_expenditure_on_education__tanzi__and__schuktnecht__2000"]].reset_index().copy()
    )
    # Recent literacy rates
    recent_literacy = tb[["year", "country", "SE.ADT.LITR.ZS"]].copy()

    # Recent public expenditure
    recent_expenditure = tb[["year", "country", "SE.XPD.TOTL.GD.ZS"]].copy()

    # Merge the historic and more recent literacy data based on 'year' and 'country'
    combined_df = pr.merge(
        historic_literacy,
        recent_literacy,
        on=["year", "country"],
        how="outer",
        suffixes=("_historic_lit", "_recent_lit"),
    ).copy_metadata(from_table=recent_literacy)

    # Merge the historic expenditure with newly created literacy table based on 'year' and 'country'
    combined_df = pr.merge(combined_df, historic_expenditure, on=["year", "country"], how="outer").copy_metadata(
        from_table=combined_df
    )

    # Merge the recent expenditure with newly created literacy and historic expenditure table based on 'year' and 'country'
    combined_df = pr.merge(
        combined_df, recent_expenditure, on=["year", "country"], how="outer", suffixes=("_historic_exp", "_recent_exp")
    ).copy_metadata(from_table=combined_df)

    # Define the functions to decide which value to keep (prefer more recent World Bank data; if NaN pick the historic ones (which could also be NaN))
    def decide_literacy(row):
        return (
            row["SE.ADT.LITR.ZS"]
            if pd.notna(row["SE.ADT.LITR.ZS"])
            else row["literacy_rates__world_bank__cia_world_factbook__and_other_sources"]
        )

    def decide_expenditure(row):
        return (
            row["SE.XPD.TOTL.GD.ZS"]
            if pd.notna(row["SE.XPD.TOTL.GD.ZS"])
            else row["public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
        )

    # Apply the functions to prioritise more recent datat and create new columns
    combined_df["combined_literacy"] = combined_df.apply(decide_literacy, axis=1)
    combined_df["combined_expenditure"] = combined_df.apply(decide_expenditure, axis=1)

    # Now, merge the relevant columns in newly created table that includes both historic and more recent data back into the original tb based on 'year' and 'country'
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_literacy", "combined_expenditure"]],
        on=["year", "country"],
        how="outer",
    ).copy_metadata(from_table=tb)

    # Keep descriptions from the original historic datasets
    combined_literacy_description = ds_literacy.metadata.sources[0].description
    combined_expenditure_description = ds_expenditure.metadata.sources[0].description

    return tb, combined_literacy_description, combined_expenditure_description


def add_metadata(
    tb: Table, metadata_tb: Table, combined_literacy_description, combined_expenditure_description
) -> None:
    """
    Adds metadata by fetching details from the table with descriptions and sources originally retrieved in snapshot using the World Bank API.

    Args:
        tb (Table): Table containing columns to which metadata needs to be added.
        metadata_tb (Table): Table containing descriptions and sources for indicators (matched by indicator_code)

    Returns:
        Table: The table with updated metadata.
    """
    # List of columns that were calculated in the etl for which metadata won't be available
    custom_cols = [
        "normalized_hci",
        "combined_literacy",
        "combined_expenditure",
    ]
    # Loop through the DataFrame columns
    for column in tqdm(tb.columns, desc="Processing metadata for indicators"):
        origin = metadata_tb[metadata_tb.columns[0]].metadata.origins[0]
        if column not in custom_cols:
            # Extract the title from the default metadata to find the corresponding World Bank indicator
            indicator_to_find = tb[column].metadata.title

            # Extract relevant name, description and source from the metadata table using the WB code
            name = (
                metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "indicator_name"]
                .str.replace("‚", "")  # commas caused problems when renaming variables later on
                .iloc[0]
            )

            description = metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "long_definition"].iloc[0]
            source = metadata_tb.loc[metadata_tb["indicator_code"] == indicator_to_find, "source"].iloc[0]
            aggregation_method = metadata_tb.loc[
                metadata_tb["indicator_code"] == indicator_to_find, "aggregation_method"
            ].iloc[0]
            statistical_concept_and_methodology = metadata_tb.loc[
                metadata_tb["indicator_code"] == indicator_to_find, "statistical_concept_and_methodology"
            ].iloc[0]
            limitations_and_exceptions = metadata_tb.loc[
                metadata_tb["indicator_code"] == indicator_to_find, "limitations_and_exceptions"
            ].iloc[0]

            # Replace NaN values with a placeholder string
            source = "" if pd.isna(source) else source
            aggregation_method = "" if pd.isna(aggregation_method) else aggregation_method
            statistical_concept_and_methodology = (
                "" if pd.isna(statistical_concept_and_methodology) else statistical_concept_and_methodology
            )
            limitations_and_exceptions = "" if pd.isna(limitations_and_exceptions) else limitations_and_exceptions

            # Truncate the last 5 words if the length of the string exceeds 250 characters
            if len(name) > 250:
                # Separate the string into words and truncate
                words = name.split()
                # Get all words up to the fifth-to-last word
                selected_words = words[:-10]
                # Reconstruct the selected words into a single string
                name = " ".join(selected_words)

            # Convert the name to underscore format
            new_column_name = underscore(name)  # Convert extracted name to underscore format

            # If more detailed description is currently missing in the API --> use the long title as a description
            if str(description) == "nan":
                description = name
                source = ""

            # Update the column names and metadata
            tb.rename(columns={column: new_column_name}, inplace=True)

            # Now build the description string conditionally
            components = []

            if description:
                components.append(f"{description}\nWorld Bank variable id: {indicator_to_find}")
            if source:
                components.append(f"Source: {source}")
            if aggregation_method:
                components.append(f"Aggregation method: {aggregation_method}")
            if statistical_concept_and_methodology:
                components.append(f"Statistical concept and methodology: {statistical_concept_and_methodology}")
            if limitations_and_exceptions:
                components.append(f"Limitations and exceptions: {limitations_and_exceptions}")

            description_string = (
                "\n\n".join(components) if components else "No detailed metadata available from World Bank."
            )

            tb[new_column_name].metadata.description_from_producer = description_string
            tb[new_column_name].metadata.title = name
            tb[new_column_name].metadata.processing = "minor"
            tb[new_column_name].metadata.origins = [origin]

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

        elif column == "normalized_hci":
            tb[column].metadata.origins = [origin]
            tb[column].metadata.title = "Normalised harmonized learning score"
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "score"
            tb[column].metadata.short_unit = ""
        elif column == "combined_literacy":
            tb[column].metadata.origins = [origin]
            tb[column].metadata.title = "Historical and more recent literacy estimates"
            tb[column].metadata.description = (
                "**Historical literacy data:**\n\n"
                + combined_literacy_description
                + "\n\n"
                + "**Recent estimates:**\n\n"
                + "Percentage of the population between age 25 and age 64 who can, with understanding, read and write a short, simple statement on their everyday life. Generally, ‘literacy’ also encompasses ‘numeracy’, the ability to make simple arithmetic calculations. This indicator is calculated by dividing the number of literates aged 25-64 years by the corresponding age group population and multiplying the result by 100."
                + "\n\n"
                + "World Bank variable id: UIS.LR.AG25T64. Source: UNESCO Institute for Statistics"
            )
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 2
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
        elif column == "combined_expenditure":
            tb[column].metadata.origins = [origin]
            tb[column].metadata.title = "Historical and more recent expenditure estimates"
            tb[column].metadata.description = (
                "**Historical expenditure data:**\n\n"
                + combined_expenditure_description
                + "\n\n"
                + "**Recent estimates:**\n\n"
                + "General government expenditure on education (current, capital, and transfers) is expressed as a percentage of GDP. It includes expenditure funded by transfers from international sources to government. General government usually refers to local, regional and central governments."
                + "\n\n"
                + "World Bank variable id: SE.XPD.TOTL.GD.ZS. Source: UNESCO Institute for Statistics (UIS). UIS.Stat Bulk Data Download Service. Accessed October 24, 2022."
            )
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 2
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
    return tb
