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


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #

    # Load meadow datasets.
    ds_meadow = paths.load_dataset("education")
    tb = ds_meadow["education"]

    # Load historical literacy
    ds_literacy = paths.load_dataset("literacy_rates")
    tb_literacy = ds_literacy["literacy_rates"]

    # Load historical literacy expenditure data
    ds_expenditure = paths.load_dataset("public_expenditure")
    tb_expenditure = ds_expenditure["public_expenditure"]

    #
    # Process data.
    #
    tb.reset_index(inplace=True)

    # Columns containing metadata
    metadata_columns = [
        "indicator_name",
        "long_definition",
        "source",
        "aggregation_method",
        "statistical_concept_and_methodology",
        "limitations_and_exceptions",
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
    tb = combine_historical_literacy_expenditure(tb, tb_literacy, tb_expenditure)

    # Compare two columnst that seem to have identical indicies (if values are the same then remove)
    if tb["SE.XPD.TOTL.GD.ZS"].equals(tb["SE.XPD.TOTL.GD.ZS."]):
        # If they are the same, drop one of the columns
        tb.drop("SE.XPD.TOTL.GD.ZS.", axis=1, inplace=True)
    else:
        print("The columns are not the same.")

    # Set an appropriate index and sort.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    # Add metadata by finding the descriptions and sources using the indicator codes.
    tb = add_metadata(tb, metadata_tb)
    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_historical_literacy_expenditure(tb, tb_literacy, tb_expenditure):
    """
    Merges historical and recent literacy and expenditure data into a single DataFrame.

    This function integrates data from two separate DataFrames containing historical literacy rates and
    public expenditure on education with a primary DataFrame. It merges these datasets based on common
    'year' and 'country' columns. The resulting DataFrame includes two new columns, 'combined_literacy'
    and 'combined_expenditure', which contain the respective literacy and expenditure data. The function
    prioritizes recent data over historical data when both are available.

    Parameters:
    - tb (DataFrame): The primary DataFrame containing recent literacy and expenditure data.
    - tb_literacy (DataFrame): A DataFrame containing historical literacy data with columns
      ['year', 'country', 'literacy_rates__world_bank__cia_world_factbook__and_other_sources'].
    - tb_expenditure (DataFrame): A DataFrame containing historical expenditure data with columns
      ['year', 'country', 'public_expenditure_on_education__tanzi__and__schuktnecht__2000'].

    The function handles missing data by favoring recent World Bank data; if this is not available,
    it falls back to historical data, which could also be missing (NaN).

    Returns:
    DataFrame: The merged DataFrame with new columns 'combined_literacy' and 'combined_expenditure',
    and metadata about data origins added to these columns.

    This function assumes that the input DataFrames share a common structure in terms of the 'year' and
    'country' columns, and that these columns are used as keys for merging the datasets.
    The function adds metadata to the new columns, indicating the origin of the data.
    """

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
    )

    # Merge the historic expenditure with newly created literacy table based on 'year' and 'country'
    combined_df = pr.merge(combined_df, historic_expenditure, on=["year", "country"], how="outer")

    # Merge the recent expenditure with newly created literacy and historic expenditure table based on 'year' and 'country'
    combined_df = pr.merge(
        combined_df, recent_expenditure, on=["year", "country"], how="outer", suffixes=("_historic_exp", "_recent_exp")
    )
    combined_df["combined_literacy"] = combined_df["SE.ADT.LITR.ZS"].fillna(
        combined_df["literacy_rates__world_bank__cia_world_factbook__and_other_sources"]
    )
    combined_df["combined_expenditure"] = combined_df["SE.XPD.TOTL.GD.ZS"].fillna(
        combined_df["public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
    )

    # Now, merge the relevant columns in newly created table that includes both historic and more recent data back into the original tb based on 'year' and 'country'
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_literacy", "combined_expenditure"]],
        on=["year", "country"],
        how="outer",
    )

    return tb


def add_metadata(tb: Table, metadata_tb: Table) -> None:
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
                components.append(f"{description}\n\nWorld Bank variable id: {indicator_to_find}")
            if source:
                components.append(f"Original source: {source}")
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
            elif "ratio" in name_lower and "duration" not in name_lower:
                update_metadata(tb, new_column_name, 1, "ratio", " ")
            elif "number of pupils" in name_lower:
                update_metadata(tb, new_column_name, 0, "pupils", " ")
            # Check if the column name contains "number", but not "rate" or "pasec".
            elif "number" in name_lower and "rate" not in name_lower and "pasec" not in name_lower:
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
            tb[column].metadata.title = "Normalised harmonized learning score"
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "score"
            tb[column].metadata.short_unit = ""

        elif column == "combined_literacy":
            tb[column].metadata.title = "Historical and more recent literacy estimates"
            tb[column].metadata.description_from_producer = (
                "**Recent estimates:**\n\n"
                + "Percentage of the population between age 25 and age 64 who can, with understanding, read and write a short, simple statement on their everyday life. Generally, ‘literacy’ also encompasses ‘numeracy’, the ability to make simple arithmetic calculations. This indicator is calculated by dividing the number of literates aged 25-64 years by the corresponding age group population and multiplying the result by 100."
                + "\n\n"
                + "World Bank variable id: UIS.LR.AG25T64"
                + "\n\n"
                + "Original source: UNESCO Institute for Statistics"
                + "\n\n"
                "**Historical literacy data:**\n\n"
                + """The historical estimates in this long-run cross-country dataset were derived from a blend of diverse sources, each contributing to different time periods. For data before 1800, the dataset relies on the work of Buringh and Van Zanden (2009), which offers insights into literacy through the lens of manuscript and book production in Europe from the sixth to the eighteenth centuries. For the years 1820 and 1870 (excluding the United States), it incorporates data from Broadberry and O'Rourke's "The Cambridge Economic History of Modern Europe." The United States data comes from the National Center for Education Statistics. Additionally, global estimates for the period 1820-2000 are drawn from van Zanden and colleagues’ "How Was Life?: Global Well-being since 1820," an OECD publication. For historical estimates specific to Latin America, the dataset uses the Oxford Latin American Economic History Database (OxLAD). Each source follows a consistent conceptual definition of literacy, although discrepancies among sources are acknowledged, necessitating cautious interpretation of year-to-year changes. The dataset also includes instances where specific sources were preferred, such as opting for OxLAD data over the World Bank for Paraguay in 1982 due to significant differences in literacy rate estimates."""
            )
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 2
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
        elif column == "combined_expenditure":
            tb[column].metadata.title = "Historical and more recent expenditure estimates"
            tb[column].metadata.description_from_producer = (
                "**Historical expenditure data:**\n\n"
                + "Historical data in this dataset is based on a wide array of sources, reflecting a comprehensive approach to data collection across different time periods and regions. However, the diverse nature of these sources leads to inconsistencies, as methodologies and data quality vary between sources. For instance, older sources like the League of Nations Statistical Yearbook or Mitchell's 1962 data may use different metrics or collection methods compared to more modern sources like the OECD Education reports or UN surveys. This variance in source material and methodology means that direct comparisons across different years or countries might be challenging, necessitating careful interpretation and cross-reference for accuracy. The dataset serves as a rich historical repository but also underscores the complexities and challenges inherent in compiling and harmonizing historical data from multiple, diverse sources."
                + "\n\n"
                + "**Recent estimates:**\n\n"
                + "General government expenditure on education (current, capital, and transfers) is expressed as a percentage of GDP. It includes expenditure funded by transfers from international sources to government. General government usually refers to local, regional and central governments."
                + "\n\n"
                + "World Bank variable id: SE.XPD.TOTL.GD.ZS"
                + "\n\n"
                + "Original source: UNESCO Institute for Statistics (UIS). UIS.Stat Bulk Data Download Service. Accessed October 24, 2022."
            )
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 2
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
    return tb
