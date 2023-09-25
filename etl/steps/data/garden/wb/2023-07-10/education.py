"""Load a meadow dataset and create a garden dataset."""

from typing import cast

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
    # Adding share of female students in pre-primary school and total funding per student (household + government)
    tb["percentage_of_female_pre_primary_students)"] = (tb["SE.PRE.ENRL.FE"] / tb["SE.PRE.ENRL"]) * 100
    tb["total_funding_per_student_ppp"] = tb["UIS.XUNIT.PPPCONST.1.FSGOV"] + tb["UIS.XUNIT.PPPCONST.1.FSHH"]
    # Find the maximum value in the 'HD.HCI.HLOS' column
    max_value = tb["HD.HCI.HLOS"].max()

    # Normalize every value in the 'HD.HCI.HLOS' column by the maximum value (How many years of effective learning do you get for every year of educaiton)
    tb["normalized_hci"] = tb["HD.HCI.HLOS"] / max_value

    # Load additional datasets for region and income group information for regional aggregates
    ds_regions = paths.load_dependency("regions")
    ds_income_groups = paths.load_dependency("income_groups")

    tb = add_data_for_regions(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

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
    custom_cols = [
        "percentage_of_female_pre_primary_students",
        "percentage_of_female_tertiary_teachers",
        "total_funding_per_student_ppp",
        "normalized_hci",
    ]
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
        elif column == "total_funding_per_student_ppp":
            tb[column].metadata.title = "Total funding per student (in PPP)"
            tb[column].metadata.display = {}
            tb[
                column
            ].metadata.description = "Combined total payments of households and governmental funding per primary student. The total payments of households (pupils, students and their families) for educational institutions (such as for tuition fees, exam and registration fees, contribution to Parent-Teacher associations or other school funds, and fees for canteen, boarding and transport) and purchases outside of educational institutions (such as for uniforms, textbooks, teaching materials, or private classes). 'Initial funding' means that government transfers to households, such as scholarships and other financial aid for education, are subtracted from what is spent by households. Note that in some countries for some education levels, the value of this indicator may be 0, since on average households may be receiving as much, or more, in financial aid from the government than what they are spending on education. Indicators for household expenditure on education should be interpreted with caution since data comes from household surveys which may not all follow the same definitions and concepts. These types of surveys are also not carried out in all countries with regularity, and for some categories (such as pupils in pre-primary education), the sample sizes may be low. In some cases where data on government transfers to households (scholarships and other financial aid) was not available, they could not be subtracted from amounts paid by households. Total general (local, regional and central, current and capital) initial government funding of education per student, includes transfers paid (such as scholarships to students), but excludes transfers received, in this case international transfers to government for education (when foreign donors provide education sector budget support or other support integrated in the government budget). Limitations: In some instances data on total government expenditure on education refers only to the Ministry of Education, excluding other ministries which may also spend a part of their budget on educational activities. There are also cases where it may not be possible to separate international transfers to government from general government expenditure on education, in which cases they have not been subtracted in the formula. "
            tb[column].metadata.display["numDecimalPlaces"] = 0
            tb[column].metadata.unit = "international-$"
            tb[column].metadata.short_unit = "$"
        elif column == "percentage_of_female_pre_primary_students":
            tb[column].metadata.title = "Share of female students in pre-primary education"
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
        elif column == "normalized_hci":
            tb[column].metadata.title = "Normalised harmonized learning score"
            tb[column].metadata.display = {}
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "score"
            tb[column].metadata.short_unit = ""
    return tb
