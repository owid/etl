"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMN_MAPPING = {
    "Country": "country",
    "Country/Territory": "territory_type",
    "Number of people experiencing homelessness": "people_experiencing_homelessness",
    "Population": "population",
    " # people homeless per 10,000 ": "people_homeless_per_10k",
    "Definition from Source": "definition_from_source",
    "IGH Framework category": "igh_framework_category",
    "Notes on Data - Describe any conflicting data here. ": "notes_on_data",
    "Methodology": "methodology",
    "Year": "year",
    "Data Source Type": "data_source_type",
    "Data Source": "data_source",
    "Other/ Misc Notes": "other_misc_notes",
    "Methodology listed.": "methodology_listed",
    "Homelessness enumeration is from primary data source. ": "homelessness_enumeration_primary_data_source",
    "Enumeration conducted within the last four years.": "enumeration_conducted_within_last_four_years",
    "Enumeration conducted at the same time of year and/or enumeration is based on routinely updated or real-time data (e.g. administrative data)": "enumeration_conducted_same_time_of_year",
    "Definition includes people without accommodation (sleeping on the streets, open or public places, forms of transport.)": "definition_includes_people_without_accommodation",
    "Definition includes living in emergency accommodation, temporary shelters, hostels, or domestic violence refuges.": "definition_includes_living_in_emergency_accommodation",
    "Definition includes insecure or inadequate housing (sleeping at a someone's house on a temporary basis, extremely overcrowded conditions, living in trailers/tents/accommodation not fit for human habitation, and temporary structures inc. informal settlements).": "definition_includes_insecure_or_inadequate_housing",
    "Geographic scope listed including disaggregation by region, city, and/or community. ": "geographic_scope_listed",
    "Disaggregation includes gender.": "disaggregation_includes_gender",
    "Disaggregation includes age.": "disaggregation_includes_age",
    "Disaggregation includes at least two of the following: disability status, income, race and/or ethnicity, migratory status, length of time homeless, and relevant health data.": "disaggregation_includes_at_least_two",
    "Total-Homeless Data Scorecard": "total_homeless_data_scorecard",
}

# Define countries to remove
COUNTRIES_TO_REMOVE = ["England", "Northern Ireland", "Scotland", "Wales"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("better_data_homelessness.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, sheet_name="BDP 2024")

    #
    # Process data.
    #
    # Rename columns.
    tb = tb.rename(columns=COLUMN_MAPPING, errors="raise")

    # Make people_experiencing_homelessness a string to filter out data.
    tb["people_experiencing_homelessness"] = tb["people_experiencing_homelessness"].astype("string")

    # Drop rows in people_experiencing_homelessness equalling "No local or national data available".
    tb = tb[tb["people_experiencing_homelessness"] != "No local or national data available"].reset_index(drop=True)
    tb = tb[~tb["country"].isin(COUNTRIES_TO_REMOVE)].reset_index(drop=True)

    # Drop missing country values.
    tb = tb.dropna(subset=["country"]).reset_index(drop=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
