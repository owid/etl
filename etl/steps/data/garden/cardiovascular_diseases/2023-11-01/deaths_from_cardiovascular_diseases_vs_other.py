"""Process and aggregate WHO mortality data based on specified cause groupings to highlight the share of deaths from cardiovascular diseases vs other categories."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix for all causes
PREFIX = "share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_"

# Define a constant mapping for aggregating specific causes of deaths into broader categories.
CAUSE_MAPPING = {
    # Congenital & Maternal Conditions
    f"{PREFIX}congenital_anomalies": "Congenital and maternal conditions",
    f"{PREFIX}maternal_conditions": "Congenital and maternal conditions",
    f"{PREFIX}perinatal_conditions": "Congenital and maternal conditions",
    f"{PREFIX}sudden_infant_death_syndrome": "Congenital and maternal conditions",
    # Endocrine & Nutritional Disorders
    f"{PREFIX}diabetes_mellitus__blood_and_endocrine_disorders": "Endocrine and nutritional disorders",
    f"{PREFIX}nutritional_deficiencies": "Endocrine and nutritional disorders",
    # Digestive & Oral Health
    f"{PREFIX}digestive_diseases": "Digestive and oral health",
    f"{PREFIX}oral_conditions": "Digestive and oral health",
    # Ill-defined Causes & Injuries
    f"{PREFIX}ill_defined_diseases": "Ill-defined causes and injuries",
    f"{PREFIX}ill_defined_injuries": "Ill-defined causes and injuries",
    f"{PREFIX}intentional_injuries": "Ill-defined causes and injuries",
    f"{PREFIX}unintentional_injuries": "Ill-defined causes and injuries",
    # Infectious Diseases
    f"{PREFIX}infectious_and_parasitic_diseases": "Infectious diseases",
    f"{PREFIX}respiratory_infections": "Infectious diseases",
    # Cancer & Neoplasms
    f"{PREFIX}malignant_neoplasms": "Cancer and neoplasms",
    f"{PREFIX}other_neoplasms": "Cancer and neoplasms",
    # Other Diseases & Conditions
    f"{PREFIX}sense_organ_diseases": "Other diseases and conditions",
    f"{PREFIX}musculoskeletal_diseases": "Other diseases and conditions",
    f"{PREFIX}skin_diseases": "Other diseases and conditions",
    # Neurological & Mental Health
    f"{PREFIX}neuropsychiatric_conditions": "Neurological and mental health",
    # Respiratory Diseases
    f"{PREFIX}respiratory_diseases": "Respiratory diseases",
    f"{PREFIX}cardiovascular_diseases": "Cardiovascular diseases",
}


def run(dest_dir: str) -> None:
    # Load the 'mortality_database' dataset.
    ds_garden_who = paths.load_dataset("mortality_database")

    # Identify table names that are relevant for our analysis (those that include '_sexes__all_ages').
    table_names = [name for name in ds_garden_who.table_names if "_sexes__all_ages" in name]

    # Assert that the number of tables matches the expected count
    assert len(table_names) - 1 == len(
        set(CAUSE_MAPPING.keys())
    ), f"Expected {len(set(CAUSE_MAPPING.keys()))} tables, but found {len(table_names)-1}."
    # Initialize a list to hold the cause data from all tables.
    all_causes = []

    # Extract columns related to the share of deaths from each table and add to the list.
    for table_name in table_names:
        tb = ds_garden_who[table_name]
        share_of_deaths_cols = [col for col in tb.columns if "share_of_total_deaths_" in col.lower()]
        all_causes.append(tb[share_of_deaths_cols])

    # Concatenate cause data across all tables.
    all_causes = pr.concat(all_causes, axis=1)

    # Assert that the share of deaths from all causes is 100% for all rows
    assert (
        all_causes["share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_all_causes"] == 100
    ).all(), "Not all entries in the column 'share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_all_causes' are 100%."

    # Drop share of deaths from all causes column
    all_causes = all_causes.drop(
        "share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_all_causes", axis=1
    )
    # Assert that the number of unique indicators in CAUSE_MAPPING matches the number of unique columns
    assert (
        len(set(CAUSE_MAPPING.keys())) == len(all_causes.columns)
    ), "The number of indicators from CAUSE_MAPPING does not match the number of 'share_of_total_deaths_' columns found in the tables."
    # Invert the mapping to facilitate grouping.
    reverse_mapping = {col: category for col, category in CAUSE_MAPPING.items()}

    # Group and sum columns based on broader categories.
    grouped_tb = all_causes.groupby(reverse_mapping, axis=1).sum()

    # Compute the percentage of deaths with unidentified causes.
    row_sums = grouped_tb.sum(axis=1)
    assert (0 <= row_sums).all() and (
        row_sums <= 100
    ).all(), "Some countries report a percentage of deaths from all causes that falls outside the 0-100 range."
    remaining_percentage = 100 - row_sums
    grouped_tb["unidentified_cause"] = remaining_percentage

    # Set metadata and format the dataframe for saving.
    grouped_tb.metadata.short_name = paths.short_name
    grouped_tb = grouped_tb.underscore()

    # Add origins metadata to the aggregated table (remove when the new WHO mortality database is updated with new metadata)
    from etl.data_helpers.misc import add_origins_to_mortality_database

    grouped_tb = add_origins_to_mortality_database(tb_who=grouped_tb)

    # Save the processed data.
    ds_garden = create_dataset(dest_dir, tables=[grouped_tb], check_variables_metadata=True)
    ds_garden.save()
