"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix for all causes
PREFIX = "share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_"

# Define a constant mapping for aggregating specific causes of deaths into broader categories.
CAUSE_MAPPING = {
    # Congenital & Maternal Conditions
    f"{PREFIX}congenital_anomalies": "Congenital & Maternal Conditions",
    f"{PREFIX}maternal_conditions": "Congenital & Maternal Conditions",
    f"{PREFIX}perinatal_conditions": "Congenital & Maternal Conditions",
    f"{PREFIX}sudden_infant_death_syndrome": "Congenital & Maternal Conditions",
    # Endocrine & Nutritional Disorders
    f"{PREFIX}diabetes_mellitus__blood_and_endocrine_disorders": "Endocrine & Nutritional Disorders",
    f"{PREFIX}nutritional_deficiencies": "Endocrine & Nutritional Disorders",
    # Digestive & Oral Health
    f"{PREFIX}digestive_diseases": "Digestive & Oral Health",
    f"{PREFIX}oral_conditions": "Digestive & Oral Health",
    # Ill-defined Causes & Injuries
    f"{PREFIX}ill_defined_diseases": "Ill-defined Causes & Injuries",
    f"{PREFIX}ill_defined_injuries": "Ill-defined Causes & Injuries",
    f"{PREFIX}intentional_injuries": "Ill-defined Causes & Injuries",
    f"{PREFIX}unintentional_injuries": "Ill-defined Causes & Injuries",
    # Infectious Diseases
    f"{PREFIX}infectious_and_parasitic_diseases": "Infectious Diseases",
    f"{PREFIX}respiratory_infections": "Infectious Diseases",
    # Cancer & Neoplasms
    f"{PREFIX}malignant_neoplasms": "Cancer & Neoplasms",
    f"{PREFIX}other_neoplasms": "Cancer & Neoplasms",
    # Other Diseases & Conditions
    f"{PREFIX}sense_organ_diseases": "Other Diseases & Conditions",
    f"{PREFIX}musculoskeletal_diseases": "Other Diseases & Conditions",
    f"{PREFIX}skin_diseases": "Other Diseases & Conditions",
    # Neurological & Mental Health
    f"{PREFIX}neuropsychiatric_conditions": "Neurological & Mental Health",
    # Respiratory Diseases
    f"{PREFIX}respiratory_diseases": "Respiratory Diseases",
}


def run(dest_dir: str) -> None:
    """
    Process and aggregate WHO mortality data based on specified cause groupings to highlight the share of deaths from cardiovascular diseases vs other categories.

    """

    # Load the 'mortality_database' dataset.
    ds_garden_who = paths.load_dataset("mortality_database")

    # Identify table names that are relevant for our analysis (those that include '_sexes__all_ages').
    table_names = [name for name in ds_garden_who.table_names if "_sexes__all_ages" in name]

    # Initialize a list to hold the cause data from all tables.
    all_causes = []

    # Extract columns related to the share of deaths from each table and add to the list.
    for table_name in table_names:
        tb = ds_garden_who[table_name]
        share_of_deaths_cols = [col for col in tb.columns if "share_of_total_deaths_" in col.lower()]
        all_causes.append(tb[share_of_deaths_cols])

    # Concatenate cause data across all tables.
    all_causes = pr.concat(all_causes, axis=1)

    # Invert the mapping to facilitate grouping.
    reverse_mapping = {col: category for col, category in CAUSE_MAPPING.items()}

    # Group and sum columns based on broader categories.
    summed_df = all_causes.groupby(reverse_mapping, axis=1).sum()

    # Combine the aggregated data with specific columns, e.g., cardiovascular diseases.
    df_with_cardiovascular_diseases = pd.concat(
        [
            summed_df,
            all_causes["share_of_total_deaths_in_both_sexes_aged_all_ages_years_that_are_from_cardiovascular_diseases"],
        ],
        axis=1,
    )

    # Compute the percentage of deaths with unidentified causes.
    row_sums = df_with_cardiovascular_diseases.sum(axis=1)
    remaining_percentage = 100 - row_sums
    df_with_cardiovascular_diseases["unidentified_cause"] = remaining_percentage

    # Set metadata and format the dataframe for saving.
    df_with_cardiovascular_diseases.metadata.short_name = paths.short_name
    df_with_cardiovascular_diseases = df_with_cardiovascular_diseases.underscore()

    # Save the processed data.
    ds_garden = create_dataset(
        dest_dir, tables=[df_with_cardiovascular_diseases], default_metadata=ds_garden_who.metadata
    )
    ds_garden.save()
