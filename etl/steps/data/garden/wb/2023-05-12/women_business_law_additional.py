"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Keeps a selection of variables and renames them."""

    # Define different names for columns
    dict_rename = {
        "year": "year",
        "economy": "country",
        "what_is_the_legal_age_of_marriage_for_girls": "legal_age_of_marriage",
        "are_there_any_exceptions_to_the_legal_age_of_marriage": "exceptions_legal_age_of_marriage",
        "is_marriage_under_the_legal_age_void_or_explicitly_prohibited": "marriage_under_legal_age_void_or_explicitly_prohibited",
        "are_there_penalties_for_authorizing_or_entering_into_child_or_early_marriage": "penalties_for_authorizing_or_entering_into_early_marriage",
        "does_the_legislation_establish_clear_criminal_penalties_for_domestic_violence": "criminal_penalties_for_domestic_violence",
        "is_there_a_specialized_court_or_procedure_for_cases_of_domestic_violence": "specialized_court_for_domestic_violence",
        "can_a_woman_obtain_a_national_identity_card_in_the_same_way_as_a_married_man": "national_identity_card",
        "is_customary_law_a_valid_source_of_law_under_the_constitution": "customary_law_valid_source_of_law",
        "if_customary_law_is_a_valid_source_of_law_under_the_constitution__is_it_considered_invalid_if_it_violates_constitutional_provisions_on_nondiscrimination_or_equality": "customary_law_invalid_if_violates_constitutional_provisions",
        "is_there_legislation_that_specifically_addresses_sexual_harassment": "legislation_addresses_sexual_harassment",
        "can_a_married_woman_legally_confer_citizenship_to_her_children_in_the_same_way_as_a_married_man": "confer_citizenship_to_children_married",
        "can_an_unmarried_woman_legally_confer_citizenship_to_her_children_in_the_same_way_as_a_married_man": "confer_citizenship_to_children_unmarried",
        "can_a_married_woman_legally_confer_her_citizenship_to_a_non_national_spouse_in_the_same_way_as_a_married_man": "confer_citizenship_to_non_national_spouse_married",
        "do_married_couples_jointly_share_legal_responsibility_for_financially_maintaining_the_familys_expenses": "joint_legal_responsibility_family_expenses",
        "does_a_womans_testimony_carry_the_same_evidentiary_weight_in_court_as_a_mans_in_all_types_of_cases": "womans_testimony_carry_same_evidentiary_weight",
    }

    # Keep only the columns we need from the dictionary
    df = df[list(dict_rename.keys())]

    # Rename columns
    df = df.rename(columns=dict_rename)

    # Replace .. values with None
    df = df.replace("..", np.nan)

    # Replace "N/A "" values with None
    df = df.replace("N/A ", np.nan)

    df["legal_age_of_marriage"] = df["legal_age_of_marriage"].astype("Int64")

    cols = [col for col in df.columns if col not in ["year", "country"]]

    # Strip whitespaces
    for col in cols:
        if df[col].dtype == "category" or df[col].dtype == "object" or df[col].dtype == "string":
            df[col] = df[col].str.strip()
            # Check if columns values are contained in the list: ["Yes", "No", "nan"]
            assert (
                df[col].isin(["Yes", "No", np.nan]).all()
            ), f"Column {col} contains values other than 'Yes', 'No' or 'nan'."

    # Replace "Yes" and "No" with 1 and 0
    df = df.replace({"Yes": 1, "No": 0})

    return df


def run(dest_dir: str) -> None:
    log.info("women_business_law_additional.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("women_business_law_additional")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["women_business_law_additional"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    df = process_data(df)

    log.info("women_business_law_additional.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Create a new table with the processed data.
    tb_garden = Table(df)
    tb_garden.metadata.short_name = "women_business_law_additional"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("women_business_law_additional.end")
