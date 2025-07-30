"""Load a meadow dataset and create a garden dataset."""
# find out more about the data fields at https://clinicaltrials.gov/data-api/about-api/study-data-structure

import time
from collections import Counter

import pandas as pd
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

# import logger
logger = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INTERVENTIONS = [
    "DEVICE",
    "PROCEDURE",  # e.g. surgery
    "RADIATION",
    "OTHER",
    "DIETARY_SUPPLEMENT",
    "DIAGNOSTIC_TEST",
    "BEHAVIORAL",
    "BIOLOGICAL",  # e.g. vaccine
    "DRUG",
    "GENETIC",  # e.g. gene therapy
    "COMBINATION_PRODUCT",  # e.g. drug + device
]


def extract_allocation(design_string: str):
    """
    Extract the allocation type from the study design string.
    """
    if pd.isna(design_string):
        return pd.NA
    if "Allocation:" not in design_string:
        return pd.NA
    if "Allocation: RANDOMIZED" in design_string:
        return "Randomized"
    elif "Allocation: NON_RANDOMIZED" in design_string:
        return "Non-Randomized"
    elif "Allocation: NA" in design_string:
        return "Not Applicable"
    else:
        return pd.NA


def extract_intervention_type(interventions_string: str):
    """
    Extract the intervention type from the interventions string.
    """
    if pd.isna(interventions_string):
        return pd.NA
    if "Intervention Type:" not in interventions_string:
        return pd.NA
    elif "Intervention Model: PARALLEL" in interventions_string:
        return "Parallel"
    elif "Intervention Model: SINGLE_GROUP" in interventions_string:
        return "Single Group"
    elif "Intervention Model: SEQUENTIAL" in interventions_string:
        return "Sequential"
    elif "Intervention Model: CROSSOVER" in interventions_string:
        return "Crossover"
    elif "Intervention Model: FACTORIAL" in interventions_string:
        return "Factorial"
    else:
        return pd.NA


def extract_primary_purpose(purpose_string: str):
    """
    Extract the primary purpose from the study design string.
    """
    if pd.isna(purpose_string):
        return pd.NA
    if "Primary Purpose:" not in purpose_string:
        return pd.NA
    if "Primary Purpose: BASIC_SCIENCE" in purpose_string:
        return "Basic Science"
    elif "Primary Purpose: DEVICE_FEASIBILITY" in purpose_string:
        return "Device Feasibility"
    elif "Primary Purpose: DIAGNOSTIC" in purpose_string:
        return "Diagnostic"
    elif "Primary Purpose: ECT" in purpose_string:
        return "ECT"
    elif "Primary Purpose: HEALTH_SERVICES_RESEARCH" in purpose_string:
        return "Health Services Research"
    elif "Primary Purpose: OTHER" in purpose_string:
        return "Other"
    elif "Primary Purpose: PREVENTION" in purpose_string:
        return "Prevention"
    elif "Primary Purpose: SCREENING" in purpose_string:
        return "Screening"
    elif "Primary Purpose: SUPPORTIVE_CARE" in purpose_string:
        return "Supportive Care"
    elif "Primary Purpose: TREATMENT" in purpose_string:
        return "Treatment"
    else:
        return pd.NA


def add_intervention_types(tb):
    """
    Add a column for intervention types based on the 'Interventions' column.
    """
    for intervention in INTERVENTIONS:
        tb[intervention] = tb["Interventions"].str.contains(intervention, case=True, na=False).astype("boolean")

    return tb


def date_col_to_year(df: pd.DataFrame, col_name: str) -> pd.Series:
    """
    Convert a date column to a year column.
    """
    return pd.to_datetime(df[col_name], errors="raise", format="mixed", yearfirst=True).dt.year.astype("Int64")


def get_most_common(lst):
    """
    Get the most common element in a list.
    If there are multiple elements with the same frequency, it returns the first one.
    """
    if not lst:
        return "Location not specified"
    count = Counter(lst)
    most_common = count.most_common(1)[0][0].strip()
    return most_common


def extract_primary_location(loc_string: str) -> str:
    """
    Extract the primary location from a location string.

    If multiple locations are provided, it returns the most common one. If multiple locations appear equally often, it returns the first one.
    If no location is specified, it returns "Location not specified".
    """
    if pd.isna(loc_string):
        return "Location not specified"
    if "|" in loc_string:
        locations = loc_string.split("|")
        locations = [loc.split(",")[-1].strip() for loc in locations]
        return get_most_common(locations)
    else:
        return loc_string.split(",")[-1].strip()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    time_stamp = time.time()
    snap = paths.load_snapshot("clinical_trials.csv")

    # Load data from snapshot.
    tb = snap.read()

    logger.info(f"Loaded snapshot with {len(tb)} rows in {time.time() - time_stamp:.2f} seconds.")

    tb = tb.drop(
        columns=[
            "Acronym",
            "Brief Summary",
            "Collaborators",
            "Other IDs",
            "Last Update Posted",
            "Study Documents",
            "Study URL",
        ]
    )

    # get years from start and end dates
    tb["start_year"] = date_col_to_year(tb, "Start Date")
    tb["completion_year"] = date_col_to_year(tb, "Completion Date")
    tb["primary_completion_year"] = date_col_to_year(tb, "Primary Completion Date")
    tb["first_posted_year"] = date_col_to_year(tb, "First Posted")
    tb["results_first_posted_year"] = date_col_to_year(tb, "Results First Posted")

    # get length of study in days
    tb["study_length_days"] = (
        pd.to_datetime(tb["Completion Date"], errors="coerce", format="mixed", yearfirst=True)
        - pd.to_datetime(tb["Start Date"], errors="coerce", format="mixed", yearfirst=True)
    ).dt.days

    # change appriopriate columns to categorical (to save memory & speed up processing)
    tb["Study Status"] = tb["Study Status"].astype("category")
    tb["Sex"] = tb["Sex"].astype("category")
    tb["Age"] = tb["Age"].astype("category")
    tb["Phases"] = tb["Phases"].astype("category")
    tb["Funder Type"] = tb["Funder Type"].astype("category")
    tb["Study Type"] = tb["Study Type"].astype("category")
    tb["Study Results"] = tb["Study Results"].astype("category")

    # Extract primary location from locations and set as categorical
    tb["primary_location"] = tb["Locations"].apply(extract_primary_location)
    tb["primary_location"] = tb["primary_location"].astype("category")

    # extract study design information
    # masking is not used as it is not super informative
    # observational model and time perspective are not filled in
    tb["primary_purpose"] = tb["Study Design"].apply(extract_primary_purpose)
    tb["primary_purpose"] = tb["primary_purpose"].astype("category")
    tb["intervention_type"] = tb["Interventions"].apply(extract_intervention_type)
    tb["intervention_type"] = tb["intervention_type"].astype("category")
    tb["allocation"] = tb["Study Design"].apply(extract_allocation)
    tb["allocation"] = tb["allocation"].astype("category")

    # Add intervention types as boolean columns
    tb = add_intervention_types(tb)

    tb = tb.drop(
        columns=[
            "Start Date",
            "Primary Completion Date",
            "Completion Date",
            "First Posted",
            "Results First Posted",
            "Locations",
            "Study Design",
            "Interventions",
        ]
    )
    # Process data.
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="primary_location",
    )

    # get all studies by completion year and country
    tb_trials = (
        tb.groupby(["primary_location", "completion_year"]).size().reset_index(name="n_studies_country")
    ).copy_metadata(tb)
    tb_trials["n_studies_country"].m.origins = tb["Phases"].m.origins

    # studies by completion year and sponsor type
    tb_sponsor = (
        tb.groupby(["Funder Type", "completion_year"]).size().reset_index(name="n_studies_sponsor")
    ).copy_metadata(tb)
    tb_sponsor["n_studies_sponsor"].m.origins = tb["Funder Type"].m.origins

    # get sum of studies by completion year by intervention type
    tb_interventions = tb.copy()
    tb_interventions[INTERVENTIONS] = tb_interventions[INTERVENTIONS].astype("Int64")
    intvt_cols = ["completion_year"] + INTERVENTIONS
    tb_interventions = tb_interventions[intvt_cols].groupby("completion_year").sum().reset_index().copy_metadata(tb)
    tb_interventions[INTERVENTIONS].m.origins = tb["Phases"].m.origins

    # get sum of studies by completion year by study type
    tb_study_type = (
        tb.groupby(["Study Type", "completion_year"]).size().reset_index(name="n_studies_type")
    ).copy_metadata(tb)
    tb_study_type["n_studies_type"].m.origins = tb["Phases"].m.origins

    # get sum of studies by completion year by primary purpose
    tb_purpose = (
        tb.groupby(["primary_purpose", "completion_year"]).size().reset_index(name="n_studies_purpose")
    ).copy_metadata(tb)
    tb_purpose["n_studies_purpose"].m.origins = tb["Phases"].m.origins

    # get sum of studies by completion year by status
    tb_status = tb[tb["Study Type"] != "Expanded Access"]
    tb_status = (
        tb_status.groupby(["Study Status", "start_year"]).size().reset_index(name="n_studies_status")
    ).copy_metadata(tb)
    tb_status["n_studies_status"].m.origins = tb["Phases"].m.origins

    # get sum of studies by completion year and whether they have results
    tb_results = (
        tb.groupby(["Study Results", "start_year"]).size().reset_index(name="n_studies_results")
    ).copy_metadata(tb)
    tb_results["n_studies_results"].m.origins = tb["Phases"].m.origins

    # get average study length by phase and completion year
    tb_length = (
        tb.groupby(["completion_year", "Phases"])["study_length_days"].mean().reset_index(name="avg_study_length_days")
    ).copy_metadata(tb)
    tb_length["avg_study_length_days"].m.origins = tb["Phases"].m.origins

    # Improve table format.
    tb_trials, tb_sponsor, tb_interventions, tb_study_type, tb_purpose, tb_status, tb_results, tb_length = (
        format_tables(
            tb_trials, tb_sponsor, tb_interventions, tb_study_type, tb_purpose, tb_status, tb_results, tb_length
        )
    )

    # TODO: add replacements for entities
    # {'EARLY_PHASE1', 'PHASE1|PHASE2', 'PHASE4', 'PHASE2', 'PHASE1', 'PHASE3', 'PHASE2|PHASE3'}
    # {'INDIV', 'NIH', 'OTHER', 'FED', 'AMBIG', 'INDUSTRY', 'OTHER_GOV', 'UNKNOWN', 'NETWORK'}
    # {'ENROLLING_BY_INVITATION', 'RECRUITING', 'TERMINATED', 'NOT_YET_RECRUITING', 'COMPLETED', 'WITHDRAWN', 'ACTIVE_NOT_RECRUITING', 'AVAILABLE', 'NO_LONGER_AVAILABLE', 'SUSPENDED', 'APPROVED_FOR_MARKETING', 'TEMPORARILY_NOT_AVAILABLE'}
    # {'OBSERVATIONAL', 'INTERVENTIONAL', 'EXPANDED_ACCESS'}

    tables_ls = [
        tb_trials,
        tb_sponsor,
        tb_interventions,
        tb_study_type,
        tb_purpose,
        tb_status,
        tb_results,
        tb_length,
    ]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables_ls, default_metadata=snap.metadata)

    # Save garden dataset.
    ds_garden.save()


def format_tables(tb_trials, tb_sponsor, tb_interventions, tb_study_type, tb_purpose, tb_status, tb_results, tb_length):
    # rename the columns in each table to "year" and "country" so they can be used in grapher
    tb_trials = tb_trials.rename(columns={"completion_year": "year", "primary_location": "country"}, errors="raise")
    tb_sponsor = tb_sponsor.rename(columns={"completion_year": "year", "Funder Type": "country"}, errors="raise")
    tb_interventions = tb_interventions.rename(columns={"completion_year": "year"}, errors="raise")
    tb_study_type = tb_study_type.rename(columns={"completion_year": "year", "Study Type": "country"}, errors="raise")
    tb_purpose = tb_purpose.rename(columns={"completion_year": "year", "primary_purpose": "country"}, errors="raise")
    tb_status = tb_status.rename(columns={"start_year": "year", "Study Status": "country"}, errors="raise")
    tb_results = tb_results.rename(columns={"start_year": "year", "Study Results": "country"}, errors="raise")
    tb_length = tb_length.rename(columns={"completion_year": "year", "Phases": "country"}, errors="raise")

    # set the index to year and country for each table
    tb_trials = tb_trials.format(["year", "country"], short_name="trials_per_year")
    tb_sponsor = tb_sponsor.format(["year", "country"], short_name="sponsor_per_year")
    tb_interventions = tb_interventions.format(["year"], short_name="interventions_per_year")
    tb_study_type = tb_study_type.format(["year", "country"], short_name="study_type_per_year")
    tb_purpose = tb_purpose.format(["year", "country"], short_name="purpose_per_year")
    tb_status = tb_status.format(["year", "country"], short_name="status_per_year")
    tb_results = tb_results.format(["year", "country"], short_name="results_per_year")
    tb_length = tb_length.format(["year", "country"], short_name="length_per_year")

    return (
        tb_trials,
        tb_sponsor,
        tb_interventions,
        tb_study_type,
        tb_purpose,
        tb_status,
        tb_results,
        tb_length,
    )
