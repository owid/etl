"""Load a meadow dataset and create a garden dataset."""

import logging
from collections import Counter

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# import logger
logger = logging.getLogger(__name__)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INTERVENTIONS = [
    "PREVENTION",
    "DEVICE",
    "PROCEDURE",
    "RADIATION",
    "OTHER",
    "DIETARY_SUPPLEMENT",
    "DIAGNOSTIC_TEST",
    "BEHAVIORAL",
    "BIOLOGICAL",
    "DRUG",
    "BONE VOID FILLER",
    "GENETIC",
    "COMBINATION_PRODUCT",
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
    data = Counter(lst)
    return data.most_common(1)[0][0]


def extract_primary_location(loc_string: str) -> str:
    """
    Extract the primary location from a location string.

    If multiple locations are provided, it returns the most common one.
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
    snap = paths.load_snapshot("clinical_trials.csv")

    # Load data from snapshot.
    tb = snap.read()

    logger.info(f"Loaded snapshot with {len(tb)} rows.")

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
    tb_trials_per_year = (
        tb.groupby(["primary_location", "completion_year"], observed=True).size().reset_index(name="n_studies_country")
    )

    # studies by completion year and sponsor type
    tb_sponsor_per_year = (
        tb.groupby(["Funder Type", "completion_year"], observed=True).size().reset_index(name="n_studies_sponsor")
    )

    # get sum of studies by completion year by intervention type
    tb_intervention_per_year = tb.copy()
    tb_intervention_per_year[INTERVENTIONS] = tb_intervention_per_year[INTERVENTIONS].astype("Int64")
    intvt_cols = ["completion_year"] + INTERVENTIONS
    tb_intervention_per_year = (
        tb_intervention_per_year[intvt_cols].groupby("completion_year").sum().reset_index()
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    tables = [
        tb,
        tb_trials_per_year,
    ]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_garden.save()
