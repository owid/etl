"""Load a meadow dataset and create a garden dataset."""

from itertools import product

import pandas as pd
from guinea_worm_validation import (
    _validate_basic_data_integrity,
    _validate_data_processing,
    _validate_geographic_coverage,
    _validate_guinea_worm_specific_logic,
    _validate_temporal_consistency,
)
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()

CASES_2025 = {
    "Angola": 0,
    "Cameroon": 0,
    "Central African Republic": 0,
    "Chad": 4,
    "Ethiopia": 4,
    "Mali": 0,
    "South Sudan": 2,
    "World": 10,
}

CURRENT_YEAR = 2025


def run() -> None:
    #
    # Load inputs.
    #

    # Load fasttrack snapshot (includes data for guinea worm cases up to 2024).
    snap = paths.load_snapshot("guinea_worm_cases.csv")
    tb = snap.read().astype({"year": int})

    # Load certification dataset (includes data for guinea worm certification up to 2024).
    ds_garden = paths.load_dataset("guinea_worm_certification")
    # Read certification table
    tb_cert = ds_garden["guinea_worm"].reset_index()

    # add case data for new year to fast-track dataset
    tb_cases = add_new_cases_tb(tb, CASES_2025)
    tb_cases = add_missing_years(tb_cases)

    #
    # Process data.
    #
    # Harmonize country names.
    tb_cases = paths.regions.harmonize_names(tb=tb_cases)
    tb_cert = paths.regions.harmonize_names(tb=tb_cert)

    # remove leading spaces from "year_certified" column and cast as string
    tb_cert["year_certified"] = tb_cert["year_certified"].str.strip()

    # Update certification status for current year (if there are no changes in certification status for any country in the current year).
    tb_cert = update_certification_status_no_changes(tb_cert, current_year=CURRENT_YEAR)

    # Merge certification status into main table
    tb = pr.merge(tb_cases, tb_cert, on=["country", "year"], how="outer")

    # fill N/As with 0 for case counts
    tb["guinea_worm_reported_cases"] = tb["guinea_worm_reported_cases"].fillna(0)

    log.info("Validating guinea worm data")
    _validate_basic_data_integrity(tb)
    _validate_guinea_worm_specific_logic(tb)
    _validate_temporal_consistency(tb)
    _validate_geographic_coverage(tb)
    _validate_data_processing(tb, tb_cases, CURRENT_YEAR)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_new_cases_tb(tb: Table, cases_dict: dict) -> Table:
    """
    Add new case data for the current year to the fast-track dataset.
    """
    new_rows = []
    for country, cases in cases_dict.items():
        new_rows.append({"country": country, "year": CURRENT_YEAR, "guinea_worm_reported_cases": cases})

    tb = pr.concat([tb, Table(new_rows)], ignore_index=True)

    return tb


def add_missing_years(tb: Table) -> Table:
    """
    Add full spectrum of year-country combinations to fast-track dataset so we have zeros where there is missing data
    """
    years = tb["year"].unique()
    countries = tb["country"].unique()
    comb_df = Table(pd.DataFrame(list(product(countries, years)), columns=["country", "year"]))

    tb = Table(pr.merge(tb, comb_df, on=["country", "year"], how="outer"), short_name=paths.short_name)

    print(f"Added missing year-country combinations. Number of rows increased from {len(tb)} to {len(tb)}.")

    return tb


def update_certification_status_no_changes(tb_cert: Table, current_year=CURRENT_YEAR) -> Table:
    """
    Update certification status if there are no changes in certification status for any country in the current year.
    """
    # Get certification status for the most recent year in the dataset
    most_recent_year = tb_cert["year"].max()
    most_recent_cert_status = tb_cert[tb_cert["year"] == most_recent_year][
        ["country", "certification_status", "year_certified"]
    ]

    # Create a new table for the current year with the same certification status as the most recent year
    new_cert_status = most_recent_cert_status.copy()
    new_cert_status["year"] = current_year

    # Append the new certification status to the original table
    tb_cert = pr.concat([tb_cert, new_cert_status], ignore_index=True)

    return tb_cert
