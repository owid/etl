"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from dateutil.relativedelta import relativedelta
from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("xm_who.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive(
        filename="2023-05-19_covid-19_gem/WHO_COVID_Excess_Deaths_EstimatesByCountry.xlsx",
        sheet_name="Deaths by year and month",
        skiprows=12,
    )

    tb_income = snap.read_in_archive(
        filename="2023-05-19_covid-19_gem/WHO_COVID_Excess_Deaths_EstimatesByIncome.xlsx",
        sheet_name="Deaths by year and month",
        skiprows=11,
    )

    tb_region = snap.read_in_archive(
        filename="2023-05-19_covid-19_gem/WHO_COVID_Excess_Deaths_EstimatesByRegion.xlsx",
        sheet_name="Deaths by year and month",
        skiprows=11,
    )

    #
    # Process data.
    #
    # Remove NaNs
    tb = process_table(tb)
    tb_income = process_table(tb_income)
    tb_region = process_table(tb_region)
    tb_income = tb_income.rename(columns={"income": "Country"})
    tb_region = tb_region.rename(columns={"location": "Country"})

    # Combine
    tb = concat([tb, tb_income, tb_region], ignore_index=True)

    # Format
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_table(tb: Table):
    assert tb["year"].isna().sum() == 2, "More NaNs than expected in 'year' column (2)."
    tb = tb.dropna(subset=["year"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # Generate date
    def to_int(s):
        return s.astype(int).astype("string")

    tb["date"] = to_int(tb["year"]) + "-" + to_int(tb["month"]) + "-1"
    tb["date"] = pd.to_datetime(tb["date"])
    tb["date"] = tb["date"].apply(lambda x: x + relativedelta(months=1) - relativedelta(days=1))
    tb = tb.drop(columns=["year", "month"])

    return tb
