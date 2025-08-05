"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tuberculosis")

    # Read table from meadow dataset.
    tb = ds_meadow.read("tuberculosis")
    # Drop unwanted columns.
    tb = tb.drop(
        columns=[
            "tb_cases_pct_change_no",
            "tb_cases_pct_change_rate",
            "tb_deaths_pct_change_no",
            "tb_deaths_pct_change_rate",
        ]
    )
    tb = tb.replace("--", pd.NA)  # Replace '--' with NaN for better handling of missing values.
    tb["tb_cases_no"] = (
        tb["tb_cases_no"].str.replace(",", "", regex=False).astype("Int64")
    )  # Remove commas from numbers for better handling of numeric values.
    tb["tb_deaths_no"] = tb["tb_deaths_no"].str.replace(",", "", regex=False).astype("Int64")
    # Case data after 1974 are not comparable to prior years due to changes in the surveillance case definition that became effective in 1975.
    # So we need to split the table into two parts.
    tb_cases_before_1975 = tb[["country", "year", "tb_cases_no", "tb_cases_rate"]][tb["year"] <= 1974].copy()
    tb_cases_after_1975 = tb[["country", "year", "tb_cases_no", "tb_cases_rate"]][tb["year"] > 1974].copy()

    # The large decrease in death rate in 1979 occurred because late effects of tuberculosis (e.g., bronchiectasis or fibrosis) and pleurisy with effusion
    # (without mention of cause) are no longer included in tuberculosis deaths.
    # So we need to split the table into two parts.
    tb_deaths_before_1979 = tb[["country", "year", "tb_deaths_no", "tb_deaths_rate"]][tb["year"] <= 1978].copy()
    tb_deaths_after_1979 = tb[["country", "year", "tb_deaths_no", "tb_deaths_rate"]][tb["year"] > 1978].copy()

    # Improve table format.
    tb_cases_before_1975 = tb_cases_before_1975.format(["country", "year"], short_name="cases_before_1975")
    tb_cases_after_1975 = tb_cases_after_1975.format(["country", "year"], short_name="cases_after_1975")
    tb_deaths_before_1979 = tb_deaths_before_1979.format(["country", "year"], short_name="deaths_before_1979")
    tb_deaths_after_1979 = tb_deaths_after_1979.format(["country", "year"], short_name="deaths_after_1979")
    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_cases_before_1975, tb_cases_after_1975, tb_deaths_before_1979, tb_deaths_after_1979],
        default_metadata=ds_meadow.metadata,
    )

    # Save garden dataset.
    ds_garden.save()
