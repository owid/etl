"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARIABLE_NAMES = {
    "WPU801103": "ppi_new_office_construction",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("us_ppi_construction.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Translate series_id to human-readable names
    tb["series_id"] = tb["series_id"].replace(VARIABLE_NAMES)

    # Add country
    tb["country"] = "United States"

    # Create date column for monthly data
    tb["date"] = pd.to_datetime(
        tb.apply(
            lambda row: f"{row['year']}-{row['month']:02d}-01" if pd.notna(row["month"]) else f"{row['year']}-01-01",
            axis=1,
        )
    )

    # Drop unnecessary columns
    tb = tb.drop(columns=["period", "period_name"])

    # Pivot to wide format
    tb = tb.pivot(columns="series_id", values="value", index=["country", "year", "month", "date"]).reset_index()

    # Format table
    tb_garden = tb.format(["country", "year", "month"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_garden], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
