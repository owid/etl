"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Columns to select from data, and how to rename them.
COLUMNS = {
    "year": "year",
    "month": "month",
    "location": "location",
    "anomaly": "sea_temperature_anomaly",
    "lower_bound_95pct_bias_uncertainty_range": "sea_temperature_anomaly_low",
    "upper_bound_95pct_bias_uncertainty_range": "sea_temperature_anomaly_high",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("sea_surface_temperature")
    tb = ds_meadow["sea_surface_temperature"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Create a date column (assume the middle of the month for each monthly data point).
    tb["date"] = tb["year"].astype(str) + "-" + tb["month"].astype(str).str.zfill(2) + "-15"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["year", "month"], errors="raise")

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
