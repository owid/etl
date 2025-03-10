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
    tb = ds_meadow.read("sea_surface_temperature")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Switch from using 1961-1990 to using 1861-1890 as our baseline to better show how temperatures have changed since pre-industrial times.
    # Calculate the adjustment factors based only on temperature_anomaly
    adjustment_factors = (
        tb[tb["year"].between(1961, 1990)].groupby("location")["sea_temperature_anomaly"].mean()
        - tb[tb["year"].between(1861, 1890)].groupby("location")["sea_temperature_anomaly"].mean()
    )
    # Apply the temperature_anomaly adjustment factor
    # The adjustment factor is applied uniformly to the temperature anomalies and their confidence intervals to ensure that both the central values and the associated uncertainty bounds are correctly shifted relative to the new 1861–1890 baseline.
    columns_to_adjust = [
        "sea_temperature_anomaly",
        "sea_temperature_anomaly_low",
        "sea_temperature_anomaly_high",
    ]

    # Apply the temperature_anomaly adjustment factor
    # The adjustment factor is applied uniformly to the temperature anomalies and their confidence intervals to ensure that both the central values and the associated uncertainty bounds are correctly shifted relative to the new 1861–1890 baseline.
    for region in adjustment_factors.index:
        for column in columns_to_adjust:
            tb.loc[tb["location"] == region, column] += adjustment_factors[region]

    # Create a date column (assume the middle of the month for each monthly data point).
    tb["date"] = tb["year"].astype(str) + "-" + tb["month"].astype(str).str.zfill(2) + "-15"
    # Remove unnecessary columns.

    # Compute annual averages
    tb_annual = tb.groupby(["year", "location"], as_index=False).agg(
        {
            "sea_temperature_anomaly": "mean",
            "sea_temperature_anomaly_low": "mean",
            "sea_temperature_anomaly_high": "mean",
        }
    )
    tb_annual = tb_annual.rename(
        columns={
            "sea_temperature_anomaly": "sea_temperature_anomaly_annual",
            "sea_temperature_anomaly_low": "sea_temperature_anomaly_annual_low",
            "sea_temperature_anomaly_high": "sea_temperature_anomaly_annual_high",
        },
        errors="raise",
    )

    # Set an appropriate index and sort conveniently.
    tb_annual = tb_annual.format(["location", "year"], short_name="sea_surface_temperature_annual")

    tb = tb.drop(columns=["year", "month"], errors="raise")

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_annual], check_variables_metadata=True)
    ds_garden.save()
