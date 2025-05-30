"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("near_surface_temperature")
    tb_meadow = ds_meadow.read("near_surface_temperature")
    # Replace "Global" in the region column with "World" and "Southern hemisphere" with "Southern Hemisphere" and "Northern hemisphere" with "Northern Hemisphere"
    tb_meadow["region"] = tb_meadow["region"].replace(
        {"Global": "World", "Southern hemishphere": "Southern Hemisphere", "Northern hemisphere": "Northern Hemisphere"}
    )
    # Switch from using 1961-1990 to using 1861-1890 as our baseline to better show how temperatures have changed since pre-industrial times.
    # Calculate the adjustment factors based only on temperature_anomaly
    adjustment_factors = (
        tb_meadow[tb_meadow["year"].between(1961, 1990)].groupby("region")["temperature_anomaly"].mean()
        - tb_meadow[tb_meadow["year"].between(1861, 1890)].groupby("region")["temperature_anomaly"].mean()
    )

    columns_to_adjust = ["temperature_anomaly", "lower_limit", "upper_limit"]  # Add any other columns as needed

    # Apply the temperature_anomaly adjustment factor
    # The adjustment factor is applied uniformly to the temperature anomalies and their confidence intervals to ensure that both the central values and the associated uncertainty bounds are correctly shifted relative to the new 1861–1890 baseline.
    for region in adjustment_factors.index:
        for column in columns_to_adjust:
            tb_meadow.loc[tb_meadow["region"] == region, column] += adjustment_factors[region]

    tb_meadow = tb_meadow.format(["region", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_meadow])
    ds_garden.save()
