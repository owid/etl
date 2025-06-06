"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("surface_temperature")
    tb = ds_meadow["surface_temperature"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]
    # Use the baseline from the Copernicus Climate Service https://climate.copernicus.eu/surface-air-temperature-january-2024
    tb_baseline = tb[(tb["year"].astype(int) > 1990) & (tb["year"].astype(int) < 2021)]
    tb_baseline = tb_baseline.groupby(["country", "month"], as_index=False)["temperature_2m"].mean()
    tb_baseline = tb_baseline.rename(columns={"temperature_2m": "mean_temp"})

    # Ensure that the reference mean DataFrame has a name for the mean column, e.g., 'mean_temp'
    merged_df = pr.merge(tb, tb_baseline, on=["country", "month"])

    # Calculate the anomalies (below and above the mean)
    merged_df["temperature_anomaly"] = merged_df["temperature_2m"] - merged_df["mean_temp"]
    merged_df = merged_df.drop(columns=["mean_temp"])

    merged_df["anomaly_below_0"] = merged_df["temperature_anomaly"].copy()
    merged_df.loc[merged_df["anomaly_below_0"] >= 0, "anomaly_below_0"] = None

    merged_df["anomaly_above_0"] = merged_df["temperature_anomaly"].copy()
    merged_df.loc[merged_df["anomaly_above_0"] <= 0, "anomaly_above_0"] = None
    merged_df = merged_df.drop(columns=["month", "year"])
    merged_df = merged_df.set_index(["country", "time"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[merged_df], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
