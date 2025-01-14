"""Load a meadow dataset and create a garden dataset."""

import calendar

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("total_precipitation")
    tb = ds_meadow["total_precipitation"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Extract year and month as integers
    tb["year"] = tb["time"].astype(str).str[0:4].astype(int)
    tb["month"] = tb["time"].astype(str).str[5:7].astype(int)

    # Get the number of days in the given month and year
    tb["days_in_month"] = tb.apply(lambda row: calendar.monthrange(row["year"], row["month"])[1], axis=1)
    tb["days_in_month"] = tb["days_in_month"].copy_metadata(tb["total_precipitation"])

    # Use the number of days to convert to monthly totals rather than daily averages - as per info here https://confluence.ecmwf.int/pages/viewpage.action?pageId=197702790. The data is in meters so we convert to mm.
    tb["total_precipitation"] = tb["total_precipitation"] * 1000 * tb["days_in_month"]

    # Use the baseline from the Copernicus Climate Service https://climate.copernicus.eu/surface-air-temperature-january-2024
    tb_baseline = tb[(tb["year"].astype(int) > 1990) & (tb["year"].astype(int) < 2021)]
    tb_baseline = tb_baseline.groupby(["country", "month"], as_index=False)["total_precipitation"].mean()
    tb_baseline = tb_baseline.rename(columns={"total_precipitation": "mean_total_precipitation"})

    # Ensure that the reference mean DataFrame has a name for the mean column, e.g., 'mean_temp'
    tb = pr.merge(tb, tb_baseline, on=["country", "month"])

    # Calculate the anomalies (below and above the mean)
    tb["precipitation_anomaly"] = tb["total_precipitation"] - tb["mean_total_precipitation"]

    tb = tb.drop(columns=["month", "year", "mean_total_precipitation"])
    tb = tb.format(["country", "time"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
