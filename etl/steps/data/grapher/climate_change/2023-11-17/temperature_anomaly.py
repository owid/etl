"""Load a garden dataset and create a grapher dataset."""

import numpy as np
import owid.catalog.processing as pr
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("temperature_anomaly")

    # Read table from garden dataset.
    tb = ds_garden["temperature_anomaly"].reset_index()

    #
    # Process data.
    #
    # For now, select data for the World.
    tb = tb[tb["location"] == "World"].reset_index(drop=True)

    # Create a column for year and month.
    tb["year"] = tb["date"].str.split("-").str[0].astype(int)
    tb["month"] = tb["date"].str.split("-").str[1].astype(int)

    # Sort by year and month.
    tb = tb.sort_values(["year", "month"]).reset_index(drop=True)

    # Count the number of months per year and assert that there are 12 months per year (except for the current one).
    tb["month_count"] = tb.groupby("year")["month"].transform("count")
    assert tb[tb["year"] != 2023]["month_count"].unique() == 12
    # Remove the month count column.
    tb = tb.drop(columns=["month_count"])

    # Create a column for each year and plot a curve for each year.
    # df_plot = tb.pivot(index="month", columns="year", values="temperature_anomaly").reset_index()
    # px.line(df_plot, x="month", y=df_plot.columns[1:], title="Global Temperature Anomaly")

    # Combine data in decades prior to year 2000 and compute the average temperature anomaly per decade and month.
    tb_decade = tb.copy()
    tb_decade["decade"] = tb["year"] // 10 * 10
    tb_decade = tb_decade.groupby(["decade", "month"]).agg({"temperature_anomaly": "mean", "location": "first"}).reset_index().rename(columns={"decade": "year"})

    # Combine decadal and yearly data from year 2000 onwards.
    DECADE_TO_YEAR = 2010
    tb_combined = pr.merge(tb[tb["year"] >= DECADE_TO_YEAR], tb_decade[tb_decade["year"]<DECADE_TO_YEAR], how="outer").sort_values(["year", "month"]).reset_index(drop=True).drop(columns=["date"])

    # Create a column for each year and plot a curve for each year.
    # df_plot = df_combined.pivot(index="month", columns="year", values="temperature_anomaly").reset_index()
    # px.line(df_plot, x="month", y=df_plot.columns[1:], title="Global Temperature Anomaly")

    # Create an array of the cumulative number of days in each month (30 days per month, for simplicity).
    tb_plot = tb_combined.pivot(index=["location", "month"], columns="year", values="temperature_anomaly").reset_index()
    tb_plot["days"] = np.cumsum(np.repeat(30, 12))

    # Column names must be strings, and start with the word "year_".
    tb_plot = tb_plot.rename(columns={column: f"year_{column}" if isinstance(column, int) else column for column in tb_plot.columns})

    # The column "year" needs to be the number of days since a specific date.
    tb_plot = tb_plot.rename(columns={"days": "year"})
    tb_plot = tb_plot.drop(columns=["month"])
    tb_plot = tb_plot.rename(columns={"location": "country"}).set_index(["country", "year"], verify_integrity=True)

    # Add minimal required metadata.
    for column in tb_plot.columns:
        tb_plot[column].metadata.title = f"Temperature anomaly in {column.replace('_', ' ')}"
        tb_plot[column].metadata.unit = "°C"
        tb_plot[column].metadata.display = {"yearIsDay": True, "zeroDay": "2023-01-01", "name": column.split("_")[1]}

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_plot], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
