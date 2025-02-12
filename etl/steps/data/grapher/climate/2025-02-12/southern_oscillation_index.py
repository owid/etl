"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("southern_oscillation_index")

    # Read table from garden dataset.
    tb = ds_garden.read("southern_oscillation_index")

    tb = tb.drop(columns={"date", "soi"})
    # Drop rows with NaN values
    tb = tb.dropna()

    ds_anomalies = paths.load_dataset("surface_temperature")
    tb_anomalies = ds_anomalies.read("surface_temperature")

    tb_anomalies = format_surface_temerpature_data(tb_anomalies)

    tb = pr.merge(tb, tb_anomalies, on=["country", "year"], how="left")

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def format_surface_temerpature_data(tb):
    #
    # Process data.
    #
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]

    # Filter to include only global data
    tb_global = tb[tb["country"] == "World"]

    # Group the data by year and calculate the mean temperature anomaly for each year
    tb = tb_global.groupby("year")["temperature_anomaly"].mean().reset_index()
    tb["country"] = "World"
    tb["year"] = tb["year"].astype(int)

    return tb
