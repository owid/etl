"""Load a garden dataset and create a grapher dataset."""



from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature")
    tb = ds_garden["surface_temperature"].reset_index()

    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["time"].astype(str).str[0:4]
    # Group by year and calculate the mean of the specified columns
    tb_annual_average = (
        tb.groupby(["year", "country"])
        .agg(
            {
                "temperature_2m": "mean",
                "temperature_anomaly": "mean",
                "anomaly_below_0": "mean",
                "anomaly_above_0": "mean",
            }
        )
        .reset_index()
    )

    tb_annual_average = tb_annual_average.set_index(["year", "country"])
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_annual_average], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Annual surface temperatures and anomalies since 1950 by country"

    ds_grapher.save()
