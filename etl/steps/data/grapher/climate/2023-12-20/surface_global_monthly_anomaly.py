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
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]

    #
    # Process data.
    #
    # Filter to include only global data
    tb_global = tb[tb["country"] == "World"]

    # Group the data by year and calculate the mean temperature anomaly for each year
    average_anomaly = tb_global.groupby("year")["temperature_anomaly"].mean().reset_index()

    # Create a new column for temperature anomalies below 0
    average_anomaly["Below the average"] = average_anomaly["temperature_anomaly"].copy()
    # Set the values in the 'annual_below_0' column to None for rows where the temperature anomaly is not below 0
    average_anomaly.loc[average_anomaly["Below the average"] >= 0, "Below the average"] = None

    # Create a new column for temperature anomalies above 0
    average_anomaly["Above the average"] = average_anomaly["temperature_anomaly"].copy()
    # Set the values in the 'annual_above_0' column to None for rows where the temperature anomaly is not above 0
    average_anomaly.loc[average_anomaly["Above the average"] <= 0, "Above the average"] = None

    # Drop the original 'temperature_anomaly' column
    average_anomaly = average_anomaly.drop(columns=["temperature_anomaly"])

    # Reshape so that the 'annual_below_0' and 'annual_above_0' columns are stacked into a single column
    average_anomaly = average_anomaly.melt(id_vars="year", var_name="anomaly_type", value_name="Temperature Anomaly")

    # Copy the metadata from the 'temperature_anomaly' column in the original DataFrame to the 'anomaly_type' column in the reshaped DataFrame
    average_anomaly["anomaly_type"] = average_anomaly["anomaly_type"].copy_metadata(tb["temperature_anomaly"])

    # Set a short description for the 'anomaly_type' column
    average_anomaly[
        "anomaly_type"
    ].metadata.description_short = (
        "The deviation of a specific year's average surface temperature from the 1991-2020 mean, in degrees Celsius."
    )

    # Rename the 'anomaly_type' column to 'country'
    average_anomaly = average_anomaly.rename(columns={"anomaly_type": "country"})

    # Format the 'year' and 'country' columns
    average_anomaly = average_anomaly.format(["year", "country"])

    # Set a short name for the DataFrame
    average_anomaly.metadata.short_name = paths.short_name

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[average_anomaly], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Global monthly temperature anomalies"
    ds_grapher.save()
