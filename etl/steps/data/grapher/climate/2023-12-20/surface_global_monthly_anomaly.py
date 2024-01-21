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
    tb_global = tb[tb["country"] == "World"]
    tb_anomalies = tb_global[["year", "month", "anomaly_below_0", "anomaly_above_0"]]
    tb_anomalies = tb_anomalies.rename(
        columns={"anomaly_below_0": "Below the average", "anomaly_above_0": "Above the average"}
    )

    tb_melted = tb_anomalies.melt(id_vars=["year", "month"], value_vars=["Below the average", "Above the average"])
    tb_melted.rename(columns={"variable": "country"}, inplace=True)
    month_map = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }
    tb_melted["month"] = tb_melted["month"].map(month_map)

    tb_melted = tb_melted.rename(columns={"value": "temperature_anomaly"})
    tb_pivot = tb_melted.pivot(index=["country", "year"], columns="month", values="temperature_anomaly").reset_index()
    tb_pivot = tb_pivot.set_index(["country", "year"])

    # Create annual temperature anomalies
    tb_pivot["annual"] = tb_pivot.mean(axis=1)
    tb_pivot["annual"] = tb_pivot["annual"].copy_metadata(tb["temperature_anomaly"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Global monthly temperature anomalies since 1950"
    ds_grapher.save()
