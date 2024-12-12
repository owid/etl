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
    tb_anomalies = tb[["year", "country", "month", "temperature_anomaly"]]

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
    tb_anomalies["month"] = tb_anomalies["month"].map(month_map)

    tb_pivot = tb_anomalies.pivot(
        index=["country", "year"], columns="month", values="temperature_anomaly"
    ).reset_index()

    tb_pivot = tb_pivot.format(["country", "year"])

    # Create annual temperature anomalies
    tb_pivot["annual"] = tb_pivot.mean(axis=1)
    tb_pivot["annual"] = tb_pivot["annual"].copy_metadata(tb["temperature_anomaly"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.save()
