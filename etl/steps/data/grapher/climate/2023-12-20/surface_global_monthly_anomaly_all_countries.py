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
    tb_global = tb[tb["country"] == "World"].copy()
    tb_global = tb_global[["year", "month", "temperature_anomaly"]]

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
    tb_global["month"] = tb_global["month"].map(month_map)

    # Name month column "country" foro grapher purposes
    tb_global.rename(columns={"month": "country"}, inplace=True)
    tb_global = tb_global.format(["year", "country"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_global], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Global monthly temperature anomalies for all countries"
    ds_grapher.save()
