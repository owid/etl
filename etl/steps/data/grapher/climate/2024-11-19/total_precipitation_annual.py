"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Year with incomplete data
INCOMPLETE_YEAR = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("total_precipitation")
    tb = ds_garden["total_precipitation"].reset_index()

    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["time"].astype(str).str[0:4]

    # Group by year and calculate the mean of the specified columns
    tb = (
        tb.groupby(["year", "country"])
        .agg({"total_precipitation": "sum", "precipitation_anomaly": "sum"})
        .reset_index()
    )

    # Remove rows where the year is 2024 as it's incomplete
    tb["year"] = tb["year"].astype(int)
    tb = tb[tb["year"] != INCOMPLETE_YEAR]

    tb = tb.format(["year", "country"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    ds_grapher.save()
