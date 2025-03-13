"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Year with incomplete data
INCOMPLETE_YEAR = 2025


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("snowfall")
    tb = ds_garden.read("snowfall")
    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["time"].astype(str).str[0:4]

    # Group by year and calculate the mean of the specified columns
    tb = tb.groupby(["year", "country"]).agg({"snow_cover": "mean"}).reset_index()

    # Remove rows where the year is 2024 as it's incomplete
    tb["year"] = tb["year"].astype(int)
    tb = tb[tb["year"] != INCOMPLETE_YEAR]
    print(tb)

    tb = tb.format(["year", "country"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    ds_grapher.save()
