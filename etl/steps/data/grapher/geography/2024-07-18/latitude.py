"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("nat_earth_110")

    # Read table from garden dataset.
    tb = ds_garden["nat_earth_110"].reset_index()
    tb = tb[["name", "latitude_centroid", "absolute_latitude"]]
    tb["year"] = 2024
    tb = tb.rename(columns={"name": "country"})
    #
    # Process data.
    #
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
