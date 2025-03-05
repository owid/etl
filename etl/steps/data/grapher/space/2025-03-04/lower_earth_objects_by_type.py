"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("space_track")

    # Read table from garden dataset.
    tb = ds_garden.read("lower_earth_objects_by_type")

    #
    # Process data.
    #
    # Adapt column names to grapher.
    tb = tb.rename(columns={"object_type": "country"}, errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
