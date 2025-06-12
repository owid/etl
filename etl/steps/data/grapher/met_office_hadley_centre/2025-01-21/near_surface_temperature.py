"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("near_surface_temperature")
    tb_garden = ds_garden["near_surface_temperature"].reset_index()

    #
    # Process data.
    #
    # For compatibility with grapher, change the name of "region" column to "country".
    tb_garden = tb_garden.rename(columns={"region": "country"})

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb_garden], check_variables_metadata=True)
    ds_grapher.save()
