"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("nuclear_weapons_treaties")
    tb_counts = ds_garden.read("nuclear_weapons_treaties_country_counts")

    #
    # Process data.
    #
    # Rename status column to be able to use it in grapher.
    tb_counts = tb_counts.rename(columns={"status": "country"}, errors="raise")

    # Set an appropriate index for the counts table and sort conveniently.
    tb_counts = tb_counts.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_counts])
    ds_grapher.save()
