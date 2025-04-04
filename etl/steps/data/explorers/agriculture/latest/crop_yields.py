"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Crop Yields explorer:
https://ourworldindata.org/explorers/crop-yields
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("attainable_yields")
    tb_garden = ds_garden.read("attainable_yields")

    #
    # Process data.
    #
    # Remove custom regions (that clutter the explorer and do not show in the map).
    tb_garden = tb_garden[~tb_garden["country"].str.contains("(Mueller et al. (2012))", regex=False)].reset_index(
        drop=True
    )

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = paths.create_dataset(tables=[tb_garden], formats=["csv"])
    ds_explorer.save()
