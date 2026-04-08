"""Create a garden dataset on renewable energy investments based on UNEP data."""

from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_energy_investments")
    tb_meadow = ds_meadow.read("renewable_energy_investments", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_meadow])
    ds_garden.save()
