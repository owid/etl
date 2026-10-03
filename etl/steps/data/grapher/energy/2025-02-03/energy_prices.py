"""Load garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    # Load tables from garden dataset.
    ds_garden = paths.load_dataset("energy_prices")
    tb_annual = ds_garden.read("energy_prices_annual", reset_index=False)
    tb_monthly = ds_garden.read("energy_prices_monthly", reset_index=False)

    # Create a new grapher dataset.
    dataset = paths.create_dataset(tables=[tb_annual, tb_monthly], default_metadata=ds_garden.metadata)
    dataset.save()
