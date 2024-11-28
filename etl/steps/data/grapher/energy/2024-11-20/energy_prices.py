"""Load garden dataset and create a grapher dataset.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load tables from garden dataset.
    ds_garden = paths.load_dataset("energy_prices")
    tb_annual = ds_garden.read("energy_prices_annual", reset_index=False)
    tb_monthly = ds_garden.read("energy_prices_monthly", reset_index=False)

    # Create a new grapher dataset.
    dataset = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_annual, tb_monthly],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )
    dataset.save()
