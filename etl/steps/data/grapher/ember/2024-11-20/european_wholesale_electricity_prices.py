"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("european_wholesale_electricity_prices")

    # Read tables from garden dataset.
    tb_monthly = ds_garden.read("european_wholesale_electricity_prices_monthly", reset_index=False)
    tb_annual = ds_garden.read("european_wholesale_electricity_prices_annual", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_monthly, tb_annual], check_variables_metadata=True)
    ds_grapher.save()
