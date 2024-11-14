"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gas_and_electricity_prices")

    # Read tables from garden dataset.
    tb_euro = ds_garden["gas_and_electricity_price_components_euro_flat"]
    tb_pps = ds_garden["gas_and_electricity_price_components_pps_flat"]

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_euro, tb_pps], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
