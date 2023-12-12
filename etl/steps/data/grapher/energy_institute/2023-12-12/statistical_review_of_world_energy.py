"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_item_prices_table(tb_prices: Table, item_name: str) -> Table:
    if item_name == "oil_price":
        tb_item_prices = (
            tb_prices[["oil_price_crude_current_dollars_per_m3", "oil_price_crude_2022_dollars_per_m3"]]
            .reset_index()
            .assign(**{"country": "World"})
        )
    else:
        # Select relevant columns and rename them conveniently.
        tb_item_prices = tb_prices[[column for column in tb_prices.columns if column.startswith(item_name)]].copy()
        tb_item_prices = tb_item_prices.rename(
            columns={column: tb_prices[column].metadata.title.split(" - ")[1] for column in tb_item_prices.columns}
        )

        # Transpose table to have a column for price category (called "country", to adapt to grapher) and value.
        tb_item_prices = (
            tb_item_prices.reset_index().melt(id_vars=["year"], var_name="country", value_name=item_name).dropna()
        )

        # Improve metadata.
        tb_item_prices[item_name].metadata.title = item_name.capitalize().replace("_", " ")

    # Create a new short name for this table.
    tb_item_prices.metadata.short_name = f"statistical_review_of_world_energy_{item_name}"

    # Set an appropriate index and sort conveniently.
    tb_item_prices = tb_item_prices.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb_item_prices


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table and the fossil fuel prices table.
    ds_garden = cast(Dataset, paths.load_dependency("statistical_review_of_world_energy"))
    tb = ds_garden["statistical_review_of_world_energy"]
    tb_prices = ds_garden["statistical_review_of_world_energy_fossil_fuel_prices"]
    tb_price_index = ds_garden["statistical_review_of_world_energy_fossil_fuel_price_index"]

    #
    # Process data.
    #
    # Prepare price tables.
    tb_coal_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="coal_price")
    tb_gas_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="gas_price")
    tb_oil_spot_crude_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="oil_spot_crude_price")
    tb_oil_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="oil_price")

    # Prepare price index tables.
    tb_coal_price_index = prepare_item_prices_table(tb_prices=tb_price_index, item_name="coal_price_index")
    tb_gas_price_index = prepare_item_prices_table(tb_prices=tb_price_index, item_name="gas_price_index")
    tb_oil_price_index = prepare_item_prices_table(tb_prices=tb_price_index, item_name="oil_spot_crude_price_index")

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[
            tb,
            tb_coal_prices,
            tb_gas_prices,
            tb_oil_spot_crude_prices,
            tb_oil_prices,
            tb_coal_price_index,
            tb_gas_price_index,
            tb_oil_price_index,
        ],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
