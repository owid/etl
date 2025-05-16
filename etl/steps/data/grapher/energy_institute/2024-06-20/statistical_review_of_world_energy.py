"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Reference year to use for table of prices.
# NOTE: This must be identical to the one defined in the garden step.
PRICE_REFERENCE_YEAR = 2023


def prepare_item_prices_table(tb_prices: Table, item_name: str) -> Table:
    if item_name == "oil_price":
        tb_item_prices = (
            tb_prices[
                ["oil_price_crude_current_dollars_per_m3", f"oil_price_crude_{PRICE_REFERENCE_YEAR}_dollars_per_m3"]
            ]
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
    tb_item_prices = tb_item_prices.format(["country", "year"])

    return tb_item_prices


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table and the fossil fuel prices table.
    ds_garden = paths.load_dataset("statistical_review_of_world_energy")
    tb = ds_garden.read("statistical_review_of_world_energy", reset_index=False)
    tb_prices = ds_garden.read("statistical_review_of_world_energy_prices", reset_index=False)
    tb_price_index = ds_garden.read("statistical_review_of_world_energy_price_index", reset_index=False)

    #
    # Process data.
    #
    # Remove variables that are not used in grapher.
    tb = tb.drop(
        columns=[column for column in tb.columns if column.endswith(("_ej", "_pj", "_bcfd", "_bcm", "_kbd", "_bbl"))],
        errors="raise",
    )

    # Conveniently change units from TCM to cubic meters.
    tb = tb.rename(columns={"gas_reserves_tcm": "gas_reserves_m3"}, errors="raise")
    tb["gas_reserves_m3"] *= 1e12
    tb["gas_reserves_m3"].metadata.title = "Gas proved reserves - m³"
    tb["gas_reserves_m3"].metadata.unit = "cubic meters"
    tb["gas_reserves_m3"].metadata.short_unit = "m³"

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
    ds_grapher = paths.create_dataset(
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
    )
    ds_grapher.save()
