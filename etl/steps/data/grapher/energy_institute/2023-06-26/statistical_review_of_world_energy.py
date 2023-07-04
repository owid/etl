"""Load a garden dataset and create a grapher dataset."""

import copy
from typing import cast

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_item_prices_table(tb_prices: Table, item_name: str) -> Table:
    if item_name == "oil_price":
        tb_item_prices = (
            tb_prices[["oil_price_crude_dollar_money_of_the_day", "oil_price_crude_dollar_2022"]]
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

    #
    # Process data.
    #
    # For visualization purposes, fill missing values with zeros in variables used in stacked area charts.
    for column in [
        "biofuels_consumption_twh",
        "coal_consumption_twh",
        "gas_consumption_twh",
        "other_renewables_consumption_equivalent_twh",
        "hydro_consumption_equivalent_twh",
        "nuclear_consumption_equivalent_twh",
        "oil_consumption_twh",
        "solar_consumption_equivalent_twh",
        "wind_consumption_equivalent_twh",
    ]:
        new_column = f"{column}_zero_filled"
        tb[new_column] = tb[column].fillna(0)
        # Note: The following line may not be necessary once "fillna" properly propagates metadata.
        tb[new_column].metadata = copy.deepcopy(tb[column].metadata)
        tb[new_column].metadata.title += " (zero filled)"
        tb[new_column].metadata.description += " Missing values have been filled with zeros for visualization purposes."

    # Prepare price tables.
    tb_coal_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="coal_price")
    tb_gas_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="gas_price")
    tb_oil_spot_crude_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="oil_spot_crude_price")
    tb_oil_prices = prepare_item_prices_table(tb_prices=tb_prices, item_name="oil_price")

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb, tb_coal_prices, tb_gas_prices, tb_oil_spot_crude_prices, tb_oil_prices],
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
