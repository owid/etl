"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_innovation_long_run")

    # Read table from meadow dataset.
    tb_lighting_prices = ds_meadow.read("lighting_prices")

    #
    # Process data.
    #

    # Process lighting prices table to make it long with dimensions.
    tb_lighting_prices = process_lighting_prices(tb_lighting_prices=tb_lighting_prices)

    # Improve table format.
    tb_lighting_prices = tb_lighting_prices.format(["country", "year", "lighting_source", "price_year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_lighting_prices], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def process_lighting_prices(tb_lighting_prices: Table) -> Table:
    """Process lighting prices table, making it long with dimensions."""
    # Make the table long
    tb_lighting_prices = tb_lighting_prices.melt(
        id_vars=["country", "year"],
        var_name="measure",
        value_name="value",
    )

    # Extract lighting_source and price_year from measure column
    tb_lighting_prices["lighting_source"] = tb_lighting_prices["measure"].str.extract(
        r"lighting_prices_(.+?)_\d{4}prices", expand=False
    )
    tb_lighting_prices["price_year"] = (
        tb_lighting_prices["measure"].str.extract(r"lighting_prices_.+?_(\d{4})prices", expand=False).astype(int)
    )

    # Drop the original measure column
    tb_lighting_prices = tb_lighting_prices.drop(columns=["measure"])

    # Rename value column
    tb_lighting_prices = tb_lighting_prices.rename(columns={"value": "lighting_price"}, errors="raise")

    # Replace underscore with space in lighting_source
    tb_lighting_prices["lighting_source"] = tb_lighting_prices["lighting_source"].str.replace("_", " ")

    return tb_lighting_prices
