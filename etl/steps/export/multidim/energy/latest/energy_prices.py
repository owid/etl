from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Eurostat data on gas and electricity prices.
    ds_grapher = paths.load_dataset("energy_prices")

    # Read table of prices in euros.
    tb_annual = ds_grapher.read("energy_prices_annual")
    tb_monthly = ds_grapher.read("energy_prices_monthly")

    #
    # Process data.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Create views.
    config["views"] = multidim.generate_views_for_dimensions(
        dimensions=config["dimensions"],
        tables=[tb_annual, tb_monthly],
        dimensions_order_in_slug=("frequency", "source", "consumer", "price_component", "unit"),
        warn_on_missing_combinations=False,
        additional_config={
            "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
            "chartTypes": ["LineChart"],
            "hasMapTab": True,
            "tab": "map",
            "map": {
                "projection": "Europe",
                "colorScale": {"baseColorScheme": "YlOrBr"},
            },
        },
    )

    #
    # Save outputs.
    #
    multidim.upsert_multidim_data_page(slug="mdd-energy-prices", config=config, engine=get_engine())
