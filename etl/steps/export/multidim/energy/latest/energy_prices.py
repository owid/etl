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

    # Create special view for the stacked area chart of total consumer price by components.
    table_name = paths.get_dependency_step_name("energy_prices").replace("data://", "")
    for source in ["electricity", "gas"]:
        price_components = [
            # The total price is (to a very good approximation) equivalent to the combination of "Energy and supply", "Network costs", and "Taxes, fees, levies, and charges".
            # We can either show only those three main components, or split the latter into its subcomponents, namely:
            # "Capacity taxes", "Environmental taxes", "Nuclear taxes", "Renewable taxes", "Value added tax (VAT)", "Other".
            # NOTE: For more details, see analysis in the garden gas_and_electricity_prices step).
            # Three main components:
            "energy_and_supply",
            "network_costs",
            # "taxes_fees_levies_and_charges",
            # Subcomponents of "taxes_fees_levies_and_charges":
            "capacity_taxes",
            "environmental_taxes",
            # Nuclear taxes is a component of electricity prices only (not gas), so it will be appended only for electricity.
            # "nuclear_taxes",
            "renewable_taxes",
            "value_added_tax_vat",
            "other",
        ]
        if source == "electricity":
            price_components.append("nuclear_taxes")
        for consumer in ["household", "non_household"]:
            for unit in ["euro", "pps"]:
                subtitle = "Prices are given in euros per [megawatt-hour](#dod:watt-hours)."
                if unit == "euro":
                    subtitle += " They are not adjusted for inflation or differences in living costs between countries."
                config["views"].append(
                    {
                        "dimensions": {
                            "frequency": "annual",
                            "source": source,
                            "price_component": "consumer_price_components",
                            "consumer": consumer,
                            "unit": unit,
                        },
                        "indicators": {
                            "y": [
                                f"{table_name}/energy_prices_annual#annual_{source}_{consumer}_{component}_{unit}"
                                for component in price_components
                            ],
                        },
                        "config": {
                            "chartTypes": ["StackedBar"],
                            "tab": "chart",
                            "title": f"Components of {source} price for {consumer.replace('_', '-')} consumers",
                            "subtitle": subtitle,
                        },
                    },
                )

    #
    # Save outputs.
    #
    multidim.upsert_multidim_data_page(slug="mdd-energy-prices", config=config, engine=get_engine())
