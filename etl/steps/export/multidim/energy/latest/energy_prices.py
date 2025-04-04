from typing import Any, Dict, List

from owid.catalog.utils import underscore
from pandas import DataFrame

from etl.collections import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Price components to include in the views
INCLUDE_PRICE_COMPONENTS = [
    "total_price_including_taxes",
    "wholesale",
    "consumer_price_components",
]


def prepare_annual_data(tb_annual: DataFrame) -> List[str]:
    """
    Prepare annual energy price data by standardizing dimensions and filtering columns.
    """
    use_cols_annual = []

    for col in tb_annual.columns:
        # Standardize dimension names
        new_dims = {}
        for k, v in tb_annual[col].m.dimensions.items():
            if k == "consumer_type":
                k = "consumer"
            if k == "price_component_or_level":
                k = "price_component"

            new_dims[k] = underscore(v)

        tb_annual[col].m.dimensions = new_dims
        tb_annual[col].m.original_short_name = "price"

        # Only display columns with relevant price components
        if tb_annual[col].m.dimensions["price_component"] in INCLUDE_PRICE_COMPONENTS:
            use_cols_annual.append(col)

    return use_cols_annual


def prepare_monthly_data(tb_monthly: DataFrame) -> None:
    """
    Prepare monthly energy price data by standardizing dimensions.
    """
    col = "monthly_electricity_all_wholesale_euro"
    tb_monthly[col].m.dimensions = {
        "consumer": "all",
        "frequency": "monthly",
        "price_component": "wholesale",
        "source": "electricity",
        "unit": "euro",
    }
    tb_monthly[col].m.original_short_name = "price"


def create_stacked_component_views(tb_annual: DataFrame) -> List[Dict[str, Any]]:
    """
    Create stacked bar chart views for energy price components.
    """
    component_views = []

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

        # Add nuclear taxes only for electricity
        if source == "electricity":
            price_components.append("nuclear_taxes")

        for consumer in ["household", "non_household"]:
            for unit in ["euro", "pps"]:
                # Create indicator list for this view
                indicators = [f"annual_{source}_{consumer}_{component}_{unit}" for component in price_components]

                # Gather description keys from all indicators
                description_keys = list(
                    dict.fromkeys(sum([tb_annual[indicator].metadata.description_key for indicator in indicators], []))
                )
                # Include an additional key description to clarify why some components can be negative.
                description_keys += [
                    'Some price components can be negative. For example, a negative "All other taxes" component may occur when governments introduce compensation measures during periods of high electricity prices to reduce costs for consumers.'
                ]

                # Configure unit-specific text
                if unit == "euro":
                    subtitle = "Prices are given in euros per [megawatt-hour](#dod:watt-hours). They are adjusted for inflation but not for differences in living costs between countries."
                    title_variant = None
                    footnote = "This data is expressed in constant 2015 euros, deflated using the Harmonised Index of Consumer Prices."
                else:
                    subtitle = "Prices are given in [purchasing power standard (PPS)](#dod:pps) per [megawatt-hour](#dod:watt-hours). This data is adjusted for inflation and differences in living costs between countries."
                    title_variant = "PPS"
                    footnote = "PPS have been adjusted for inflation, expressed in 2015 prices, using the Harmonised Index of Consumer Prices."

                # Create title and presentation settings
                title = f"{source.capitalize()} price components for {consumer.replace('_', '-')} consumers"
                presentation = {
                    "title_public": title,
                }
                if title_variant:
                    presentation["title_variant"] = title_variant

                # Add the complete view configuration
                component_views.append(
                    {
                        "dimensions": {
                            "frequency": "annual",
                            "source": source,
                            "price_component": "consumer_price_components",
                            "consumer": consumer,
                            "unit": unit,
                        },
                        "indicators": {
                            "y": [f"energy_prices_annual#{indicator}" for indicator in indicators],
                        },
                        "config": {
                            "chartTypes": ["StackedBar"],
                            "tab": "chart",
                            "title": title,
                            "subtitle": subtitle,
                            "note": footnote,
                        },
                        # Currently, the stacked area chart uses multiple indicators, but the data page shows only the metadata of the first one. We need to override that metadata with the combination of the metadata of all indicators shown.
                        "metadata": {
                            "description_short": subtitle,
                            "description_key": description_keys,
                            "presentation": presentation,
                        },
                    }
                )

    return component_views


def run() -> None:
    """
    Main function to process energy price data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    ds_grapher = paths.load_dataset("energy_prices")
    tb_annual = ds_grapher.read("energy_prices_annual", reset_index=False)
    tb_monthly = ds_grapher.read("energy_prices_monthly", reset_index=False)

    #
    # Process data.
    #
    config = paths.load_mdim_config()

    # Define common view configuration
    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart"],
        "hasMapTab": True,
        "tab": "map",
        "map": {
            "region": "Europe",
            "colorScale": {"baseColorScheme": "YlOrBr"},
        },
    }

    # Prepare data
    use_cols_annual = prepare_annual_data(tb_annual)
    prepare_monthly_data(tb_monthly)

    # Create standard line/map views
    dimensions = ["frequency", "source", "consumer", "price_component", "unit"]
    annual_config = multidim.expand_config(
        tb_annual.loc[:, use_cols_annual],
        indicator_names=["price"],
        dimensions=dimensions,
        common_view_config=common_view_config,
    )

    monthly_config = multidim.expand_config(
        tb_monthly.loc[:, ["monthly_electricity_all_wholesale_euro"]],
        indicator_names=["price"],
        dimensions=dimensions,
        common_view_config=common_view_config,
    )

    # Combine standard views
    config["views"] = annual_config["views"] + monthly_config["views"]

    # Create and add stacked component views
    component_views = create_stacked_component_views(tb_annual)
    config["views"].extend(component_views)

    #
    # Save outputs.
    #
    mdim = paths.create_mdim(config=config)
    mdim.save()
