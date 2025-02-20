from typing import Dict, List

from owid.catalog import Table

from etl import multidim
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
    tb_annual = ds_grapher.read("energy_prices_annual", reset_index=False)
    tb_monthly = ds_grapher.read("energy_prices_monthly", reset_index=False)

    #
    # Process data.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Get possible dimension values
    dim_options = {}
    for dim in config["dimensions"]:
        dim_options[dim["slug"]] = [c["slug"] for c in dim["choices"]]

    # TODO: construct indicator names from dimensions and if it is there, add dimensions info

    # Add dimensions to the metadata of the table.
    # e.g. annual_gas_household_other_pps -> annual, gas, household, other, pps
    use_cols_annual = add_dimensions_to_metadata(tb_annual, dim_options)
    use_cols_monthly = add_dimensions_to_metadata(tb_monthly, dim_options)

    common_view_config = {
        "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
        "chartTypes": ["LineChart"],
        "hasMapTab": True,
        "tab": "map",
        "map": {
            "projection": "Europe",
            "colorScale": {"baseColorScheme": "YlOrBr"},
        },
    }

    # Create views.
    dimensions = ["frequency", "source", "consumer", "price_component", "unit"]

    annual_config = multidim.expand_config(
        tb_annual.loc[:, use_cols_annual],
        indicator_name="annual",
        dimensions=dimensions,
        common_view_config=common_view_config,
    )
    monthly_config = multidim.expand_config(
        tb_monthly.loc[:, use_cols_monthly],
        indicator_name="monthly",
        dimensions=dimensions,
        common_view_config=common_view_config,
    )
    print(annual_config["views"])
    config["views"] = annual_config["views"] + monthly_config["views"]

    # Create special view for the stacked area chart of total consumer price by components.
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
                title = f"{source.capitalize()} price components for {consumer.replace('_', '-')} consumers"
                indicators = [f"annual_{source}_{consumer}_{component}_{unit}" for component in price_components]
                description_keys = list(
                    dict.fromkeys(sum([tb_annual[indicator].metadata.description_key for indicator in indicators], []))
                )
                # Include an additional key description to clarify why some components can be negative.
                description_keys += [
                    'Some price components can be negative. For example, a negative "All other taxes" component may occur when governments introduce compensation measures during periods of high electricity prices to reduce costs for consumers.'
                ]
                if unit == "euro":
                    subtitle = "Prices are given in euros per [megawatt-hour](#dod:watt-hours). They are adjusted for inflation but not for differences in living costs between countries."
                    title_variant = None
                    footnote = "This data is expressed in constant 2015 euros, deflated using the Harmonised Index of Consumer Prices."
                else:
                    subtitle = "Prices are given in [purchasing power standard (PPS)](#dod:pps) per [megawatt-hour](#dod:watt-hours). This data is adjusted for inflation and differences in living costs between countries."
                    title_variant = "PPS"
                    footnote = "PPS have been adjusted for inflation, expressed in 2015 prices, using the Harmonised Index of Consumer Prices."
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
                            "descriptionShort": subtitle,
                            "descriptionKey": description_keys,
                            "presentation": {"titlePublic": title, "titleVariant": title_variant},
                        },
                    },
                )

    #
    # Save outputs.
    #
    multidim.upsert_multidim_data_page(
        slug="mdd-energy-prices",
        config=config,
        dependencies=paths.dependencies,
    )


def add_dimensions_to_metadata(tb: Table, dim_options: Dict[str, List[str]]) -> List[str]:
    use_cols = []
    for col in tb:
        filters = []
        try:
            for dim, values in dim_options.items():
                filters.append({"name": dim, "value": [v for v in values if v in col][0]})
        # If the column does not contain any of the dimension values, skip it.
        except IndexError:
            continue

        tb[col].metadata.additional_info = {
            "dimensions": {
                "originalShortName": col.split("_")[0],
                "short_name": col,
                "filters": filters,
            }
        }

        use_cols.append(col)

    return use_cols
