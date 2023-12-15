"""Grapher step for the Electricity Mix (Energy Institute & Ember) dataset.

"""


from etl.grapher_helpers import add_columns_for_multiindicator_chart
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("electricity_mix")
    tb_garden = ds_garden["electricity_mix"]

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb_garden.drop(columns=["population"], errors="raise")

    # Create columns for specific multi-indicator charts, to handle issues with missing data.
    # Add columns for chart with slug "electricity-prod-source-stacked".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "other_renewables_excluding_bioenergy_generation__twh",
            "bioenergy_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
            "hydro_generation__twh",
            "nuclear_generation__twh",
            "oil_generation__twh",
            "gas_generation__twh",
            "coal_generation__twh",
        ],
        chart_slug="electricity-prod-source-stacked",
        columns_to_fill_with_zeros=[
            "other_renewables_excluding_bioenergy_generation__twh",
            "bioenergy_generation__twh",
            "solar_generation__twh",
            "wind_generation__twh",
        ],
    )

    # Add columns for chart with slug "elec-fossil-nuclear-renewables".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "renewable_generation__twh",
            "nuclear_generation__twh",
            "fossil_generation__twh",
        ],
        chart_slug="elec-fossil-nuclear-renewables",
        columns_to_fill_with_zeros=[],
    )

    # Add columns for chart with slug "elec-mix-bar".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "renewable_generation__twh",
            "nuclear_generation__twh",
            "fossil_generation__twh",
        ],
        chart_slug="elec-mix-bar",
        columns_to_fill_with_zeros=[],
    )

    # Add columns for chart with slug "per-capita-electricity-source-stacked".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "per_capita_coal_generation__kwh",
            "per_capita_gas_generation__kwh",
            "per_capita_oil_generation__kwh",
            "per_capita_nuclear_generation__kwh",
            "per_capita_hydro_generation__kwh",
            "per_capita_wind_generation__kwh",
            "per_capita_solar_generation__kwh",
            "per_capita_bioenergy_generation__kwh",
            "per_capita_other_renewables_excluding_bioenergy_generation__kwh",
        ],
        chart_slug="per-capita-electricity-source-stacked",
        columns_to_fill_with_zeros=[
            "per_capita_hydro_generation__kwh",
            "per_capita_wind_generation__kwh",
            "per_capita_solar_generation__kwh",
            "per_capita_bioenergy_generation__kwh",
            "per_capita_other_renewables_excluding_bioenergy_generation__kwh",
        ],
    )

    # Add columns for chart with slug "per-capita-electricity-fossil-nuclear-renewables".
    tb = add_columns_for_multiindicator_chart(
        table=tb,
        columns_in_chart=[
            "per_capita_fossil_generation__kwh",
            "per_capita_nuclear_generation__kwh",
            "per_capita_renewable_generation__kwh",
        ],
        chart_slug="per-capita-electricity-fossil-nuclear-renewables",
        columns_to_fill_with_zeros=[],
    )

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
