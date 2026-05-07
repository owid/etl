"""Build the CO₂ and Greenhouse Gas Emissions explorer.

54 single-indicator views split across two upstream grapher datasets:
- gcp/2025-11-13/global_carbon_budget (41 indicators) — fossil emissions, cumulative
  totals, per-fuel breakdowns.
- emissions/2025-12-04/national_contributions (13 indicators) — non-CO₂ gases (CH₄,
  N₂O, all-GHG combined) plus warming-impact temperature responses.

Each chart's text (title, subtitle, note, default tab) lives upstream in the
indicator's `presentation.grapher_config` (set in the relevant garden meta YAML); this
step's only job is to tag each column with `m.dimensions` so
`paths.create_collection(tb=[...], ...)` auto-expands one view per indicator.

Two dimensions have an "na" slot:
- `fuel`: consumption-based and non-CO₂ gas views have no fuel breakdown.
- `relative_to_world`: only Per country / Cumulative views expose the toggle.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Each indicator's (gas, accounting, fuel, count, relative_to_world) tuple, sourced from
# the legacy explorer's TSV (54 grapherId rows). Locked here so we exactly preserve
# production's dimensional layout — adding/removing an indicator means editing this map.
COLUMN_DIMENSIONS = {
    "annual_emissions_ch4_total_co2eq": {"gas": "methane", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "total"},
    "annual_emissions_ch4_total_co2eq_per_capita": {"gas": "methane", "accounting": "territorial", "fuel": "na", "count": "per_capita", "relative_to_world": "na"},
    "annual_emissions_ghg_total_co2eq": {"gas": "all_ghg", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "total"},
    "annual_emissions_ghg_total_co2eq_per_capita": {"gas": "all_ghg", "accounting": "territorial", "fuel": "na", "count": "per_capita", "relative_to_world": "na"},
    "annual_emissions_n2o_total_co2eq": {"gas": "nitrous_oxide", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "total"},
    "annual_emissions_n2o_total_co2eq_per_capita": {"gas": "nitrous_oxide", "accounting": "territorial", "fuel": "na", "count": "per_capita", "relative_to_world": "na"},
    "consumption_emissions": {"gas": "co2", "accounting": "consumption_based", "fuel": "na", "count": "per_country", "relative_to_world": "na"},
    "consumption_emissions_per_capita": {"gas": "co2", "accounting": "consumption_based", "fuel": "na", "count": "per_capita", "relative_to_world": "na"},
    "consumption_emissions_per_gdp": {"gas": "co2", "accounting": "consumption_based", "fuel": "na", "count": "per_gdp", "relative_to_world": "na"},
    "cumulative_emissions_from_cement": {"gas": "co2", "accounting": "territorial", "fuel": "cement", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_cement_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "cement", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_from_coal": {"gas": "co2", "accounting": "territorial", "fuel": "coal", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_coal_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "coal", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_from_flaring": {"gas": "co2", "accounting": "territorial", "fuel": "flaring", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_flaring_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "flaring", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_from_gas": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_gas", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_gas_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_gas", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_from_land_use_change": {"gas": "co2", "accounting": "territorial", "fuel": "land_use", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_land_use_change_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "land_use", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_from_oil": {"gas": "co2", "accounting": "territorial", "fuel": "oil", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_from_oil_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "oil", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_total": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_total_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "cumulative", "relative_to_world": "relative"},
    "cumulative_emissions_total_including_land_use_change": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "cumulative", "relative_to_world": "total"},
    "cumulative_emissions_total_including_land_use_change_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "cumulative", "relative_to_world": "relative"},
    "emissions_from_cement": {"gas": "co2", "accounting": "territorial", "fuel": "cement", "count": "per_country", "relative_to_world": "na"},
    "emissions_from_cement_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "cement", "count": "per_capita", "relative_to_world": "na"},
    "emissions_from_coal": {"gas": "co2", "accounting": "territorial", "fuel": "coal", "count": "per_country", "relative_to_world": "na"},
    "emissions_from_coal_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "coal", "count": "per_capita", "relative_to_world": "na"},
    "emissions_from_flaring": {"gas": "co2", "accounting": "territorial", "fuel": "flaring", "count": "per_country", "relative_to_world": "na"},
    "emissions_from_flaring_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "flaring", "count": "per_capita", "relative_to_world": "na"},
    "emissions_from_gas": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_gas", "count": "per_country", "relative_to_world": "na"},
    "emissions_from_gas_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_gas", "count": "per_capita", "relative_to_world": "na"},
    "emissions_from_land_use_change": {"gas": "co2", "accounting": "territorial", "fuel": "land_use", "count": "per_country", "relative_to_world": "total"},
    "emissions_from_land_use_change_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "land_use", "count": "per_country", "relative_to_world": "relative"},
    "emissions_from_land_use_change_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "land_use", "count": "per_capita", "relative_to_world": "na"},
    "emissions_from_oil": {"gas": "co2", "accounting": "territorial", "fuel": "oil", "count": "per_country", "relative_to_world": "na"},
    "emissions_from_oil_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "oil", "count": "per_capita", "relative_to_world": "na"},
    "emissions_total": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "per_country", "relative_to_world": "total"},
    "emissions_total_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "per_country", "relative_to_world": "relative"},
    "emissions_total_including_land_use_change": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "per_country", "relative_to_world": "total"},
    "emissions_total_including_land_use_change_as_share_of_global": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "per_country", "relative_to_world": "relative"},
    "emissions_total_including_land_use_change_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "per_capita", "relative_to_world": "na"},
    "emissions_total_per_capita": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "per_capita", "relative_to_world": "na"},
    "emissions_total_per_gdp": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "per_gdp", "relative_to_world": "na"},
    "emissions_total_per_unit_energy": {"gas": "co2", "accounting": "territorial", "fuel": "all_fossil", "count": "per_kwh", "relative_to_world": "na"},
    "pct_traded_emissions": {"gas": "co2", "accounting": "consumption_based", "fuel": "na", "count": "share_embedded_in_trade", "relative_to_world": "na"},
    "share_of_annual_emissions_ch4_total": {"gas": "methane", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "relative"},
    "share_of_annual_emissions_ghg_total": {"gas": "all_ghg", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "relative"},
    "share_of_annual_emissions_n2o_total": {"gas": "nitrous_oxide", "accounting": "territorial", "fuel": "na", "count": "per_country", "relative_to_world": "relative"},
    "share_of_temperature_response_ghg_total": {"gas": "warming_impact", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "per_country", "relative_to_world": "relative"},
    "temperature_response_ghg_fossil": {"gas": "warming_impact", "accounting": "territorial", "fuel": "all_fossil", "count": "per_country", "relative_to_world": "total"},
    "temperature_response_ghg_land": {"gas": "warming_impact", "accounting": "territorial", "fuel": "land_use", "count": "per_country", "relative_to_world": "total"},
    "temperature_response_ghg_total": {"gas": "warming_impact", "accounting": "territorial", "fuel": "fossil_plus_land_use", "count": "per_country", "relative_to_world": "total"},
}

# Which upstream table each indicator lives in.
COLUMN_DATASET = {
    "annual_emissions_ch4_total_co2eq": "national_contributions",
    "annual_emissions_ch4_total_co2eq_per_capita": "national_contributions",
    "annual_emissions_ghg_total_co2eq": "national_contributions",
    "annual_emissions_ghg_total_co2eq_per_capita": "national_contributions",
    "annual_emissions_n2o_total_co2eq": "national_contributions",
    "annual_emissions_n2o_total_co2eq_per_capita": "national_contributions",
    "consumption_emissions": "global_carbon_budget",
    "consumption_emissions_per_capita": "global_carbon_budget",
    "consumption_emissions_per_gdp": "global_carbon_budget",
    "cumulative_emissions_from_cement": "global_carbon_budget",
    "cumulative_emissions_from_cement_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_from_coal": "global_carbon_budget",
    "cumulative_emissions_from_coal_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_from_flaring": "global_carbon_budget",
    "cumulative_emissions_from_flaring_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_from_gas": "global_carbon_budget",
    "cumulative_emissions_from_gas_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_from_land_use_change": "global_carbon_budget",
    "cumulative_emissions_from_land_use_change_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_from_oil": "global_carbon_budget",
    "cumulative_emissions_from_oil_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_total": "global_carbon_budget",
    "cumulative_emissions_total_as_share_of_global": "global_carbon_budget",
    "cumulative_emissions_total_including_land_use_change": "global_carbon_budget",
    "cumulative_emissions_total_including_land_use_change_as_share_of_global": "global_carbon_budget",
    "emissions_from_cement": "global_carbon_budget",
    "emissions_from_cement_per_capita": "global_carbon_budget",
    "emissions_from_coal": "global_carbon_budget",
    "emissions_from_coal_per_capita": "global_carbon_budget",
    "emissions_from_flaring": "global_carbon_budget",
    "emissions_from_flaring_per_capita": "global_carbon_budget",
    "emissions_from_gas": "global_carbon_budget",
    "emissions_from_gas_per_capita": "global_carbon_budget",
    "emissions_from_land_use_change": "global_carbon_budget",
    "emissions_from_land_use_change_as_share_of_global": "global_carbon_budget",
    "emissions_from_land_use_change_per_capita": "global_carbon_budget",
    "emissions_from_oil": "global_carbon_budget",
    "emissions_from_oil_per_capita": "global_carbon_budget",
    "emissions_total": "global_carbon_budget",
    "emissions_total_as_share_of_global": "global_carbon_budget",
    "emissions_total_including_land_use_change": "global_carbon_budget",
    "emissions_total_including_land_use_change_as_share_of_global": "global_carbon_budget",
    "emissions_total_including_land_use_change_per_capita": "global_carbon_budget",
    "emissions_total_per_capita": "global_carbon_budget",
    "emissions_total_per_gdp": "global_carbon_budget",
    "emissions_total_per_unit_energy": "global_carbon_budget",
    "pct_traded_emissions": "global_carbon_budget",
    "share_of_annual_emissions_ch4_total": "national_contributions",
    "share_of_annual_emissions_ghg_total": "national_contributions",
    "share_of_annual_emissions_n2o_total": "national_contributions",
    "share_of_temperature_response_ghg_total": "national_contributions",
    "temperature_response_ghg_fossil": "national_contributions",
    "temperature_response_ghg_land": "national_contributions",
    "temperature_response_ghg_total": "national_contributions",
}


def _tag(tb, dataset_short: str):
    """Set `m.dimensions` on every column of this dataset that the explorer references."""
    for col, ds in COLUMN_DATASET.items():
        if ds != dataset_short or col not in tb.columns:
            continue
        tb[col].metadata.dimensions = COLUMN_DIMENSIONS[col]
        tb[col].metadata.original_short_name = "value"
    return tb


def run() -> None:
    config = paths.load_collection_config()

    ds_gcb = paths.load_dataset("global_carbon_budget")
    tb_gcb = ds_gcb.read("global_carbon_budget", load_data=False)
    _tag(tb_gcb, "global_carbon_budget")

    ds_nc = paths.load_dataset("national_contributions")
    tb_nc = ds_nc.read("national_contributions", load_data=False)
    _tag(tb_nc, "national_contributions")

    c = paths.create_collection(
        config=config,
        tb=[tb_gcb, tb_nc],
        indicator_names="value",
        short_name="co2",
        explorer=True,
    )

    # Universal chart-level config; per-view title/subtitle/tab inherit from each
    # indicator's upstream `presentation.grapher_config`.
    c.set_global_config(
        {
            "type": "LineChart",
            "hasMapTab": True,
        }
    )

    c.save(tolerate_extra_indicators=True)
