"""Garden step that combines OWID's energy datasets into the single wide "Energy dataset" published on the catalog.

Tables combined (all on the Total Energy Supply methodology of the 2026 Statistical Review):
* Energy mix (Energy Institute, extended with EIA and Smil).
* Electricity mix (Ember and Energy Institute, extended with historical reconstructions).
* Fossil fuels: production, trade, reserves and consumption in physical units (Energy Institute and EIA).

Auxiliary datasets: regions (ISO codes), population and GDP (Maddison Project Database).

Column names follow one grammar: `<source>_<domain>_<measure>`, with the unit last and no abbreviations, e.g.
`hydro_energy_twh`, `hydro_energy_share_pct`, `hydro_electricity_per_capita_kwh`, `coal_production_twh`.
A second table, `column_mapping`, records the name each column had in the previous release of the dataset
(the `owid-energy-data.csv` file), so that users can migrate.
"""

import numpy as np
from owid.catalog import Dataset, Origin, Table
from owid.catalog import processing as pr

from etl.data_helpers.geo import add_gdp_to_table
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Columns that identify a row or give context, kept first in the output.
CONTEXT_COLUMNS = ["country", "year", "iso_code", "population", "gdp"]

########################################################################################################################
# Energy mix (total energy supply).
# Source columns already follow the grammar, but without the domain: `hydro_twh` becomes `hydro_energy_twh`.
########################################################################################################################

# Source names in energy_mix that carry no explicit domain (their columns get the `_energy_` domain added).
ENERGY_SOURCES = [
    "biofuels",
    "coal",
    "fossil_fuels",
    "gas",
    "hydro",
    "nuclear",
    "oil",
    "other_renewables",
    "renewables",
    "solar",
    "solar_and_wind",
    "traditional_biomass",
    "wind",
]
# Columns of energy_mix that already carry a domain and are kept as they are.
ENERGY_COLUMNS_KEPT = [
    "low_carbon_energy_annual_change_pct",
    "low_carbon_energy_annual_change_twh",
    "low_carbon_energy_per_capita_kwh",
    "low_carbon_energy_share_pct",
    "low_carbon_energy_twh",
    "total_energy_supply_annual_change_pct",
    "total_energy_supply_annual_change_twh",
    "total_energy_supply_per_capita_kwh",
    "total_energy_supply_per_gdp_kwh_per_dollar",
    "total_energy_supply_twh",
]
ENERGY_MEASURES = [
    "twh",
    "share_pct",
    "share_including_biomass_pct",
    "per_capita_kwh",
    "annual_change_pct",
    "annual_change_twh",
]

########################################################################################################################
# Electricity mix.
# Source columns use an older grammar (`hydro_generation__twh`, `per_capita_hydro_generation__kwh`); they are
# renamed to `hydro_electricity_twh`, `hydro_electricity_per_capita_kwh`, `hydro_electricity_share_pct`.
########################################################################################################################

# Source names as they appear in electricity_mix, mapped to the name used in the output.
ELECTRICITY_SOURCES = {
    "bioenergy": "bioenergy",
    "coal": "coal",
    "fossil": "fossil_fuels",
    "gas": "gas",
    "hydro": "hydro",
    "low_carbon": "low_carbon",
    "nuclear": "nuclear",
    "oil": "oil",
    "other_renewables_excluding_bioenergy": "other_renewables_excluding_bioenergy",
    "other_renewables_including_bioenergy": "other_renewables_including_bioenergy",
    "renewable": "renewables",
    "solar": "solar",
    "solar_and_wind": "solar_and_wind",
    "wind": "wind",
}
ELECTRICITY_TOTALS = {
    "total_generation__twh": "electricity_generation_twh",
    "per_capita_total_generation__kwh": "electricity_generation_per_capita_kwh",
    "total_demand__twh": "electricity_demand_twh",
    "per_capita_total_demand__kwh": "electricity_demand_per_capita_kwh",
    "total_net_imports__twh": "electricity_net_imports_twh",
    "net_imports_share_of_demand__pct": "electricity_net_imports_share_of_demand_pct",
    "total_electricity_share_of_primary_energy__pct": "electricity_share_of_total_energy_supply_pct",
    "total_emissions__mtco2": "electricity_emissions_mtco2",
    "co2_intensity__gco2_kwh": "electricity_carbon_intensity_gco2_kwh",
}
# Chart helpers and duplicates that do not belong in the public file:
# - the "stacked" columns are zero-filled copies for stacked charts;
# - `other_renewables_generation__twh` includes bioenergy in the years where the source does not separate them,
#   so its meaning changes over time; the explicit including/excluding columns are kept instead;
# - population is added again below, so that every row has it.
ELECTRICITY_COLUMNS_DROPPED = [
    "bioenergy_stacked_generation__twh",
    "per_capita_bioenergy_stacked_generation__kwh",
    "other_renewables_generation__twh",
    "per_capita_other_renewables_generation__kwh",
    "population",
]

########################################################################################################################
# Fossil fuels.
# Source columns already follow the grammar. Consumption in energy units is dropped: for countries it is identical
# to the energy-mix columns (`coal_energy_twh`, ...), and for the World before 1965 the two tables differ, so
# publishing both would be confusing. Consumption in physical units (tonnes, cubic meters) is kept.
########################################################################################################################

FOSSIL_FUELS_RENAMED = {
    "total_production_twh": "fossil_fuels_production_twh",
    "total_production_per_capita_kwh": "fossil_fuels_production_per_capita_kwh",
    "coal_reserves_to_production_ratio": "coal_reserves_to_production_ratio_years",
    "gas_reserves_to_production_ratio": "gas_reserves_to_production_ratio_years",
    "oil_reserves_to_production_ratio": "oil_reserves_to_production_ratio_years",
}
FOSSIL_FUELS_COLUMNS_DROPPED = [
    "coal_consumption_twh",
    "coal_consumption_per_capita_kwh",
    "gas_consumption_twh",
    "gas_consumption_per_capita_kwh",
    "oil_consumption_twh",
    "oil_consumption_per_capita_kwh",
    "total_consumption_twh",
    "total_consumption_per_capita_kwh",
]

########################################################################################################################
# Names in the previous release of the dataset (owid-energy-data.csv), mapped to the new names.
# A value of None means the column is no longer published.
########################################################################################################################

PREVIOUS_COLUMNS = {
    "country": "country",
    "year": "year",
    "iso_code": "iso_code",
    "population": "population",
    "gdp": "gdp",
    "biofuel_cons_change_pct": "biofuels_energy_annual_change_pct",
    "biofuel_cons_change_twh": "biofuels_energy_annual_change_twh",
    "biofuel_cons_per_capita": "biofuels_energy_per_capita_kwh",
    "biofuel_consumption": "biofuels_energy_twh",
    "biofuel_elec_per_capita": "bioenergy_electricity_per_capita_kwh",
    "biofuel_electricity": "bioenergy_electricity_twh",
    "biofuel_share_elec": "bioenergy_electricity_share_pct",
    "biofuel_share_energy": "biofuels_energy_share_pct",
    "carbon_intensity_elec": "electricity_carbon_intensity_gco2_kwh",
    "coal_cons_change_pct": "coal_energy_annual_change_pct",
    "coal_cons_change_twh": "coal_energy_annual_change_twh",
    "coal_cons_per_capita": "coal_energy_per_capita_kwh",
    "coal_consumption": "coal_energy_twh",
    "coal_elec_per_capita": "coal_electricity_per_capita_kwh",
    "coal_electricity": "coal_electricity_twh",
    "coal_prod_change_pct": "coal_production_annual_change_pct",
    "coal_prod_change_twh": "coal_production_annual_change_twh",
    "coal_prod_per_capita": "coal_production_per_capita_kwh",
    "coal_production": "coal_production_twh",
    "coal_share_elec": "coal_electricity_share_pct",
    "coal_share_energy": "coal_energy_share_pct",
    "electricity_demand": "electricity_demand_twh",
    "electricity_demand_per_capita": "electricity_demand_per_capita_kwh",
    "electricity_generation": "electricity_generation_twh",
    "electricity_share_energy": "electricity_share_of_total_energy_supply_pct",
    "energy_cons_change_pct": "total_energy_supply_annual_change_pct",
    "energy_cons_change_twh": "total_energy_supply_annual_change_twh",
    "energy_per_capita": "total_energy_supply_per_capita_kwh",
    "energy_per_gdp": "total_energy_supply_per_gdp_kwh_per_dollar",
    "fossil_cons_change_pct": "fossil_fuels_energy_annual_change_pct",
    "fossil_cons_change_twh": "fossil_fuels_energy_annual_change_twh",
    "fossil_elec_per_capita": "fossil_fuels_electricity_per_capita_kwh",
    "fossil_electricity": "fossil_fuels_electricity_twh",
    "fossil_energy_per_capita": "fossil_fuels_energy_per_capita_kwh",
    "fossil_fuel_consumption": "fossil_fuels_energy_twh",
    "fossil_share_elec": "fossil_fuels_electricity_share_pct",
    "fossil_share_energy": "fossil_fuels_energy_share_pct",
    "gas_cons_change_pct": "gas_energy_annual_change_pct",
    "gas_cons_change_twh": "gas_energy_annual_change_twh",
    "gas_consumption": "gas_energy_twh",
    "gas_elec_per_capita": "gas_electricity_per_capita_kwh",
    "gas_electricity": "gas_electricity_twh",
    "gas_energy_per_capita": "gas_energy_per_capita_kwh",
    "gas_prod_change_pct": "gas_production_annual_change_pct",
    "gas_prod_change_twh": "gas_production_annual_change_twh",
    "gas_prod_per_capita": "gas_production_per_capita_kwh",
    "gas_production": "gas_production_twh",
    "gas_share_elec": "gas_electricity_share_pct",
    "gas_share_energy": "gas_energy_share_pct",
    "greenhouse_gas_emissions": "electricity_emissions_mtco2",
    "hydro_cons_change_pct": "hydro_energy_annual_change_pct",
    "hydro_cons_change_twh": "hydro_energy_annual_change_twh",
    "hydro_consumption": "hydro_energy_twh",
    "hydro_elec_per_capita": "hydro_electricity_per_capita_kwh",
    "hydro_electricity": "hydro_electricity_twh",
    "hydro_energy_per_capita": "hydro_energy_per_capita_kwh",
    "hydro_share_elec": "hydro_electricity_share_pct",
    "hydro_share_energy": "hydro_energy_share_pct",
    "low_carbon_cons_change_pct": "low_carbon_energy_annual_change_pct",
    "low_carbon_cons_change_twh": "low_carbon_energy_annual_change_twh",
    "low_carbon_consumption": "low_carbon_energy_twh",
    "low_carbon_elec_per_capita": "low_carbon_electricity_per_capita_kwh",
    "low_carbon_electricity": "low_carbon_electricity_twh",
    "low_carbon_energy_per_capita": "low_carbon_energy_per_capita_kwh",
    "low_carbon_share_elec": "low_carbon_electricity_share_pct",
    "low_carbon_share_energy": "low_carbon_energy_share_pct",
    "net_elec_imports": "electricity_net_imports_twh",
    "net_elec_imports_share_demand": "electricity_net_imports_share_of_demand_pct",
    "nuclear_cons_change_pct": "nuclear_energy_annual_change_pct",
    "nuclear_cons_change_twh": "nuclear_energy_annual_change_twh",
    "nuclear_consumption": "nuclear_energy_twh",
    "nuclear_elec_per_capita": "nuclear_electricity_per_capita_kwh",
    "nuclear_electricity": "nuclear_electricity_twh",
    "nuclear_energy_per_capita": "nuclear_energy_per_capita_kwh",
    "nuclear_share_elec": "nuclear_electricity_share_pct",
    "nuclear_share_energy": "nuclear_energy_share_pct",
    "oil_cons_change_pct": "oil_energy_annual_change_pct",
    "oil_cons_change_twh": "oil_energy_annual_change_twh",
    "oil_consumption": "oil_energy_twh",
    "oil_elec_per_capita": "oil_electricity_per_capita_kwh",
    "oil_electricity": "oil_electricity_twh",
    "oil_energy_per_capita": "oil_energy_per_capita_kwh",
    "oil_prod_change_pct": "oil_production_annual_change_pct",
    "oil_prod_change_twh": "oil_production_annual_change_twh",
    "oil_prod_per_capita": "oil_production_per_capita_kwh",
    "oil_production": "oil_production_twh",
    "oil_share_elec": "oil_electricity_share_pct",
    "oil_share_energy": "oil_energy_share_pct",
    "other_renewable_consumption": "other_renewables_energy_twh",
    "other_renewable_electricity": "other_renewables_including_bioenergy_electricity_twh",
    "other_renewable_exc_biofuel_electricity": "other_renewables_excluding_bioenergy_electricity_twh",
    "other_renewables_cons_change_pct": "other_renewables_energy_annual_change_pct",
    "other_renewables_cons_change_twh": "other_renewables_energy_annual_change_twh",
    "other_renewables_elec_per_capita": "other_renewables_including_bioenergy_electricity_per_capita_kwh",
    "other_renewables_elec_per_capita_exc_biofuel": "other_renewables_excluding_bioenergy_electricity_per_capita_kwh",
    "other_renewables_energy_per_capita": "other_renewables_energy_per_capita_kwh",
    "other_renewables_share_elec": "other_renewables_including_bioenergy_electricity_share_pct",
    "other_renewables_share_elec_exc_biofuel": "other_renewables_excluding_bioenergy_electricity_share_pct",
    "other_renewables_share_energy": "other_renewables_energy_share_pct",
    "per_capita_electricity": "electricity_generation_per_capita_kwh",
    "primary_energy_consumption": "total_energy_supply_twh",
    "renewables_cons_change_pct": "renewables_energy_annual_change_pct",
    "renewables_cons_change_twh": "renewables_energy_annual_change_twh",
    "renewables_consumption": "renewables_energy_twh",
    "renewables_elec_per_capita": "renewables_electricity_per_capita_kwh",
    "renewables_electricity": "renewables_electricity_twh",
    "renewables_energy_per_capita": "renewables_energy_per_capita_kwh",
    "renewables_share_elec": "renewables_electricity_share_pct",
    "renewables_share_energy": "renewables_energy_share_pct",
    "solar_cons_change_pct": "solar_energy_annual_change_pct",
    "solar_cons_change_twh": "solar_energy_annual_change_twh",
    "solar_consumption": "solar_energy_twh",
    "solar_elec_per_capita": "solar_electricity_per_capita_kwh",
    "solar_electricity": "solar_electricity_twh",
    "solar_energy_per_capita": "solar_energy_per_capita_kwh",
    "solar_share_elec": "solar_electricity_share_pct",
    "solar_share_energy": "solar_energy_share_pct",
    "wind_cons_change_pct": "wind_energy_annual_change_pct",
    "wind_cons_change_twh": "wind_energy_annual_change_twh",
    "wind_consumption": "wind_energy_twh",
    "wind_elec_per_capita": "wind_electricity_per_capita_kwh",
    "wind_electricity": "wind_electricity_twh",
    "wind_energy_per_capita": "wind_energy_per_capita_kwh",
    "wind_share_elec": "wind_electricity_share_pct",
    "wind_share_energy": "wind_energy_share_pct",
}


def energy_mix_columns(tb: Table) -> dict[str, str]:
    """Map energy_mix columns to their names in the output."""
    columns = {column: column for column in ENERGY_COLUMNS_KEPT}
    for source in ENERGY_SOURCES:
        for measure in ENERGY_MEASURES:
            column = f"{source}_{measure}"
            if column in tb.columns:
                columns[column] = f"{source}_energy_{measure}"
    unmapped = set(tb.columns) - set(columns) - {"country", "year"}
    assert not unmapped, f"Columns of energy_mix neither mapped nor dropped: {sorted(unmapped)}"
    return columns


def electricity_mix_columns(tb: Table) -> dict[str, str]:
    """Map electricity_mix columns to their names in the output."""
    columns = dict(ELECTRICITY_TOTALS)
    for source, name in ELECTRICITY_SOURCES.items():
        columns[f"{source}_generation__twh"] = f"{name}_electricity_twh"
        columns[f"{source}_share_of_electricity__pct"] = f"{name}_electricity_share_pct"
        columns[f"per_capita_{source}_generation__kwh"] = f"{name}_electricity_per_capita_kwh"
    missing = set(columns) - set(tb.columns)
    assert not missing, f"Columns expected in electricity_mix but not found: {sorted(missing)}"
    unmapped = set(tb.columns) - set(columns) - set(ELECTRICITY_COLUMNS_DROPPED) - {"country", "year"}
    assert not unmapped, f"Columns of electricity_mix neither mapped nor dropped: {sorted(unmapped)}"
    return columns


def fossil_fuels_columns(tb: Table) -> dict[str, str]:
    """Map fossil_fuels columns to their names in the output."""
    columns = {
        column: FOSSIL_FUELS_RENAMED.get(column, column)
        for column in tb.columns
        if column not in FOSSIL_FUELS_COLUMNS_DROPPED + ["country", "year"]
    }
    missing = set(FOSSIL_FUELS_RENAMED) | set(FOSSIL_FUELS_COLUMNS_DROPPED)
    missing -= set(tb.columns)
    assert not missing, f"Columns expected in fossil_fuels but not found: {sorted(missing)}"
    return columns


def select_and_rename(tb: Table, columns: dict[str, str]) -> Table:
    return tb[["country", "year"] + list(columns)].rename(columns=columns, errors="raise")


def add_context_columns(tb: Table, ds_regions: Dataset, ds_gdp: Dataset) -> Table:
    """Add ISO codes, population and GDP."""
    tb_regions = (
        ds_regions["regions"]
        .reset_index()[["name", "iso_alpha3"]]
        .rename(columns={"name": "country", "iso_alpha3": "iso_code"})
    )
    tb = pr.merge(tb, tb_regions, on="country", how="left")
    # Dataset-specific regions (e.g. those ending in "(EI)" or "(Ember)") have no population or GDP.
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    tb = add_gdp_to_table(tb=tb, ds_gdp=ds_gdp)
    return tb


def improve_metadata(tb: Table, ds_regions: Dataset) -> None:
    regions_origin = [Origin(producer="Our World in Data", title="Regions", date_published=ds_regions.metadata.version)]
    for column, title, description in [
        ("country", "Country", "Country or region."),
        ("year", "Year", "Year of observation."),
    ]:
        tb[column].metadata.title = title
        tb[column].metadata.description_short = description
        tb[column].metadata.description = None
        tb[column].metadata.unit = ""
        tb[column].metadata.origins = regions_origin

    tb["iso_code"].metadata.title = "ISO code"
    tb[
        "iso_code"
    ].metadata.description_short = (
        "ISO 3166-1 alpha-3 three-letter country code. Empty for regions and other aggregates."
    )
    tb["iso_code"].metadata.unit = ""
    tb["iso_code"].metadata.origins = [
        Origin(
            producer="International Organization for Standardization",
            title="ISO 3166 Country Codes",
            date_published=ds_regions.metadata.version,
            url_main="https://www.iso.org/iso-3166-country-codes.html",
        )
    ]


def sanity_check_columns(tb: Table) -> None:
    columns = tb.columns
    assert list(columns[: len(CONTEXT_COLUMNS)]) == CONTEXT_COLUMNS, "Context columns should come first."
    assert not columns.str.contains("__").any(), f"Double underscores: {columns[columns.str.contains('__')].tolist()}"
    assert (columns == columns.str.lower()).all(), "Column names should be lowercase."
    assert not columns.str.contains(r"\s").any(), "Column names should not contain whitespace."
    for old_abbreviation in ["_cons_", "_elec_", "_prod_", "_exc_"]:
        assert not columns.str.contains(old_abbreviation).any(), f"Abbreviation {old_abbreviation} in column names."
    # Every column of the previous release is either mapped to an existing column or explicitly dropped.
    mapped = {new for new in PREVIOUS_COLUMNS.values() if new is not None}
    missing = mapped - set(columns)
    assert not missing, f"Previous columns mapped to columns that do not exist: {sorted(missing)}"
    assert len(set(columns)) == len(columns), "Repeated column names."


def sanity_check_data(tb: Table) -> None:
    numeric_columns = tb.select_dtypes("number").columns
    columns_with_inf = [column for column in numeric_columns if np.isinf(tb[column].astype(float)).any()]
    assert not columns_with_inf, f"Infinity values in columns: {columns_with_inf}"
    row_all_nan = tb.drop(columns=CONTEXT_COLUMNS).isnull().all(axis=1)
    assert row_all_nan.sum() == 0, f"{row_all_nan.sum()} rows have no data."
    assert tb.codebook["column"].tolist() == tb.columns.tolist(), "Codebook and data columns differ."
    countries_lower = set(tb["country"].str.lower())
    for old_name in ["burma", "macedonia", "swaziland", "czech republic"]:
        assert old_name not in countries_lower, f"Deprecated country name: {old_name}"
    # A few values that pin the methodology: on the total energy supply basis, the World's hydro is far below
    # the substitution-method figure (about 4,300 TWh in 2024 instead of about 11,000 TWh).
    world_2024 = tb[(tb["country"] == "World") & (tb["year"] == 2024)]
    assert len(world_2024) == 1
    assert 3000 < world_2024["hydro_energy_twh"].item() < 6000, "World hydro 2024 is not on the TES basis."
    assert 150000 < world_2024["total_energy_supply_twh"].item() < 190000


def build_column_mapping(tb: Table) -> Table:
    """Table listing the name of each column in the previous release next to its current name."""
    previous = {new: old for old, new in PREVIOUS_COLUMNS.items() if new is not None}
    tb_mapping = Table(
        {
            "column": list(tb.columns),
            "previous_column": [previous.get(column) for column in tb.columns],
        },
        short_name="column_mapping",
    )
    dropped = Table(
        {
            "column": [None] * sum(new is None for new in PREVIOUS_COLUMNS.values()),
            "previous_column": [old for old, new in PREVIOUS_COLUMNS.items() if new is None],
        },
        short_name="column_mapping",
    )
    tb_mapping = pr.concat([tb_mapping, dropped], ignore_index=True)
    tb_mapping["status"] = "unchanged"
    tb_mapping.loc[tb_mapping["previous_column"].isnull(), "status"] = "new"
    tb_mapping.loc[tb_mapping["column"].isnull(), "status"] = "dropped"
    renamed = tb_mapping["previous_column"].notnull() & (tb_mapping["previous_column"] != tb_mapping["column"])
    tb_mapping.loc[renamed, "status"] = "renamed"
    tb_mapping["column"] = tb_mapping["column"].fillna("")
    tb_mapping["previous_column"] = tb_mapping["previous_column"].fillna("")
    # Index columns cannot take their metadata from the yaml file, so all three are described here.
    for column, title, description in [
        ("column", "Column", "Name of the column in the current release. Empty for columns no longer published."),
        ("previous_column", "Previous column", "Name of the column in the previous release. Empty for new columns."),
        ("status", "Status", 'One of "unchanged", "renamed", "new" or "dropped".'),
    ]:
        tb_mapping[column].metadata.title = title
        tb_mapping[column].metadata.description_short = description
        tb_mapping[column].metadata.unit = ""
        tb_mapping[column].metadata.origins = []
    return tb_mapping


def run() -> None:
    #
    # Load data.
    #
    ds_energy_mix = paths.load_dataset("energy_mix")
    ds_electricity_mix = paths.load_dataset("electricity_mix")
    ds_fossil_fuels = paths.load_dataset("fossil_fuels")
    ds_gdp = paths.load_dataset("maddison_project_database")
    ds_regions = paths.load_dataset("regions")

    tb_energy_mix = ds_energy_mix.read("energy_mix")
    # The electricity mix dataset also has a monthly table; only the annual one is used.
    tb_electricity_mix = ds_electricity_mix.read("electricity_mix")
    tb_fossil_fuels = ds_fossil_fuels.read("fossil_fuels")

    #
    # Process data.
    #
    tb_energy_mix = select_and_rename(tb_energy_mix, energy_mix_columns(tb_energy_mix))
    tb_electricity_mix = select_and_rename(tb_electricity_mix, electricity_mix_columns(tb_electricity_mix))
    tb_fossil_fuels = select_and_rename(tb_fossil_fuels, fossil_fuels_columns(tb_fossil_fuels))

    tb = pr.multi_merge([tb_energy_mix, tb_electricity_mix, tb_fossil_fuels], on=["country", "year"], how="outer")
    tb = add_context_columns(tb, ds_regions=ds_regions, ds_gdp=ds_gdp)

    # Context columns first, then everything else in alphabetical order.
    data_columns = sorted(column for column in tb.columns if column not in CONTEXT_COLUMNS)
    tb = tb[CONTEXT_COLUMNS + data_columns]

    # Drop rows without any data (context columns aside).
    tb = tb.dropna(subset=data_columns, how="all").reset_index(drop=True)

    improve_metadata(tb, ds_regions=ds_regions)
    sanity_check_columns(tb)
    sanity_check_data(tb)

    tb_mapping = build_column_mapping(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name, sort_columns=False)
    tb_mapping = tb_mapping.format(["column", "previous_column"], short_name="column_mapping", sort_columns=False)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_mapping])
    ds_garden.save()
