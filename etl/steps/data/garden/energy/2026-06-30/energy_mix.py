"""Energy mix based on Total Energy Supply (TES) from the Energy Institute's Statistical Review of World Energy.

Since the 2025 release, the Statistical Review reports energy as Total Energy Supply (physical energy
content method) instead of the old substitution-method primary energy consumption. This step therefore
reports a single TES measure per source (no more "direct" vs "input-equivalent" split).

It consolidates what used to be three separate steps:
- energy_mix: TES by source (absolute, per capita, share of total, annual change).
- primary_energy_consumption: the total, extended with EIA for country coverage, plus per-GDP (Maddison).
- global_primary_energy: the World long-run, extended back with Smil (2017).  [added in a later step]
"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers.geo import add_gdp_to_table
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Countries whose data have to be removed since they were identified as outliers.
OUTLIERS = ["Gibraltar"]

# Base TES sources taken directly from the Statistical Review (SR garden column -> short source name).
SR_SOURCES = {
    "coal_consumption_twh": "coal",
    "oil_consumption_twh": "oil",
    "gas_consumption_twh": "gas",
    "nuclear_consumption_twh": "nuclear",
    "hydro_consumption_twh": "hydro",
    "solar_consumption_twh": "solar",
    "wind_consumption_twh": "wind",
    "other_renewables_consumption_twh": "other_renewables",
    "biofuels_consumption_twh": "biofuels",
}

# All sources for which we report metrics (base sources + aggregates), and their display names.
SOURCE_NAMES = {
    "coal": "Coal",
    "oil": "Oil",
    "gas": "Gas",
    "fossil_fuels": "Fossil fuels",
    "nuclear": "Nuclear",
    "hydro": "Hydropower",
    "solar": "Solar",
    "wind": "Wind",
    "solar_and_wind": "Solar and wind",
    "other_renewables": "Other renewables",
    "renewables": "Renewables",
    "low_carbon_energy": "Low-carbon energy",
    "biofuels": "Biofuels",
}
ALL_SOURCES = list(SOURCE_NAMES)

# Mapping of Smil (2017) World columns (direct energy, in TWh) onto our source columns, used to extend
# the World series before the Statistical Review begins (1965).
# NOTE: We use Smil's commercially-traded sources only. Smil also reports traditional biomass, but the
# Statistical Review does not, so we exclude it to keep the World series on a single (commercial-energy)
# basis with no step at the 1965 splice. Nuclear before 1965 is negligible, so the fact that Smil counts
# it as gross generation (rather than the heat-input basis used by the physical energy content method)
# has no visible effect.
SMIL_SOURCES = {
    "coal__twh_direct_energy": "coal_twh",
    "oil__twh_direct_energy": "oil_twh",
    "gas__twh_direct_energy": "gas_twh",
    "hydropower__twh_direct_energy": "hydro_twh",
    "nuclear__twh_direct_energy": "nuclear_twh",
    "solar__twh_direct_energy": "solar_twh",
    "wind__twh_direct_energy": "wind_twh",
    "other_renewables__twh_direct_energy": "other_renewables_twh",
    "biofuels__twh_direct_energy": "biofuels_twh",
}
# Year from which the Statistical Review covers the World (Smil only fills the earlier years).
STATISTICAL_REVIEW_FIRST_YEAR = 1965


def get_statistical_review_data(tb_review: Table) -> Table:
    """Select the TES-by-source columns and the total from the Statistical Review."""
    tb = tb_review.reset_index()[["country", "year", "total_energy_supply_twh"] + list(SR_SOURCES)]
    tb = tb.rename(columns={col: f"{name}_twh" for col, name in SR_SOURCES.items()}, errors="raise")
    return tb


def add_smil_world_long_run(tb: Table, tb_smil: Table) -> Table:
    """Extend the World series back to 1800 with Smil (2017), before the Statistical Review begins.

    Only the World is affected, and only years before the Statistical Review's coverage (1965); the
    modern series is left unchanged.
    """
    smil = tb_smil.reset_index()
    smil = smil[smil["country"] == "World"][["country", "year"] + list(SMIL_SOURCES)].rename(
        columns=SMIL_SOURCES, errors="raise"
    )
    # Keep only years before the Statistical Review's World coverage.
    smil = smil[smil["year"] < STATISTICAL_REVIEW_FIRST_YEAR].reset_index(drop=True)
    # The total energy supply for these early years is the sum of the sources.
    smil["total_energy_supply_twh"] = smil[list(SMIL_SOURCES.values())].sum(axis=1, min_count=1)

    # Combine, prioritizing the Statistical Review; Smil only fills the earlier World years.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=smil, index_columns=["country", "year"])
    return tb


def add_aggregate_sources(tb: Table) -> Table:
    """Create aggregate sources (fossil fuels, renewables, low-carbon energy, solar and wind)."""
    tb = tb.copy()
    # Fossil fuels.
    tb["fossil_fuels_twh"] = tb[["coal_twh", "oil_twh", "gas_twh"]].sum(axis=1, min_count=3)
    # Renewables (hydro is the anchor; other renewable sources are often missing in early years, filled with zeros).
    tb["renewables_twh"] = (
        tb["hydro_twh"]
        + tb["solar_twh"].fillna(0)
        + tb["wind_twh"].fillna(0)
        + tb["other_renewables_twh"].fillna(0)
        + tb["biofuels_twh"].fillna(0)
    )
    # Low-carbon energy (renewables plus nuclear).
    tb["low_carbon_energy_twh"] = tb["renewables_twh"] + tb["nuclear_twh"].fillna(0)
    # Solar and wind.
    tb["solar_and_wind_twh"] = tb["solar_twh"].fillna(0) + tb["wind_twh"].fillna(0)
    return tb


def extend_total_with_eia(tb: Table, tb_eia: Table) -> Table:
    """Extend the total energy supply with EIA data, to cover countries not in the Statistical Review.

    The Statistical Review is prioritized on overlapping country-years; EIA adds rows for countries and
    years the Statistical Review does not cover (those rows have no by-source breakdown).
    """
    tb_eia = tb_eia.reset_index()[["country", "year", "total_energy_consumption"]].rename(
        columns={"total_energy_consumption": "total_energy_supply_twh"}, errors="raise"
    )
    tb_eia = tb_eia.dropna(subset=["total_energy_supply_twh"]).reset_index(drop=True)

    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix): region totals come from the
    # Statistical Review (OWID regions); EIA is used only to extend country coverage.
    tb_eia = tb_eia[~tb_eia["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)

    # Combine, prioritizing the Statistical Review (placed last) on overlapping country-years.
    tb = pr.concat([tb_eia, tb], ignore_index=True).drop_duplicates(subset=["country", "year"], keep="last")
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    return tb


def add_shares(tb: Table) -> Table:
    """Add the share of each source in total energy supply (as a percentage)."""
    tb = tb.copy()
    for source in ALL_SOURCES:
        tb[f"{source}_share_pct"] = 100 * tb[f"{source}_twh"] / tb["total_energy_supply_twh"]
    return tb


def add_annual_change(tb: Table) -> Table:
    """Add annual change (absolute and percentage) for each source and the total.

    Only consecutive-year changes are kept: the World long-run series (Smil) is decadal before 1900,
    so a naive row-to-row change there would be a multi-year change mislabeled as an annual change.
    """
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    is_consecutive = tb.groupby("country", observed=True)["year"].diff() == 1
    for source in ALL_SOURCES + ["total_energy_supply"]:
        pct_change = tb.groupby("country", observed=True)[f"{source}_twh"].pct_change(fill_method=None) * 100
        abs_change = tb.groupby("country", observed=True)[f"{source}_twh"].diff()
        tb[f"{source}_annual_change_pct"] = pct_change.where(is_consecutive)
        tb[f"{source}_annual_change_twh"] = abs_change.where(is_consecutive)
    return tb


def add_per_capita(tb: Table) -> Table:
    """Add per-capita variables (in kWh per person) for each source and the total."""
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    for source in ALL_SOURCES + ["total_energy_supply"]:
        tb[f"{source}_per_capita_kwh"] = tb[f"{source}_twh"] / tb["population"] * TWH_TO_KWH
    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def add_per_gdp(tb: Table, ds_gdp: Dataset) -> Table:
    """Add total energy supply per unit of GDP (in kWh per dollar), using Maddison GDP."""
    tb = add_gdp_to_table(tb=tb, ds_gdp=ds_gdp, gdp_col="gdp")
    tb["total_energy_supply_per_gdp_kwh_per_dollar"] = tb["total_energy_supply_twh"] / tb["gdp"] * TWH_TO_KWH
    tb = tb.drop(columns=["gdp"], errors="raise")
    return tb


def add_variable_metadata(tb: Table) -> Table:
    """Set title, unit and short unit for all generated variables."""
    metric_specs = {
        "twh": ("{name}", "terawatt-hours", "TWh"),
        "per_capita_kwh": ("{name} per capita", "kilowatt-hours per person", "kWh"),
        "share_pct": ("{name} as a share of total energy supply", "%", "%"),
        "annual_change_twh": ("Annual change in {name_lower}", "terawatt-hours", "TWh"),
        "annual_change_pct": ("Annual change in {name_lower} (%)", "%", "%"),
    }
    source_names = {**SOURCE_NAMES, "total_energy_supply": "Total energy supply"}
    for column in tb.columns:
        for suffix, (title_template, unit, short_unit) in metric_specs.items():
            for source, name in source_names.items():
                if column == f"{source}_{suffix}":
                    tb[column].metadata.title = title_template.format(name=name, name_lower=name.lower())
                    tb[column].metadata.unit = unit
                    tb[column].metadata.short_unit = short_unit
    # Per-GDP variable.
    if "total_energy_supply_per_gdp_kwh_per_dollar" in tb.columns:
        tb["total_energy_supply_per_gdp_kwh_per_dollar"].metadata.title = "Total energy supply per unit of GDP"
        tb["total_energy_supply_per_gdp_kwh_per_dollar"].metadata.unit = "kilowatt-hours per dollar"
        tb["total_energy_supply_per_gdp_kwh_per_dollar"].metadata.short_unit = "kWh"
    return tb


def sanity_check_outputs(tb: Table) -> None:
    # No fully-NaN columns.
    assert tb.columns[tb.isna().all()].empty, f"Fully-NaN columns: {list(tb.columns[tb.isna().all()])}"
    # Shares should be within [0, 100] (allowing a small tolerance).
    for source in ALL_SOURCES:
        col = f"{source}_share_pct"
        valid = tb[col].dropna()
        assert (valid >= -0.01).all() and (valid <= 100.01).all(), f"{col} out of [0, 100]."
    # World total energy supply for the latest year should be in a plausible range (~600 EJ ~= 167000 TWh).
    world_latest = tb[(tb["country"] == "World")].sort_values("year").iloc[-1]
    assert 140000 < world_latest["total_energy_supply_twh"] < 200000, (
        f"World total energy supply is out of the expected range: {world_latest['total_energy_supply_twh']:.0f} TWh."
    )


def run() -> None:
    #
    # Load data.
    #
    # Load the Statistical Review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    # Load the EIA International Energy dataset and read its main table.
    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy", reset_index=False)

    # Load the Maddison GDP dataset.
    ds_gdp = paths.load_dataset("maddison_project_database")

    # Load the Smil (2017) dataset, used to extend the World series before 1965.
    ds_smil = paths.load_dataset("smil_2017")
    tb_smil = ds_smil.read("smil_2017")

    #
    # Process data.
    #
    # Select TES-by-source data from the Statistical Review.
    tb = get_statistical_review_data(tb_review=tb_review)

    # Extend the World series back to 1800 with Smil (2017).
    tb = add_smil_world_long_run(tb=tb, tb_smil=tb_smil)

    # Create aggregate sources (fossil fuels, renewables, low-carbon energy, solar and wind).
    tb = add_aggregate_sources(tb=tb)

    # Extend the total energy supply with EIA data (for countries not covered by the Statistical Review).
    tb = extend_total_with_eia(tb=tb, tb_eia=tb_eia)

    # Add shares, annual change, per-capita and per-GDP variables.
    tb = add_shares(tb=tb)
    tb = add_annual_change(tb=tb)
    tb = add_per_capita(tb=tb)
    tb = add_per_gdp(tb=tb, ds_gdp=ds_gdp)

    # Remove outliers.
    tb = tb[~tb["country"].isin(OUTLIERS)].reset_index(drop=True)

    # Set variable metadata.
    tb = add_variable_metadata(tb=tb)

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Format table conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
