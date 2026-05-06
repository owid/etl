"""Garden step for the OWID Fossil Fuels dataset, used by the fossil-fuels explorer.

Combines:
  - EIA International Energy (bulk file) — production, consumption, imports, exports for coal,
    natural gas, and petroleum/oil; plus coal reserves.
  - Energy Institute Statistical Review of World Energy — natural gas and oil reserves
    (these are not in the EIA bulk file).

All extensive indicators are produced in physical units (tonnes for coal, cubic metres for gas
and oil) so the explorer matches the old natural-resources/CSV-based one. Per-capita variants,
net imports, and import/export shares are derived after region aggregation so they apply to
both countries and OWID regions.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Conversion factors (sourced from the original natural_resources importer).
SHORT_TON_TO_TONNE = 0.9071847
BARREL_TO_M3 = 0.1589873  # 1 barrel = 0.1589873 m³
DAYS_PER_JULIAN_YEAR = 365.25
# 1 thousand barrels per day → m³ per (Julian) year.
KBPD_TO_M3_PER_YEAR = 1000 * DAYS_PER_JULIAN_YEAR * BARREL_TO_M3

# (eia_variable, eia_unit_in_meadow, output_column, factor_to_target_unit)
EIA_INDICATORS: list[tuple[str, str, str, float]] = [
    # Coal — output in tonnes.
    ("Coal production", "1000 metric tons", "coal_production", 1_000),
    ("Coal consumption", "1000 metric tons", "coal_consumption", 1_000),
    ("Coal imports", "1000 metric tons", "coal_imports", 1_000),
    ("Coal exports", "1000 metric tons", "coal_exports", 1_000),
    ("Coal reserves", "million short tons", "coal_reserves", 1_000_000 * SHORT_TON_TO_TONNE),
    # Natural gas — output in cubic metres.
    ("Dry natural gas production", "billion cubic meters", "natural_gas_production", 1e9),
    ("Dry natural gas consumption", "billion cubic meters", "natural_gas_consumption", 1e9),
    ("Dry natural gas imports", "billion cubic meters", "natural_gas_imports", 1e9),
    ("Dry natural gas exports", "billion cubic meters", "natural_gas_exports", 1e9),
    # Oil / petroleum — output in m³ per year (raw is kb/d, an average daily rate).
    ("Petroleum and other liquids production", "thousand barrels per day", "oil_production", KBPD_TO_M3_PER_YEAR),
    ("Refined petroleum products consumption", "thousand barrels per day", "oil_consumption", KBPD_TO_M3_PER_YEAR),
    ("Crude oil including lease condensate imports", "thousand barrels per day", "oil_imports", KBPD_TO_M3_PER_YEAR),
    ("Crude oil including lease condensate exports", "thousand barrels per day", "oil_exports", KBPD_TO_M3_PER_YEAR),
]

# Statistical Review reserves: (sr_column, output_column, factor_to_target_unit)
SR_RESERVES: list[tuple[str, str, float]] = [
    # Trillion m³ → m³.
    ("gas_reserves_tcm", "natural_gas_reserves", 1e12),
    # Billion barrels → m³.
    ("oil_reserves_bbl", "oil_reserves", 1e9 * BARREL_TO_M3),
]

# Net-imports indicators: (output_column, imports_column, exports_column).
NET_IMPORTS = [
    ("coal_net_imports", "coal_imports", "coal_exports"),
    ("natural_gas_net_imports", "natural_gas_imports", "natural_gas_exports"),
    ("oil_net_imports", "oil_imports", "oil_exports"),
]

# Trade-share indicators: (output_column, numerator_column, denominator_column).
SHARE_INDICATORS = [
    ("share_of_coal_consumption_imported", "coal_imports", "coal_consumption"),
    ("share_of_natural_gas_consumption_imported", "natural_gas_imports", "natural_gas_consumption"),
    ("share_of_oil_consumption_imported", "oil_imports", "oil_consumption"),
    ("share_of_coal_production_exported", "coal_exports", "coal_production"),
    ("share_of_natural_gas_production_exported", "natural_gas_exports", "natural_gas_production"),
    ("share_of_oil_production_exported", "oil_exports", "oil_production"),
]

# Columns that should be summed when computing regional aggregates (everything else is intensive
# or derived later).
EXTENSIVE_COLUMNS = [name for *_, name, _ in EIA_INDICATORS] + [name for _, name, _ in SR_RESERVES]

# OWID region aggregates to add. We deliberately exclude "World" so EIA's own World totals are
# kept (they include estimates for missing-country gaps that a plain country sum would miss).
REGIONS = {
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    "Low-income countries": {},
    "Lower-middle-income countries": {},
    "Upper-middle-income countries": {},
    "High-income countries": {},
}

# Same Aruba/Netherlands Antilles overlap as the EIA international_energy garden.
KNOWN_OVERLAPS = [{year: {"Aruba", "Netherlands Antilles"} for year in range(1986, 2030)}]

# After harmonization, EIA's continental aggregates are renamed with a "(EIA)" suffix; we drop
# them here to avoid two parallel sets of regions in the explorer (OWID's continents are added
# below from country-level data).
EIA_REGION_SUFFIX = " (EIA)"


def prepare_eia_data(tb_meadow: Table) -> Table:
    """Filter the EIA meadow long-format table to the (variable, unit) pairs we need, pivot wide,
    and apply unit conversions.
    """
    selected_pairs = {(var, unit) for var, unit, *_ in EIA_INDICATORS}
    keep = [(v, u) in selected_pairs for v, u in zip(tb_meadow["variable"], tb_meadow["unit"])]
    tb = tb_meadow[keep][["country", "year", "variable", "unit", "value"]].copy()

    # Apply unit conversions per (variable, unit) pair.
    pair_to_target = {(var, unit): (name, factor) for var, unit, name, factor in EIA_INDICATORS}
    factors = [pair_to_target[(v, u)][1] for v, u in zip(tb["variable"], tb["unit"])]
    tb["value"] = tb["value"] * factors
    tb["indicator"] = [pair_to_target[(v, u)][0] for v, u in zip(tb["variable"], tb["unit"])]
    tb = tb.drop(columns=["variable", "unit"])

    # Pivot to wide.
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()
    tb.columns.name = None
    return tb


def prepare_statistical_review_reserves(tb_review: Table) -> Table:
    """Pick the reserves columns we need from the Statistical Review and convert to physical units."""
    cols = ["country", "year"] + [src for src, *_ in SR_RESERVES]
    tb = tb_review.reset_index()[cols].copy()
    for src, target, factor in SR_RESERVES:
        tb[target] = tb[src] * factor
    tb = tb.drop(columns=[src for src, *_ in SR_RESERVES])
    return tb


def add_derived_columns(tb: Table) -> Table:
    """Add net-imports, import/export shares, and per-capita variants.

    Run after region aggregation so these apply to both countries and regions.
    """
    # Net imports = imports − exports.
    for target, imports_col, exports_col in NET_IMPORTS:
        tb[target] = tb[imports_col] - tb[exports_col]

    # Trade shares (in %), capped at 100 because supply mismatches in raw data sometimes push
    # the ratio above unity.
    for target, num, den in SHARE_INDICATORS:
        tb[target] = (tb[num] / tb[den]).clip(upper=1) * 100

    # Per-capita variants for every extensive column plus net imports.
    per_capita_inputs = EXTENSIVE_COLUMNS + [name for name, *_ in NET_IMPORTS]
    for col in per_capita_inputs:
        tb[f"{col}_per_capita"] = tb[col] / tb["population"]

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    ds_eia = paths.load_dataset("international_energy")
    tb_eia_meadow = ds_eia.read("international_energy", safe_types=False)

    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy")

    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Wide EIA table in physical units, with EIA-style country names.
    tb_eia = prepare_eia_data(tb_eia_meadow)

    # Harmonize EIA country names using the local mapping (same one the EIA garden uses); after
    # this, EIA's continental aggregates are tagged with " (EIA)", which we drop so they don't
    # double up with the OWID-computed continents added below.
    tb_eia = paths.regions.harmonize_names(tb=tb_eia)
    tb_eia = tb_eia[~tb_eia["country"].astype(str).str.endswith(EIA_REGION_SUFFIX)].reset_index(drop=True)

    # Reserves from Statistical Review (already harmonized, country-level + OWID regions).
    tb_reserves = prepare_statistical_review_reserves(tb_review)

    # Combine the two sources on (country, year).
    tb = pr.merge(tb_eia, tb_reserves, on=["country", "year"], how="outer", short_name=paths.short_name)

    # Add OWID region aggregates (continents + income groups) by summing extensive columns from
    # countries. EIA's own "World" row is preserved because we exclude World from REGIONS.
    aggregations = {col: "sum" for col in EXTENSIVE_COLUMNS if col in tb.columns}
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        aggregations=aggregations,
        min_num_values_per_year=1,
        ignore_overlaps_of_zeros=True,
        accepted_overlaps=KNOWN_OVERLAPS,
    )

    # Population for per-capita calculations.
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    # Net imports, trade shares, per-capita variants.
    tb = add_derived_columns(tb)

    # Set an appropriate index and sort.
    tb = tb.format(keys=["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
