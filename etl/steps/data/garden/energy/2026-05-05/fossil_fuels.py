"""Garden step for the OWID Fossil Fuels dataset, used by the natural-resources explorer.

Reads the curated EIA International Energy garden (which itself merges the live EIA bulk file
with a frozen 2022 archive of EIA's old international-tool tables for indicators EIA has
since retired — natural gas reserves and oil reserves) and converts to the physical units
the explorer expects (tonnes for coal, cubic metres for gas and oil). Adds OWID region
aggregates, then derives net imports, import/export shares, and per-capita variants.
"""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Conversion factors (sourced from the original natural_resources importer).
BARREL_TO_M3 = 0.1589873  # 1 barrel = 0.1589873 m³.
DAYS_PER_JULIAN_YEAR = 365.25
# 1 thousand barrels per day → m³ per (Julian) year.
KBPD_TO_M3_PER_YEAR = 1000 * DAYS_PER_JULIAN_YEAR * BARREL_TO_M3
# 1 thousand barrels per day → m³ per (average Julian) month.
KBPD_TO_M3_PER_MONTH = KBPD_TO_M3_PER_YEAR / 12
# Mass / volume scale-ups.
MT_TO_TONNES = 1_000_000  # million tonnes → tonnes.
BCM_TO_M3 = 1e9  # billion cubic metres → cubic metres.
TCM_TO_M3 = 1e12  # trillion cubic metres → cubic metres.
GBBL_TO_M3 = 1e9 * BARREL_TO_M3  # billion barrels → cubic metres.

# (international_energy_column, output_column, factor_to_target_unit)
FROM_INTERNATIONAL_ENERGY: list[tuple[str, str, float]] = [
    # Coal — Mt → tonnes.
    ("coal_production_mt", "coal_production", MT_TO_TONNES),
    ("coal_consumption_mt", "coal_consumption", MT_TO_TONNES),
    ("coal_imports_mt", "coal_imports", MT_TO_TONNES),
    ("coal_exports_mt", "coal_exports", MT_TO_TONNES),
    ("coal_reserves", "coal_reserves", MT_TO_TONNES),
    # Natural gas — bcm → m³, gas reserves TCM → m³.
    ("natural_gas_production", "natural_gas_production", BCM_TO_M3),
    ("natural_gas_consumption", "natural_gas_consumption", BCM_TO_M3),
    ("natural_gas_imports", "natural_gas_imports", BCM_TO_M3),
    ("natural_gas_exports", "natural_gas_exports", BCM_TO_M3),
    ("natural_gas_reserves", "natural_gas_reserves", TCM_TO_M3),
    # Oil — kb/d → m³/year, oil reserves Gbbl → m³.
    ("petroleum_production", "oil_production", KBPD_TO_M3_PER_YEAR),
    ("petroleum_consumption", "oil_consumption", KBPD_TO_M3_PER_YEAR),
    ("crude_oil_imports", "oil_imports", KBPD_TO_M3_PER_YEAR),
    ("crude_oil_exports", "oil_exports", KBPD_TO_M3_PER_YEAR),
    ("oil_reserves", "oil_reserves", GBBL_TO_M3),
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
EXTENSIVE_COLUMNS = [out for _, out, _ in FROM_INTERNATIONAL_ENERGY]

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
# NOTE: The year range must exactly match the years where both entities have data; extra years
# trigger an "overlaps not found" warning because the function compares the full year-set dict.
KNOWN_OVERLAPS = [{year: {"Aruba", "Netherlands Antilles"} for year in range(1986, 2025)}]


def select_and_convert(tb_input: Table) -> Table:
    """Pick the columns we need from international_energy and convert each to physical units."""
    keep = ["country", "year"] + [src for src, _, _ in FROM_INTERNATIONAL_ENERGY if src in tb_input.columns]
    tb = tb_input[keep].copy()
    for src, target, factor in FROM_INTERNATIONAL_ENERGY:
        if src in tb.columns:
            tb[target] = tb[src] * factor
    tb = tb[["country", "year"] + [t for _, t, _ in FROM_INTERNATIONAL_ENERGY if t in tb.columns]]
    return tb


def add_derived_columns(tb: Table) -> Table:
    """Add net imports, import/export shares, and per-capita variants.

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


def build_monthly_table(tb_eia_monthly: Table, ds_population) -> Table:
    """Build the monthly companion table.

    Converts the kb/d crude-oil-production-monthly column from international_energy_monthly
    into m³ per month, then adds a per-capita variant by joining annual population on the year
    of each date. Country-level only (no OWID region aggregates).
    """
    tb = tb_eia_monthly.reset_index()[["country", "date", "crude_oil_production_monthly"]].copy()
    tb["oil_production_monthly"] = tb["crude_oil_production_monthly"] * KBPD_TO_M3_PER_MONTH
    tb = tb.drop(columns=["crude_oil_production_monthly"])

    # Add per-capita: pull annual population, join on (country, year-of-date).
    tb["year"] = tb["date"].dt.year
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["oil_production_monthly_per_capita"] = tb["oil_production_monthly"] / tb["population"]
    tb = tb.drop(columns=["year", "population"])

    return tb.format(keys=["country", "date"], short_name=f"{paths.short_name}_monthly")


def run() -> None:
    #
    # Load inputs.
    #
    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy")
    tb_eia_monthly = ds_eia.read("international_energy_monthly")
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Pick the columns we need and convert to physical units.
    tb = select_and_convert(tb_eia)

    # Add OWID region aggregates by summing the extensive columns from country-level data. The
    # incoming table already contains EIA's regional aggregates (e.g. "Africa (EIA)") and OWID
    # continents computed by the upstream international_energy garden, but those continents were
    # computed in the upstream's energy units; recomputing here gives consistent physical-unit
    # totals for our explorer.
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

    # Build the monthly companion table.
    tb_monthly = build_monthly_table(tb_eia_monthly, ds_population)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_monthly])
    ds_garden.save()
