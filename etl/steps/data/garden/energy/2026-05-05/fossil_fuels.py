"""Garden step for the OWID Fossil Fuels data explorer."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Conversion factors, sourced from:
# https://www.eia.gov/state/seds/sep_prices/notes/pr_metric.pdf
BARREL_TO_M3 = 0.1589873
DAYS_PER_JULIAN_YEAR = 365.25
# 1 thousand barrels per day to m³ per (Julian) year.
KBPD_TO_M3_PER_YEAR = 1000 * DAYS_PER_JULIAN_YEAR * BARREL_TO_M3
# 1 thousand barrels per day to m³ per (average Julian) month.
KBPD_TO_M3_PER_MONTH = KBPD_TO_M3_PER_YEAR / 12
# Mass / volume scale-ups.
MT_TO_TONNES = 1_000_000  # million tonnes to tonnes.
BCM_TO_M3 = 1e9  # billion cubic metres to cubic metres.
TCM_TO_M3 = 1e12  # trillion cubic metres to cubic metres.
GBBL_TO_M3 = 1e9 * BARREL_TO_M3  # billion barrels to cubic metres.

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
    ("crude_oil_production", "oil_production", KBPD_TO_M3_PER_YEAR),
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

# Columns derived by linear unit conversion from upstream extensive columns.
EXTENSIVE_COLUMNS = [out for _, out, _ in FROM_INTERNATIONAL_ENERGY]


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
    """Add net imports + import/export shares.

    Per-capita variants are added separately via ``paths.regions.add_per_capita``.
    """
    # Net imports = imports − exports.
    for target, imports_col, exports_col in NET_IMPORTS:
        tb[target] = tb[imports_col] - tb[exports_col]

    # Trade shares (in %), capped at 100 because supply mismatches in raw data sometimes push
    # the ratio above unity.
    for target, num, den in SHARE_INDICATORS:
        tb[target] = (tb[num] / tb[den]).clip(upper=1) * 100

    return tb


def build_monthly_table(tb_eia_monthly: Table) -> Table:
    """Build the monthly companion table.

    Converts the kb/d crude-oil-production-monthly column from international_energy_monthly
    into m³ per month, then adds a per-capita variant by joining annual population on the year
    of each date. Country-level only (no OWID region aggregates).
    """
    tb = tb_eia_monthly.reset_index()[["country", "date", "crude_oil_production_monthly"]].copy()
    tb["oil_production_monthly"] = tb["crude_oil_production_monthly"] * KBPD_TO_M3_PER_MONTH
    tb = tb.drop(columns=["crude_oil_production_monthly"])

    # add_per_capita expects a year column to look up annual population, so derive year from date.
    tb["year"] = tb["date"].dt.year
    tb = paths.regions.add_per_capita(tb=tb, columns=["oil_production_monthly"], warn_on_missing_countries=False)
    tb = tb.drop(columns=["year"])

    return tb.format(keys=["country", "date"], short_name=f"{paths.short_name}_monthly")


def run() -> None:
    #
    # Load inputs.
    #
    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy")
    tb_eia_monthly = ds_eia.read("international_energy_monthly")

    #
    # Process data.
    #
    # Pick the columns we need and convert to physical units.
    tb = select_and_convert(tb_eia)

    # Net imports + trade shares.
    tb = add_derived_columns(tb)

    # Per-capita variants for all extensive + net-imports columns; this also joins population.
    per_capita_inputs = EXTENSIVE_COLUMNS + [name for name, *_ in NET_IMPORTS]
    tb = paths.regions.add_per_capita(tb=tb, columns=per_capita_inputs, warn_on_missing_countries=False)

    # Set an appropriate index and sort.
    tb = tb.format(keys=["country", "year"], short_name=paths.short_name)

    # Build the monthly companion table.
    tb_monthly = build_monthly_table(tb_eia_monthly)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_monthly])
    ds_garden.save()
