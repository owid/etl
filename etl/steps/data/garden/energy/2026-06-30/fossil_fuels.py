"""Garden step for fossil fuels: production, trade, consumption, and reserves.

Coal, oil and gas production come from (in priority order):
- Energy Institute Statistical Review of World Energy (from 1965).
- Etemad & Luciani (1900-1979), for the historical fill before the Statistical Review.
- U.S. EIA International Energy (1949-2025), for country breadth and recent years not covered by the
  Statistical Review.
- Smil (2017), for World coal before 1900.
- NIC / Fouquet UK historical energy, for United Kingdom coal before 1900.

Etemad is prioritized over EIA for the pre-1965 fill so the historical series joins the Statistical
Review.

The dataset also includes:
- Consumption in energy units, combined from the same sources as production (Statistical Review,
  extended with EIA). For the World, consumption before 1965 is filled with production (the
  Statistical Review's notes attribute the small global differences between the two to stock changes
  and disparities in the definition, measurement or conversion of supply and demand data).
- Production in physical units, from EIA (broad country coverage).
- Proved reserves, trade (imports, exports, net imports), and consumption in physical units, from EIA.
- The World reserves-to-production ratio for each fossil fuel.
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from shared import EXCLUDED_PROVIDER_REGIONS

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Reserves/production ratio conversion factors (from the "Approximate conversion factors" sheet).
BILLION_BARRELS_TO_TONNES = 1e9 * 0.1364
MILLION_TONNES_TO_TONNES = 1e6
TRILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e12
BILLION_CUBIC_METERS_TO_CUBIC_METERS = 1e9
# A barrel is a volume unit, so barrels convert to cubic meters exactly (no density assumption).
# From the National Institute of Standards and Technology's Guide for the Use of the International System of Units (SI), page 45 of https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication811e2008.pdf
BARREL_TO_CUBIC_METERS = 0.1589873
BILLION_BARRELS_TO_CUBIC_METERS = 1e9 * BARREL_TO_CUBIC_METERS
# EIA reports oil trade and consumption in thousand barrels per day; convert to cubic meters per year.
KBPD_TO_CUBIC_METERS_PER_YEAR = 1000 * 365.25 * BARREL_TO_CUBIC_METERS

# Year from which the Statistical Review covers fossil fuel production.
STATISTICAL_REVIEW_FIRST_YEAR = 1965
# Year before which the historical (Smil / Fouquet) coal data is used.
HISTORICAL_LAST_YEAR = 1900

FUELS = ["coal", "oil", "gas"]


def prepare_statistical_review_data(tb_review: Table) -> Table:
    columns = (
        ["country", "year"]
        + [f"{fuel}_production_twh" for fuel in FUELS]
        + [f"{fuel}_consumption_twh" for fuel in FUELS]
    )
    tb = tb_review.reset_index()[columns]
    return tb


def prepare_etemad_data(tb_etemad: Table) -> Table:
    tb = tb_etemad.reset_index()[["country", "year", "coal_production_twh", "oil_production_twh", "gas_production_twh"]]
    return tb


def prepare_eia_data(tb_eia: Table) -> Table:
    columns = {
        "country": "country",
        "year": "year",
        "energy_production_from_coal": "coal_production_twh",
        "energy_production_from_petroleum": "oil_production_twh",
        "energy_production_from_natural_gas": "gas_production_twh",
        "energy_consumption_from_coal": "coal_consumption_twh",
        "energy_consumption_from_petroleum": "oil_consumption_twh",
        "energy_consumption_from_natural_gas": "gas_consumption_twh",
    }
    tb = tb_eia.reset_index()[list(columns)].rename(columns=columns, errors="raise")
    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix).
    tb = tb[~tb["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)
    # Drop EIA's USSR. Its figures are on a different basis from the two sources that bracket them:
    # for coal production it sits 31% above Etemad & Luciani's last year (1979) and 24% above the
    # Statistical Review where they overlap (1981-1984), with the same gap in consumption. Since the
    # Statistical Review only reports USSR production for those four years, keeping EIA would splice
    # three incompatible levels into one series and show jumps that are source seams, not history.
    # The USSR therefore ends where the Statistical Review ends it, in 1984; Russia starts in 1985 as
    # its own (smaller) entity, which is the Statistical Review's own convention.
    tb = tb[tb["country"] != "USSR"].reset_index(drop=True)
    return tb


def prepare_smil_data(tb_smil: Table) -> Table:
    # Smil only reports World coal (as direct energy, which equals production).
    tb = tb_smil.reset_index()[["country", "year", "coal__twh_direct_energy"]].rename(
        columns={"coal__twh_direct_energy": "coal_production_twh"}, errors="raise"
    )
    tb = tb.dropna(subset=["coal_production_twh"]).reset_index(drop=True)
    return tb


def prepare_uk_historical_data(tb_uk: Table) -> Table:
    tb = tb_uk.reset_index()[["country", "year", "coal_production_twh"]]
    return tb


def combine_production_data(tb_review: Table, tb_etemad: Table, tb_eia: Table, tb_historical: Table) -> Table:
    index_columns = ["country", "year"]
    for tb, name in [(tb_review, "Statistical Review"), (tb_etemad, "Etemad & Luciani"), (tb_eia, "EIA")]:
        assert not tb.duplicated(subset=index_columns).any(), f"Duplicated (country, year) rows in {name} data."

    # Combine by priority: Statistical Review > Etemad & Luciani > EIA.
    combined = combine_two_overlapping_dataframes(df1=tb_review, df2=tb_etemad, index_columns=index_columns)
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_eia, index_columns=index_columns)

    # Extend coal production before 1900 with the historical data (Smil for the World, Fouquet for the UK),
    # giving it the lowest priority.
    combined = combine_two_overlapping_dataframes(df1=combined, df2=tb_historical, index_columns=index_columns)

    # Remove rows that only have nans.
    value_columns = [f"{fuel}_{metric}_twh" for fuel in FUELS for metric in ("production", "consumption")]
    combined = combined.dropna(subset=value_columns, how="all")
    combined = combined.sort_values(index_columns).reset_index(drop=True)
    return combined


def backfill_world_consumption(tb: Table) -> Table:
    """Extend World consumption before 1965 with World production.

    Consumption data starts in 1965 (the Statistical Review's first year), but globally consumption
    closely tracks production (the Statistical Review's notes attribute the differences to stock
    changes and disparities in the definition, measurement or conversion of supply and demand data),
    so earlier years take the production series.
    Only the World is filled: for individual countries and regions, trade makes the two differ.
    """
    world = tb[tb["country"] == "World"]
    mask = (tb["country"] == "World") & (tb["year"] < STATISTICAL_REVIEW_FIRST_YEAR)
    for fuel in FUELS:
        first_year = world.dropna(subset=[f"{fuel}_consumption_twh"])["year"].min()
        assert first_year == STATISTICAL_REVIEW_FIRST_YEAR, (
            f"World {fuel} consumption starts in {first_year}, not {STATISTICAL_REVIEW_FIRST_YEAR}; "
            "reassess the pre-1965 backfill from production."
        )
        backfill = tb[f"{fuel}_production_twh"].copy()
        backfill.loc[~mask] = float("nan")
        # World production before 1965 comes only from Etemad & Luciani and (for coal before 1900)
        # Smil; keep only those origins so fillna doesn't merge production origins the backfilled
        # rows never use (e.g. the UK-only NIC / Fouquet feeder) into the consumption column.
        backfill.m.origins = [o for o in backfill.m.origins if o.producer in ("Etemad & Luciani", "Smil")]
        assert backfill.m.origins, f"Etemad & Luciani missing from the {fuel} production origins."
        tb[f"{fuel}_consumption_twh"] = tb[f"{fuel}_consumption_twh"].fillna(backfill)
    return tb


def add_annual_change(tb: Table) -> Table:
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    # Only consider changes between consecutive years (the World coal series is decadal before 1900).
    is_consecutive = tb.groupby("country", observed=True)["year"].diff() == 1
    for fuel in FUELS:
        pct_change = tb.groupby("country", observed=True)[f"{fuel}_production_twh"].pct_change(fill_method=None) * 100
        abs_change = tb.groupby("country", observed=True)[f"{fuel}_production_twh"].diff()
        tb[f"{fuel}_production_annual_change_pct"] = pct_change.where(is_consecutive)
        tb[f"{fuel}_production_annual_change_twh"] = abs_change.where(is_consecutive)
    return tb


def add_physical_production(tb: Table, tb_eia: Table) -> Table:
    """Add fossil fuel production in physical units, from EIA.

    Coal in tonnes, oil and gas in cubic meters. EIA reports coal in million tonnes, gas in billion
    cubic meters, and crude oil in thousand barrels per day; all are converted to base units here.
    These units differ by fuel and cannot be summed across fuels, so there is no cross-fuel total. EIA is
    preferred over the Statistical Review here, as it covers significantly more producing countries.
    """
    tb_eia = tb_eia.reset_index()[
        ["country", "year", "coal_production_mt", "natural_gas_production", "crude_oil_production"]
    ]
    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix).
    tb_eia = tb_eia[~tb_eia["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)
    tb_eia["coal_production_tonnes"] = tb_eia["coal_production_mt"] * MILLION_TONNES_TO_TONNES
    tb_eia["gas_production_m3"] = tb_eia["natural_gas_production"] * BILLION_CUBIC_METERS_TO_CUBIC_METERS
    tb_eia["oil_production_m3"] = tb_eia["crude_oil_production"] * KBPD_TO_CUBIC_METERS_PER_YEAR
    tb_eia = tb_eia.drop(
        columns=["coal_production_mt", "natural_gas_production", "crude_oil_production"], errors="raise"
    )
    tb_eia = tb_eia.dropna(subset=["coal_production_tonnes", "gas_production_m3", "oil_production_m3"], how="all")
    tb = tb.merge(tb_eia, on=["country", "year"], how="outer")
    return tb


def add_reserves(tb: Table, tb_eia: Table) -> Table:
    """Add fossil fuel proved reserves in physical units, from EIA.

    Coal in tonnes, oil and gas in cubic meters (oil is reported in billion barrels and converted
    exactly, since barrels are a volume unit). Reserves are stored in base units.
    EIA is preferred over the Statistical Review for reserves, as it covers significantly more countries,
    extends further, and matches the numbers previously published in the fossil fuels explorer.
    """
    columns = {
        "coal_reserves": "coal_reserves_mt",
        "oil_reserves": "oil_reserves_bbl",
        "natural_gas_reserves": "gas_reserves_tcm",
    }
    tb_eia = tb_eia.reset_index()[["country", "year"] + list(columns)].rename(columns=columns, errors="raise")
    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix).
    tb_eia = tb_eia[~tb_eia["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)
    tb_eia["coal_reserves_tonnes"] = tb_eia["coal_reserves_mt"] * MILLION_TONNES_TO_TONNES
    tb_eia["oil_reserves_m3"] = tb_eia["oil_reserves_bbl"] * BILLION_BARRELS_TO_CUBIC_METERS
    tb_eia["gas_reserves_m3"] = tb_eia["gas_reserves_tcm"] * TRILLION_CUBIC_METERS_TO_CUBIC_METERS
    tb_eia = tb_eia.drop(columns=["coal_reserves_mt", "oil_reserves_bbl", "gas_reserves_tcm"], errors="raise")
    tb_eia = tb_eia.dropna(subset=[c for c in tb_eia.columns if c not in ["country", "year"]], how="all")
    tb = tb.merge(tb_eia, on=["country", "year"], how="outer")
    return tb


# EIA columns with trade and consumption in physical units, mapped to output columns in base units.
# EIA reports coal in million tonnes, gas in billion cubic meters, and oil in thousand barrels per
# day; all are converted to base units below so grapher applies magnitude prefixes itself.
EIA_TRADE_AND_CONSUMPTION_COLUMNS = {
    "coal_consumption_mt": "coal_consumption_tonnes",
    "coal_imports_mt": "coal_imports_tonnes",
    "coal_exports_mt": "coal_exports_tonnes",
    "natural_gas_consumption": "gas_consumption_m3",
    "natural_gas_imports": "gas_imports_m3",
    "natural_gas_exports": "gas_exports_m3",
    "petroleum_consumption": "oil_consumption_m3",
    "crude_oil_imports": "oil_imports_m3",
    "crude_oil_exports": "oil_exports_m3",
}
# Per-fuel column suffix (base unit) of the trade and consumption columns.
TRADE_SUFFIXES = {"coal": "tonnes", "gas": "m3", "oil": "m3"}
# Factor from each fuel's EIA-native unit to the base unit named in TRADE_SUFFIXES.
TRADE_CONVERSION_FACTORS = {
    "coal": MILLION_TONNES_TO_TONNES,
    "gas": BILLION_CUBIC_METERS_TO_CUBIC_METERS,
    "oil": KBPD_TO_CUBIC_METERS_PER_YEAR,
}


def add_trade_and_consumption(tb: Table, tb_eia: Table) -> Table:
    """Add trade (imports, exports, net imports) and consumption in physical units, from EIA.

    Coal in tonnes, oil and gas in cubic meters per year. Oil consumption covers all refined
    petroleum products, while oil trade covers crude oil (including lease condensate).
    """
    tb_eia = tb_eia.reset_index()[["country", "year"] + list(EIA_TRADE_AND_CONSUMPTION_COLUMNS)].rename(
        columns=EIA_TRADE_AND_CONSUMPTION_COLUMNS, errors="raise"
    )
    # Drop EIA's own regional aggregates (marked with an "(EIA)" suffix).
    tb_eia = tb_eia[~tb_eia["country"].str.contains("(EIA)", regex=False)].reset_index(drop=True)
    # Convert consumption, imports and exports to base units (net imports are derived after, so they
    # inherit the base unit automatically).
    for fuel, suffix in TRADE_SUFFIXES.items():
        for metric in ["consumption", "imports", "exports"]:
            tb_eia[f"{fuel}_{metric}_{suffix}"] *= TRADE_CONVERSION_FACTORS[fuel]
    # Net imports = imports - exports.
    for fuel, suffix in TRADE_SUFFIXES.items():
        tb_eia[f"{fuel}_net_imports_{suffix}"] = tb_eia[f"{fuel}_imports_{suffix}"] - tb_eia[f"{fuel}_exports_{suffix}"]
    tb_eia = tb_eia.dropna(subset=[c for c in tb_eia.columns if c not in ["country", "year"]], how="all")
    # Outer merge, so countries with trade data but no production of their own are kept.
    tb = tb.merge(tb_eia, on=["country", "year"], how="outer")
    return tb


def add_per_capita(tb: Table) -> Table:
    # Antarctica has EIA energy data (research stations) but no population; its per-capita values
    # are legitimately empty.
    expected_countries_without_population = [
        country for country in tb["country"].unique() if ("(EI)" in country) or ("(EIA)" in country)
    ] + ["Antarctica"]
    tb = paths.regions.add_population(
        tb=tb,
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=expected_countries_without_population,
    )
    for fuel in FUELS:
        tb[f"{fuel}_production_per_capita_kwh"] = tb[f"{fuel}_production_twh"] / tb["population"] * TWH_TO_KWH
        tb[f"{fuel}_consumption_per_capita_kwh"] = tb[f"{fuel}_consumption_twh"] / tb["population"] * TWH_TO_KWH
    # Per-capita production in physical units (EIA): coal in tonnes, oil and gas in cubic meters.
    # Totals are already in base units, so per capita is just total over population.
    tb["coal_production_per_capita_tonnes"] = tb["coal_production_tonnes"] / tb["population"]
    tb["oil_production_per_capita_m3"] = tb["oil_production_m3"] / tb["population"]
    tb["gas_production_per_capita_m3"] = tb["gas_production_m3"] / tb["population"]
    # Per-capita trade and consumption: coal in tonnes, oil and gas in cubic meters per person.
    for metric in ["consumption", "imports", "exports", "net_imports"]:
        tb[f"coal_{metric}_per_capita_tonnes"] = tb[f"coal_{metric}_tonnes"] / tb["population"]
        tb[f"gas_{metric}_per_capita_m3"] = tb[f"gas_{metric}_m3"] / tb["population"]
        tb[f"oil_{metric}_per_capita_m3"] = tb[f"oil_{metric}_m3"] / tb["population"]
    # Per-capita reserves: coal in tonnes, oil and gas in cubic meters per person.
    tb["coal_reserves_per_capita_tonnes"] = tb["coal_reserves_tonnes"] / tb["population"]
    tb["oil_reserves_per_capita_m3"] = tb["oil_reserves_m3"] / tb["population"]
    tb["gas_reserves_per_capita_m3"] = tb["gas_reserves_m3"] / tb["population"]
    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def add_reserves_to_production_ratio(tb: Table, tb_review: Table) -> Table:
    """Add World reserves-to-production ratios (years of fossil fuels left), from the Statistical Review."""
    tb_review = tb_review.reset_index()
    columns = [
        "coal_reserves_mt",
        "coal_production_mt",
        "oil_reserves_bbl",
        "oil_production_mt",
        "gas_reserves_m3",
        "gas_production_bcm",
    ]
    world = tb_review[tb_review["country"] == "World"][["country", "year"] + columns].copy()

    # Convert reserves and production to common units.
    world["coal_reserves"] = world["coal_reserves_mt"] * MILLION_TONNES_TO_TONNES
    world["coal_production"] = world["coal_production_mt"] * MILLION_TONNES_TO_TONNES
    world["oil_reserves"] = world["oil_reserves_bbl"] * BILLION_BARRELS_TO_TONNES
    world["oil_production"] = world["oil_production_mt"] * MILLION_TONNES_TO_TONNES
    world["gas_reserves"] = world["gas_reserves_m3"]
    world["gas_production"] = world["gas_production_bcm"] * BILLION_CUBIC_METERS_TO_CUBIC_METERS

    # Reserves-to-production ratio (years of fossil fuels left at current production).
    for fuel in FUELS:
        world[f"{fuel}_reserves_to_production_ratio"] = world[f"{fuel}_reserves"] / world[f"{fuel}_production"]

    world = world[["country", "year"] + [f"{fuel}_reserves_to_production_ratio" for fuel in FUELS]]

    # Merge the World ratio columns into the main table.
    tb = tb.merge(world, on=["country", "year"], how="left")
    return tb


def add_total_fossil_fuels(tb: Table) -> Table:
    """Add total fossil fuel production and consumption (coal + oil + gas), absolute and per capita."""
    for suffix in ["production_twh", "production_per_capita_kwh", "consumption_twh", "consumption_per_capita_kwh"]:
        # Table.sum(axis=1) preserves the columns' metadata/origins. min_count=1 keeps NaN only where
        # every fuel is missing (a coal-only producer's total is just its coal production).
        tb[f"total_{suffix}"] = tb[[f"{fuel}_{suffix}" for fuel in FUELS]].sum(axis=1, min_count=1)
    return tb


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    for fuel in FUELS:
        assert (tb[f"{fuel}_production_twh"].dropna() >= 0).all(), f"Negative {fuel} production found."
        assert (tb[f"{fuel}_consumption_twh"].dropna() >= 0).all(), f"Negative {fuel} consumption found."
    # Physical-unit production, reserves, trade and consumption must be non-negative too (net imports
    # are legitimately negative for net exporters, so they are not checked).
    for column in [
        "coal_production_tonnes",
        "oil_production_m3",
        "gas_production_m3",
        "coal_reserves_tonnes",
        "oil_reserves_m3",
        "gas_reserves_m3",
    ] + [
        f"{fuel}_{metric}_{suffix}"
        for fuel, suffix in TRADE_SUFFIXES.items()
        for metric in ["consumption", "imports", "exports"]
    ]:
        assert (tb[column].dropna() >= 0).all(), f"Negative {column} found."

    # Guard the kb/d -> cubic meters per year conversion for oil: US petroleum consumption has been
    # roughly 1.1-1.3 billion cubic meters per year for the last decades.
    us_oil = tb[(tb["country"] == "United States") & (tb["year"] == 2019)]["oil_consumption_m3"]
    assert 9e8 < us_oil.item() < 1.5e9, "US oil consumption outside the expected range; check the kb/d conversion."

    # Coal coverage after extending with historical data: World from 1800, United Kingdom from 1700.
    coal = tb.dropna(subset=["coal_production_twh"])
    assert coal[coal["country"] == "World"]["year"].min() == 1800, "World coal coverage changed."
    assert coal[coal["country"] == "United Kingdom"]["year"].min() == 1700, "UK coal coverage changed."

    # Continuity across the pre-1900 -> modern splice for the UK (which has annual coal data around 1900).
    uk_coal = coal[coal["country"] == "United Kingdom"].set_index("year")["coal_production_twh"]
    rel_diff = abs(uk_coal.loc[1900] - uk_coal.loc[1899]) / uk_coal.loc[1900]
    assert rel_diff < 0.10, f"Discontinuity in UK coal production at the 1899->1900 splice ({rel_diff:.0%})."

    # World consumption is backfilled with production before 1965, so its coverage mirrors production.
    world = tb[tb["country"] == "World"]
    for fuel, first_year in [("coal", 1800), ("oil", 1900), ("gas", 1900)]:
        assert world.dropna(subset=[f"{fuel}_consumption_twh"])["year"].min() == first_year, (
            f"World {fuel} consumption coverage changed."
        )
    # Continuity at the 1965 handover from production to Statistical Review consumption. Gas gets a
    # looser tolerance: production includes gas that was flared rather than consumed (historically a
    # significant share), so the two series sit on different levels around the seam.
    world_1965 = world[world["year"] == STATISTICAL_REVIEW_FIRST_YEAR]
    for fuel, tolerance in [("coal", 0.05), ("oil", 0.05), ("gas", 0.20)]:
        production = world_1965[f"{fuel}_production_twh"].item()
        consumption = world_1965[f"{fuel}_consumption_twh"].item()
        rel_diff = abs(consumption - production) / production
        assert rel_diff < tolerance, f"Discontinuity in World {fuel} consumption at the 1965 splice ({rel_diff:.0%})."


def run() -> None:
    #
    # Load data.
    #
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    ds_etemad = paths.load_dataset("etemad_luciani")
    tb_etemad = ds_etemad.read("etemad_luciani", reset_index=False)

    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy", reset_index=False)

    ds_smil = paths.load_dataset("smil_2017")
    tb_smil = ds_smil.read("smil_2017")

    ds_uk = paths.load_dataset("uk_historical_energy")
    tb_uk = ds_uk.read("uk_historical_energy")

    #
    # Process data.
    #
    tb_review_prod = prepare_statistical_review_data(tb_review=tb_review)
    tb_etemad = prepare_etemad_data(tb_etemad=tb_etemad)
    tb_eia_prod = prepare_eia_data(tb_eia=tb_eia)

    # Historical coal (Smil for the World, Fouquet for the UK), restricted to before 1900.
    tb_smil = prepare_smil_data(tb_smil=tb_smil)
    tb_uk = prepare_uk_historical_data(tb_uk=tb_uk)
    tb_historical = pr.concat([tb_smil, tb_uk], ignore_index=True)
    tb_historical = tb_historical[tb_historical["year"] < HISTORICAL_LAST_YEAR].reset_index(drop=True)

    # Combine all production sources.
    tb = combine_production_data(
        tb_review=tb_review_prod, tb_etemad=tb_etemad, tb_eia=tb_eia_prod, tb_historical=tb_historical
    )

    # Extend World consumption before 1965 with World production.
    tb = backfill_world_consumption(tb=tb)

    # Add annual change, physical-unit production, reserves, trade and consumption (all from EIA),
    # and per-capita.
    tb = add_annual_change(tb=tb)
    tb = add_physical_production(tb=tb, tb_eia=tb_eia)
    tb = add_reserves(tb=tb, tb_eia=tb_eia)
    tb = add_trade_and_consumption(tb=tb, tb_eia=tb_eia)
    tb = add_per_capita(tb=tb)

    # Add total fossil fuel production (coal + oil + gas), absolute and per capita.
    tb = add_total_fossil_fuels(tb=tb)

    # Add World reserves-to-production ratios.
    tb = add_reserves_to_production_ratio(tb=tb, tb_review=tb_review)

    # Remove residual and undefined provider regions (kept in the Statistical Review garden as
    # aggregation inputs, but meaningless to readers).
    tb = tb[~tb["country"].isin(EXCLUDED_PROVIDER_REGIONS)].reset_index(drop=True)

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Combined multi-source indicators must not inherit key points or producer text from a single
    # input (e.g. the UK feeder's interpolation notes, or EI's per-fuel footnotes on series that also
    # blend Etemad, Smil, NIC and EIA). Key points come only from this step's own meta.yml; producer
    # text is cleared on the blended energy-unit columns and kept on single-source physical ones.
    for column in tb.columns:
        tb[column].m.description_key = []
        if "twh" in column or "kwh" in column:
            tb[column].m.description_from_producer = None

    # Format table conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
