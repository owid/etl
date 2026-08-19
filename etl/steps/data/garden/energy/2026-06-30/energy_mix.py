"""Energy mix: Total Energy Supply (TES) by source, from the Energy Institute's Statistical Review.

For each source and aggregate it reports TES in absolute terms, per capita, as a share of the total,
and as annual change. The World series is extended back to 1800 with Smil (2017), and a per-GDP
variable is added (Maddison). Traditional biomass (Smil, World only) is kept separate from TES.

Countries the Statistical Review does not report are covered with EIA data, source by source, and
their total is the sum of those sources. EIA's own total is not used: it counts electricity consumed
rather than generated and leaves geothermal and biomass uninflated, so it measures something else
(for Iceland it lands 62% below the Statistical Review's). The OWID region aggregates are then built
from the combined countries, over the years EIA covers.
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

# Region aggregates, rebuilt here from the combined data. The Statistical Review's own are incomplete,
# since it cannot attribute part of each region to any country.
CONTINENTS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
]

# Entities EIA reports whose territory the Statistical Review covers at the same time under another name:
# East and West Germany against Germany over 1980-1990, and Czechoslovakia against Czechia and Slovakia
# over 1980-1992. Their own series are kept, but they are left out of the region aggregates, where they
# would count the same territory twice.
DUPLICATED_TERRITORIES = ["East Germany", "West Germany", "Czechoslovakia"]

REGIONS = CONTINENTS + [
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
]

# Years each region's aggregate covers, which is EIA's own range (see add_region_aggregates). Asserted in
# sanity_check_outputs so that a change has to be acknowledged here rather than found on a chart.
EXPECTED_REGION_YEARS: dict[str, tuple[int, int]] = {region: (1980, 2024) for region in REGIONS}

# EIA columns measuring the same quantity as each Statistical Review source, in the same units. Other
# renewables is the estimate the EIA garden step builds from geothermal, biomass and tide generation.
EIA_SOURCES = {
    "coal": "energy_consumption_from_coal",
    "oil": "energy_consumption_from_petroleum",
    "gas": "energy_consumption_from_natural_gas",
    "nuclear": "energy_consumption_from_nuclear",
    "hydro": "electricity_from_hydro",
    "solar": "electricity_from_solar",
    "wind": "electricity_from_wind",
    "other_renewables": "energy_consumption_from_other_renewables",
    "biofuels": "energy_consumption_from_biofuels",
}

# Tolerances for the reconciliations in sanity_check_outputs.
# Every region family must add up to the Statistical Review's own World total, in every energy column
# (see sanity_check_outputs). A year has to breach both tolerances to fail. The relative one carries the
# check wherever the quantity is large; the absolute one is what makes it usable on a source that starts
# from almost nothing, where the producer leaves a few terawatt-hours unattributed to any country and a
# percentage would be enormous while the discrepancy is trivial. The largest relative deviation measured
# is 2.2% (renewables in 1988) and the largest absolute one 378 TWh (0.2% of the total, in 2005); the
# only column that needs the absolute allowance is biofuels, which peaks at 39.9% and 15 TWh in 1980.
MAX_WORLD_DEVIATION_PCT = 3
MAX_WORLD_DEVIATION_TWH = 20
MAX_SOURCES_DEVIATION_PCT = 2
MAX_SHARE_SUM_DEVIATION_PCT = 2

# Predecessor and successor entities that the sources report side by side without double-counting: EIA
# reports Aruba separately from 1986 while keeping the old name for the rest of the Netherlands Antilles,
# and the Statistical Review's Yugoslavia is zero in the two years it overlaps its successors.
ACCEPTED_OVERLAPS = [
    {year: {"Aruba", "Netherlands Antilles"} for year in range(1986, 2025)},
    *(
        {year: {"Yugoslavia", successor} for year in (1990, 1991)}
        for successor in ("Croatia", "North Macedonia", "Slovenia")
    ),
]

# Indicators that must not carry the producer's description, because it describes only one of the
# inputs that go into them (see where this is applied, at the end of run()).
COLUMNS_WITHOUT_PRODUCER_DESCRIPTION = [
    "low_carbon_energy_annual_change_pct",
    "low_carbon_energy_annual_change_twh",
    "low_carbon_energy_per_capita_kwh",
    "low_carbon_energy_twh",
    "total_energy_supply_per_gdp_kwh_per_dollar",
]

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

# Sources shown in the biomass-inclusive share chart: the individual TES sources plus traditional biomass.
BIOMASS_SHARE_SOURCES = {
    **{source: SOURCE_NAMES[source] for source in SR_SOURCES.values()},
    "traditional_biomass": "Traditional biomass",
}

# Columns that are legitimately empty for a region, so that any other gap in a region's series fails the
# check in sanity_check_outputs. Traditional biomass is a World-only series from Smil, and so are the
# shares measured against a denominator that includes it; Maddison reports no GDP for OWID regions.
COLUMNS_ALLOWED_TO_BE_EMPTY_FOR_REGIONS = {
    "traditional_biomass_twh",
    "total_energy_supply_per_gdp_kwh_per_dollar",
    *(f"{source}_share_including_biomass_pct" for source in BIOMASS_SHARE_SOURCES),
}

# Countries that enter only through the EIA total-energy extension (no Statistical Review by-source
# breakdown) and that are confirmed to have no nuclear power. The Statistical Review garden step already
# zero-fills nuclear for the no-nuclear countries it *does* cover; this list extends that to the
# EIA-only countries, so the nuclear map shows "No nuclear" for them instead of "No data".
COUNTRIES_WITHOUT_NUCLEAR = [
    "Afghanistan",
    "Albania",
    "American Samoa",
    "Antarctica",
    "Antigua and Barbuda",
    "Aruba",
    "Bahamas",
    "Barbados",
    "Belize",
    "Benin",
    "Bermuda",
    "Bhutan",
    "Bosnia and Herzegovina",
    "Botswana",
    "British Virgin Islands",
    "Burkina Faso",
    "Burundi",
    "Cambodia",
    "Cameroon",
    "Cape Verde",
    "Cayman Islands",
    "Central African Republic",
    "Comoros",
    "Cook Islands",
    "Costa Rica",
    "Cote d'Ivoire",
    "Djibouti",
    "Dominica",
    "Dominican Republic",
    "East Timor",
    "El Salvador",
    "Eritrea",
    "Eswatini",
    "Ethiopia",
    "Falkland Islands",
    "Faroe Islands",
    "Fiji",
    "French Guiana",
    "French Polynesia",
    "Gambia",
    "Georgia",
    "Ghana",
    "Gibraltar",
    "Greenland",
    "Grenada",
    "Guadeloupe",
    "Guam",
    "Guatemala",
    "Guinea",
    "Guinea-Bissau",
    "Haiti",
    "Honduras",
    "Jamaica",
    "Jordan",
    "Kenya",
    "Kiribati",
    "Kosovo",
    "Kyrgyzstan",
    "Laos",
    "Lebanon",
    "Lesotho",
    "Liberia",
    "Macao",
    "Malawi",
    "Maldives",
    "Mali",
    "Malta",
    "Martinique",
    "Mauritania",
    "Mauritius",
    "Micronesia (country)",
    "Moldova",
    "Montenegro",
    "Montserrat",
    "Namibia",
    "Nauru",
    "Nepal",
    "Netherlands Antilles",
    "Nicaragua",
    "Niger",
    "Niue",
    "North Korea",
    "Northern Mariana Islands",
    "Palestine",
    "Panama",
    "Paraguay",
    "Puerto Rico",
    "Reunion",
    "Rwanda",
    "Saint Helena",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Pierre and Miquelon",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "Sao Tome and Principe",
    "Senegal",
    "Serbia and Montenegro",
    "Seychelles",
    "Sierra Leone",
    "Solomon Islands",
    "Somalia",
    "Suriname",
    "Tajikistan",
    "Tanzania",
    "Togo",
    "Tonga",
    "Turks and Caicos Islands",
    "Tuvalu",
    "Uganda",
    "United States Virgin Islands",
    "Uruguay",
    "Vanuatu",
    "Western Sahara",
]

# EIA-only entities that DID operate nuclear power (Armenia's plant is still active; the others are
# historical states that ran reactors). These keep "No data" for nuclear rather than a misleading zero.
COUNTRIES_WITH_NUCLEAR_NO_BREAKDOWN = [
    "Armenia",
    "Czechoslovakia",
    "East Germany",
    "West Germany",
    "Yugoslavia",
]

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
    """Select the TES-by-source columns and the total, dropping the region aggregates (see REGIONS).

    The producer's own regions, the World and the European Union are kept: it reports them directly.
    """
    tb = tb_review[["country", "year", "total_energy_supply_twh"] + list(SR_SOURCES)]
    tb = tb.rename(columns={col: f"{name}_twh" for col, name in SR_SOURCES.items()}, errors="raise")
    tb = tb[~tb["country"].isin(REGIONS)].reset_index(drop=True)
    return tb


def combine_with_eia(tb: Table, tb_eia: Table) -> Table:
    """Extend the total and each source to the countries the Statistical Review does not report.

    It is prioritized per value, so no country's series mixes the two producers.

    EIA's own total is not used. It measures domestic consumption net of electricity trade and counts
    renewable electricity as generated rather than as heat input, so it is on a different basis from
    both its own by-source columns and the Statistical Review: Paraguay's hydro generation alone
    exceeds it, and Kenya's geothermal heat input is four times its whole renewables figure. The total
    is instead the sum of the nine sources, which is how the Statistical Review's own total behaves.
    """
    columns = {eia_column: f"{source}_twh" for source, eia_column in EIA_SOURCES.items()}
    tb_eia = tb_eia[["country", "year"] + list(columns)].rename(columns=columns, errors="raise")
    tb_eia = tb_eia.dropna(subset=list(columns.values()), how="all").reset_index(drop=True)

    # Only rows reporting every source get a total; a partial sum would understate it silently.
    sources = list(columns.values())
    tb_eia["total_energy_supply_twh"] = tb_eia[sources].sum(axis=1, min_count=len(sources))

    # Keep only countries: EIA's own regions and the OWID ones its garden step builds would be counted
    # twice, here and again in add_region_aggregates.
    is_aggregate = tb_eia["country"].str.contains("(EIA)", regex=False) | tb_eia["country"].isin(
        REGIONS + ["World", "European Union (27)"]
    )
    tb_eia = tb_eia[~is_aggregate].reset_index(drop=True)

    # EIA's USSR is dropped: the Statistical Review covers it to 1984 and its successors from 1985, so
    # keeping EIA's would overlap them. East and West Germany and Czechoslovakia are kept — their own
    # series are worth publishing — and are left out of the aggregates instead, in add_region_aggregates.
    tb_eia = tb_eia[tb_eia["country"] != "USSR"].reset_index(drop=True)

    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_eia, index_columns=["country", "year"])
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    return tb


def fill_missing_biofuels(tb: Table) -> Table:
    """Fill missing biofuels with zero, where the total leaves no room for it.

    The Statistical Review fills its other sources this way itself, but not biofuels, because its total
    omits the biofuels it had not started tracking (it reports none for the United States until 1990, while
    the EIA measured 2-21 TWh a year there over 1981-1989). Filling it here, once both producers are in,
    keeps a zero we derived from outranking a value someone measured.
    """
    others = [f"{source}_twh" for source in SR_SOURCES.values() if source != "biofuels"]
    total = tb["total_energy_supply_twh"]
    # A total of exactly zero is handled in complete_or_drop_mixes, where energy cannot be negative.
    accounted = tb[others].sum(axis=1, min_count=len(others))
    to_fill = tb["biofuels_twh"].isna() & (total > 0) & ((total - accounted).abs() <= 0.01 * total)
    assert to_fill.any(), "The producer's total no longer confirms any missing biofuels as zero."
    tb.loc[to_fill, "biofuels_twh"] = 0
    return tb


def get_eia_year_range(tb_eia: Table) -> tuple[int, int]:
    """First and last year EIA reports every source, which bounds the region aggregates."""
    years = tb_eia.loc[tb_eia[list(EIA_SOURCES.values())].notna().all(axis=1), "year"]
    assert not years.empty, "EIA reports no year with all sources informed."
    return int(years.min()), int(years.max())


def add_region_aggregates(tb: Table, eia_years: tuple[int, int]) -> Table:
    """Build the region aggregates from the combined country-level data.

    Aggregates are published only for the years EIA covers, because most of the countries in them come
    from EIA. Outside that window the Statistical Review itemizes too few countries to stand for a
    region (4 of Africa's 57, home to 23% of the continent), and adding them at 1980 would show a rise
    that is ours, not the world's: South America's would climb 6.5% in a year purely because Bolivia,
    Paraguay, Uruguay, Guyana and Suriname appear.

    No coverage condition is imposed within that window. Every region reports at least three quarters of
    the countries that ever report there, so any threshold low enough to admit them all never fires; the
    checks in sanity_check_outputs are what catch a region losing data.
    """
    is_country = ~tb["country"].str.contains("(EI)", regex=False) & ~tb["country"].isin(
        REGIONS + ["World", "European Union (27)"] + DUPLICATED_TERRITORIES
    )
    # A country contributes only where it has a total. Otherwise it would add to the sources while
    # adding nothing to their denominator, and the shares would exceed 100%: EIA reports Afghanistan's
    # coal and oil from 2021 but not its gas, so it has no total for those years.
    has_total = tb["total_energy_supply_twh"].notna()
    tb_aggregates = paths.regions.add_aggregates(
        tb[is_country & has_total].reset_index(drop=True),
        regions={region: {} for region in REGIONS},
        min_num_values_per_year=1,
        accepted_overlaps=ACCEPTED_OVERLAPS,
        ignore_overlaps_of_zeros=True,
    )
    tb_aggregates = tb_aggregates[
        tb_aggregates["country"].isin(REGIONS) & tb_aggregates["year"].between(*eia_years)
    ].reset_index(drop=True)
    return pr.concat([tb, tb_aggregates], ignore_index=True).sort_values(["country", "year"]).reset_index(drop=True)


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


def add_traditional_biomass(tb: Table, tb_smil: Table) -> Table:
    """Add World traditional biomass (Smil 2017) as a separate series.

    Traditional biomass is deliberately NOT part of Total Energy Supply: it is excluded from the total
    and from every aggregate, and is not exposed as an MDIM source. It is provided here as a standalone
    World-only series so the biomass-inclusive charts (e.g. global primary energy by source, including
    traditional biomass) can stack it alongside the commercially-traded sources.

    Smil's estimates run until ~2015; for later years we hold the last value constant (as the previous
    global_primary_energy step did), because there is no reliable recent measurement of traditional
    biomass use.
    """
    smil = tb_smil.reset_index()
    smil = smil[smil["country"] == "World"][["country", "year", "traditional_biomass__twh_direct_energy"]].rename(
        columns={"traditional_biomass__twh_direct_energy": "traditional_biomass_twh"}, errors="raise"
    )
    tb = combine_two_overlapping_dataframes(df1=tb, df2=smil, index_columns=["country", "year"])
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    # Hold Smil's last value constant from its final year up to the latest year in the data.
    smil_last_year = int(smil["year"].max())
    mask = (tb["country"] == "World") & (tb["year"] >= smil_last_year)
    tb.loc[mask, "traditional_biomass_twh"] = tb.loc[mask, "traditional_biomass_twh"].ffill()
    return tb


def add_aggregate_sources(tb: Table) -> Table:
    """Create aggregate sources (fossil fuels, renewables, low-carbon energy, solar and wind).

    Every component is required: the Statistical Review garden step already fills in the zeros the
    producer omits, so a component still missing here is unknown, not zero.
    """
    tb = tb.copy()
    renewables = ["hydro_twh", "solar_twh", "wind_twh", "other_renewables_twh", "biofuels_twh"]
    tb["fossil_fuels_twh"] = tb[["coal_twh", "oil_twh", "gas_twh"]].sum(axis=1, min_count=3)
    tb["renewables_twh"] = tb[renewables].sum(axis=1, min_count=len(renewables))
    tb["low_carbon_energy_twh"] = tb[renewables + ["nuclear_twh"]].sum(axis=1, min_count=len(renewables) + 1)
    tb["solar_and_wind_twh"] = tb[["solar_twh", "wind_twh"]].sum(axis=1, min_count=2)
    return tb


def complete_or_drop_mixes(tb: Table) -> Table:
    """Leave every row reporting either the whole mix or none of it.

    A total needs all nine sources, so a row without one has an incomplete mix. Most carry nothing but a
    zero left by one of the fills (Angola has nuclear before the Statistical Review covers it, Afghanistan
    has nuclear and biofuels in 2025), and the rest would read as a complete mix in a stacked chart:
    Tuvalu's oil would be all of its energy, and Reunion's three zeros would make it look like it consumes
    nothing at all. Those rows lose their sources.

    Where the total is zero the mix is complete by arithmetic, since energy cannot be negative, so the
    unreported sources are set to zero instead. The Statistical Review carries such rows for the years
    before a country exists, Bangladesh before 1971 among them.
    """
    columns = [f"{source}_twh" for source in ALL_SOURCES]
    total = tb["total_energy_supply_twh"]
    tb.loc[total.isna(), columns] = float("nan")
    for column in columns:
        tb.loc[(total == 0) & tb[column].isna(), column] = 0
    return tb


def add_shares(tb: Table) -> Table:
    """Add the share of each source in total energy supply (as a percentage)."""
    tb = tb.copy()
    for source in ALL_SOURCES:
        # Clip to 100: float32 noise otherwise leaves values like 100.000008, which makes grapher
        # render an open-ended ">100%" bracket on the map legend.
        tb[f"{source}_share_pct"] = (100 * tb[f"{source}_twh"] / tb["total_energy_supply_twh"]).clip(upper=100)
    return tb


def fill_nuclear_zero_for_countries_without_nuclear(tb: Table) -> Table:
    """Set nuclear to zero for EIA-only countries confirmed to have no nuclear power.

    Countries that enter only through the EIA total-energy extension have no by-source breakdown, so
    their nuclear is missing (NaN) and the map renders them as "No data". For countries we know have no
    nuclear power, that missing value is really a zero; filling it makes the map show "No nuclear".
    Entities that did operate nuclear power are deliberately left as NaN.
    """
    tb = tb.copy()
    # Countries with no by-source breakdown at all (nuclear is NaN in every year they appear).
    nuclear_all_nan = tb.groupby("country", observed=True)["nuclear_twh"].transform(lambda s: s.isna().all())
    eia_only = set(tb.loc[nuclear_all_nan, "country"])
    # Energy Institute regional aggregates (e.g. "OPEC (EI)") are not countries and can contain nuclear
    # members, so they are never zero-filled and are excluded from the classification.
    eia_only = {country for country in eia_only if not country.endswith("(EI)")}
    # Guard: every such country must be explicitly classified, so a newly-added country fails the build
    # loudly instead of silently showing "No data".
    unclassified = eia_only - set(COUNTRIES_WITHOUT_NUCLEAR) - set(COUNTRIES_WITH_NUCLEAR_NO_BREAKDOWN)
    assert not unclassified, (
        f"Countries with no nuclear data are not classified as with/without nuclear: {sorted(unclassified)}"
    )
    tb.loc[tb["country"].isin(COUNTRIES_WITHOUT_NUCLEAR) & tb["nuclear_twh"].isna(), "nuclear_twh"] = 0
    return tb


def add_biomass_inclusive_shares(tb: Table) -> Table:
    """Add World-only shares of each source in primary energy *including* traditional biomass.

    Total energy supply excludes traditional biomass by design, so these shares use an alternative
    denominator (total energy supply plus traditional biomass) and cover the World only, reproducing the
    biomass-inclusive share chart from the old global_primary_energy step. Each source's share, including
    biomass itself, sums to 100%.
    """
    tb = tb.copy()
    # Traditional biomass is World-only, so this denominator (and thus these shares) are non-null for World only.
    total_with_biomass = tb["total_energy_supply_twh"] + tb["traditional_biomass_twh"]
    for source, name in BIOMASS_SHARE_SOURCES.items():
        col = f"{source}_share_including_biomass_pct"
        tb[col] = (100 * tb[f"{source}_twh"] / total_with_biomass).clip(upper=100)
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


def sanity_check_outputs(tb: Table) -> None:
    """Check the output.

    No check here is restricted to a subset of years. Where a comparison cannot be made at all (a sum
    of continents missing one of them cannot equal the World), the years it covers are asserted
    instead, so a change in coverage fails rather than passing silently.
    """
    # No fully-NaN columns.
    assert tb.columns[tb.isna().all()].empty, f"Fully-NaN columns: {list(tb.columns[tb.isna().all()])}"
    # Shares should be within [0, 100] (allowing a small tolerance).
    for source in ALL_SOURCES:
        col = f"{source}_share_pct"
        valid = tb[col].dropna()
        assert (valid >= -0.01).all() and (valid <= 100.01).all(), f"{col} out of [0, 100]."

    # Wherever the exclusive sources are all reported, their shares must add up to ~100%. This is what
    # catches a share whose numerator and denominator cover different countries.
    exclusive = [f"{source}_share_pct" for source in SR_SOURCES.values()]
    complete = tb[exclusive].notna().all(axis=1)
    share_sum = tb.loc[complete, exclusive].sum(axis=1)
    off = tb.loc[complete][(share_sum - 100).abs() > MAX_SHARE_SUM_DEVIATION_PCT]
    assert off.empty, "Shares of the exclusive sources do not add up to ~100%: " + "; ".join(
        f"{country} ({group['year'].min()}-{group['year'].max()})"
        for country, group in off.groupby("country", observed=True)
    )
    # A row reports either every source or none, so a stacked chart cannot show part of a mix as the whole.
    sources = [f"{source}_twh" for source in SR_SOURCES.values()]
    partial = tb[sources].notna().any(axis=1) & ~tb[sources].notna().all(axis=1)
    assert not partial.any(), "Some rows report only part of the mix: " + "; ".join(
        f"{country} ({group['year'].min()}-{group['year'].max()})"
        for country, group in tb[partial].groupby("country", observed=True)
    )

    # Wherever all nine sources are reported, they must add up to the total. This is the same defect as
    # above seen from the other side: a total that covers different countries from its components.
    sources = [f"{source}_twh" for source in SR_SOURCES.values()]
    complete = tb[sources + ["total_energy_supply_twh"]].notna().all(axis=1)
    deviation = (
        100
        * (tb.loc[complete, sources].sum(axis=1) - tb.loc[complete, "total_energy_supply_twh"])
        / tb.loc[complete, "total_energy_supply_twh"]
    )
    off = tb.loc[complete][deviation.abs() > MAX_SOURCES_DEVIATION_PCT]
    assert off.empty, "The sources do not add up to the total energy supply: " + "; ".join(
        f"{country} ({group['year'].min()}-{group['year'].max()})"
        for country, group in off.groupby("country", observed=True)
    )

    # World total energy supply for the latest year should be in a plausible range (~600 EJ ~= 167000 TWh).
    world_latest = tb[(tb["country"] == "World")].sort_values("year").iloc[-1]
    assert 140000 < world_latest["total_energy_supply_twh"] < 200000, (
        f"World total energy supply is out of the expected range: {world_latest['total_energy_supply_twh']:.0f} TWh."
    )

    # Each region's aggregate must span one unbroken run of years. A coverage gate that flickers in and
    # out would otherwise publish a series full of holes.
    published_years = {}
    for region in REGIONS:
        is_informed = (tb["country"] == region) & tb["total_energy_supply_twh"].notna()
        years = sorted(tb.loc[is_informed, "year"].unique())
        if not years:
            continue
        assert years == list(range(years[0], years[-1] + 1)), (
            f"{region}'s aggregate has gaps: it is missing {sorted(set(range(years[0], years[-1] + 1)) - set(years))}."
        )
        published_years[region] = (int(years[0]), int(years[-1]))

    # The years each region covers are recorded here so that a change in coverage has to be acknowledged
    # rather than discovered on a chart.
    assert published_years == EXPECTED_REGION_YEARS, (
        "Region aggregates no longer span the expected years. Found: "
        + "; ".join(f"{region}: {first}-{last}" for region, (first, last) in sorted(published_years.items()))
    )

    # Within the years a region covers, every indicator must be informed. A new gap means the region
    # quietly lost a source, which would leave its shares no longer adding up to the whole.
    gaps = {}
    for region, (first, last) in published_years.items():
        block = tb[(tb["country"] == region) & tb["year"].between(first, last)]
        # Annual change is empty in the first year of every series, and its percentage version is also
        # undefined wherever the previous year was zero, so its gaps follow from the columns checked here.
        missing = {
            column: int(block[column].isna().sum())
            for column in block.columns
            if column not in COLUMNS_ALLOWED_TO_BE_EMPTY_FOR_REGIONS
            and "_annual_change_" not in column
            and block[column].isna().any()
        }
        if missing:
            gaps[region] = missing
    assert not gaps, f"Region aggregates have unexpected missing values: {gaps}"

    # The continents partition the globe, and so do the income groups, so wherever a family is complete it
    # must add up to the Statistical Review's own World total. This is checked for every energy column, not
    # just the total: a region built from a country set that does not cover the world shows up here, which
    # is the defect that made South America's shares mix two producers.
    world = tb[tb["country"] == "World"].set_index("year")
    families = {"continents": CONTINENTS, "income groups": [region for region in REGIONS if region not in CONTINENTS]}
    for family, regions in families.items():
        members = tb[tb["country"].isin(regions)]
        for column in [f"{source}_twh" for source in ALL_SOURCES] + ["total_energy_supply_twh"]:
            per_year = members.groupby("year", observed=True)[column].agg(["sum", "count"])
            complete_years = per_year[per_year["count"] == len(regions)]
            gap = (complete_years["sum"] - world[column]).dropna()
            assert not gap.empty, f"No year has all {family} published for {column}."
            deviation = 100 * gap / world[column][gap.index]
            off = gap[(deviation.abs() > MAX_WORLD_DEVIATION_PCT) & (gap.abs() > MAX_WORLD_DEVIATION_TWH)]
            assert off.empty, (
                f"The {family} do not add up to the Statistical Review's World total for {column}: "
                + "; ".join(f"{year}: {deviation[year]:+.1f}% ({value:+.0f} TWh)" for year, value in off.items())
            )


def run() -> None:
    #
    # Load data.
    #
    # Load the Statistical Review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy")

    # Load the EIA International Energy dataset and read its main table.
    ds_eia = paths.load_dataset("international_energy")
    tb_eia = ds_eia.read("international_energy")

    # Load the Maddison GDP dataset.
    ds_gdp = paths.load_dataset("maddison_project_database")

    # Load the Smil (2017) dataset, used to extend the World series before 1965.
    ds_smil = paths.load_dataset("smil_2017")
    tb_smil = ds_smil.read("smil_2017")

    #
    # Process data.
    #
    # Select TES-by-source data from the Statistical Review (without its own OWID region aggregates).
    tb = get_statistical_review_data(tb_review=tb_review)

    # Extend the total and each source to the countries the Statistical Review does not report.
    eia_years = get_eia_year_range(tb_eia=tb_eia)
    tb = combine_with_eia(tb=tb, tb_eia=tb_eia)

    # Now that both producers are in, fill the biofuels the Statistical Review leaves out with zeros.
    tb = fill_missing_biofuels(tb=tb)

    # Build the OWID region aggregates from the combined country-level data, over EIA's years.
    tb = add_region_aggregates(tb=tb, eia_years=eia_years)

    # Extend the World series back to 1800 with Smil (2017).
    tb = add_smil_world_long_run(tb=tb, tb_smil=tb_smil)

    # Create aggregate sources (fossil fuels, renewables, low-carbon energy, solar and wind).
    tb = add_aggregate_sources(tb=tb)

    # Add World traditional biomass (Smil), kept separate from the TES total, for biomass-inclusive charts.
    tb = add_traditional_biomass(tb=tb, tb_smil=tb_smil)

    # For EIA-only countries confirmed to have no nuclear power, fill their missing nuclear with zero
    # (so the nuclear map shows "No nuclear" instead of "No data").
    tb = fill_nuclear_zero_for_countries_without_nuclear(tb=tb)

    # Leave every row with either the whole mix or none of it, so no chart shows part of one as the whole.
    tb = complete_or_drop_mixes(tb=tb)

    # Add shares, annual change, per-capita and per-GDP variables.
    tb = add_shares(tb=tb)
    # Add World-only shares against a denominator that includes traditional biomass.
    tb = add_biomass_inclusive_shares(tb=tb)
    tb = add_annual_change(tb=tb)
    tb = add_per_capita(tb=tb)
    tb = add_per_gdp(tb=tb, ds_gdp=ds_gdp)

    # Remove outliers.
    tb = tb[~tb["country"].isin(OUTLIERS)].reset_index(drop=True)

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Derived indicators must not inherit key points from a single input (e.g. EI's oil-consumption
    # notes on the fossil aggregate, or Maddison boilerplate on energy per GDP). Key points come only
    # from this step's own meta.yml, which is applied on save.
    for column in tb.columns:
        tb[column].m.description_key = []

    # For the same reason, drop the producer's description where it describes only one input:
    # low-carbon energy sums nuclear and renewables, whose constituents the Statistical Review
    # describes differently, and energy per GDP divides its energy by another producer's GDP.
    for column in COLUMNS_WITHOUT_PRODUCER_DESCRIPTION:
        tb[column].m.description_from_producer = None

    # Format table conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
