"""Energy mix: Total Energy Supply (TES) by source, from the Energy Institute's Statistical Review.

For each source and aggregate it reports TES in absolute terms, per capita, as a share of the total,
and as annual change. The World series is extended back to 1800 with Smil (2017), the total is
extended to countries not covered by the Statistical Review with EIA data, and a per-GDP variable is
added (Maddison). Traditional biomass (Smil, World only) is kept separate from TES.
"""

from owid.catalog import Dataset, Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from shared import EXCLUDED_PROVIDER_REGIONS

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

# Sources shown in the biomass-inclusive share chart: the individual TES sources plus traditional biomass.
BIOMASS_SHARE_SOURCES = {
    **{source: SOURCE_NAMES[source] for source in SR_SOURCES.values()},
    "traditional_biomass": "Traditional biomass",
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

    # Combine, prioritizing the Statistical Review *per value*: keep its total energy supply where it has
    # one, fall back to EIA where it is missing, and add EIA-only country-years. Using a plain concat +
    # drop_duplicates(keep="last") would instead let the Statistical Review's NaN totals override EIA for
    # countries it lists for other fuels but not for total energy supply (e.g. Nigeria, Angola, Libya),
    # silently dropping them from the map.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_eia, index_columns=["country", "year"])
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
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

    # Add World traditional biomass (Smil), kept separate from the TES total, for biomass-inclusive charts.
    tb = add_traditional_biomass(tb=tb, tb_smil=tb_smil)

    # For EIA-only countries confirmed to have no nuclear power, fill their missing nuclear with zero
    # (so the nuclear map shows "No nuclear" instead of "No data").
    tb = fill_nuclear_zero_for_countries_without_nuclear(tb=tb)

    # Add shares, annual change, per-capita and per-GDP variables.
    tb = add_shares(tb=tb)
    # Add World-only shares against a denominator that includes traditional biomass.
    tb = add_biomass_inclusive_shares(tb=tb)
    tb = add_annual_change(tb=tb)
    tb = add_per_capita(tb=tb)
    tb = add_per_gdp(tb=tb, ds_gdp=ds_gdp)

    # Remove outliers.
    tb = tb[~tb["country"].isin(OUTLIERS)].reset_index(drop=True)

    # Remove residual and undefined provider regions (kept in the Statistical Review garden as
    # aggregation inputs, but meaningless to readers).
    tb = tb[~tb["country"].isin(EXCLUDED_PROVIDER_REGIONS)].reset_index(drop=True)

    # Sanity checks.
    sanity_check_outputs(tb=tb)

    # Derived indicators must not inherit key points from a single input (e.g. EI's oil-consumption
    # notes on the fossil aggregate, or Maddison boilerplate on energy per GDP). Key points come only
    # from this step's own meta.yml, which is applied on save.
    for column in tb.columns:
        tb[column].m.description_key = []

    # Format table conveniently.
    tb = tb.format(sort_columns=True, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
