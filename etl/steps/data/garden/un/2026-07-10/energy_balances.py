"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels of the commodities and transactions used from the UNSD Energy Balances.
COMMODITY_TOTAL_ENERGY = "Total energy"
COMMODITY_RENEWABLES_MEMO = "Memo: Renewables"
TRANSACTION_AGRICULTURE = "Agriculture, forestry and fishing"
TRANSACTION_FINAL_ENERGY_CONSUMPTION = "Final Energy Consumption"

# All commodities expected in the data (a change here signals a schema change at the source).
EXPECTED_COMMODITIES = {
    "Primary coal and peat",
    "Coal and peat products",
    "Primary Oil",
    "Oil Products",
    "Natural Gas",
    "Biofuels and waste",
    "Nuclear",
    "Electricity",
    "Heat",
    COMMODITY_TOTAL_ENERGY,
    COMMODITY_RENEWABLES_MEMO,
}

# Short name of the indicator table and its column.
SHARE_COLUMN = "share_of_final_energy_consumed_by_agriculture_forestry_and_fishing"

# Regions to create aggregates for.
REGIONS = [
    # Continents.
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    # Income groups.
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
    # Other groups.
    "European Union (27)",
    "World",
]

# A region aggregate is only published for years when the countries reporting the agriculture flow account for at
# least this fraction of the region's total reported final energy consumption.
MINIMUM_COVERAGE_OF_FINAL_CONSUMPTION = 0.7

# Warn about year-over-year changes in a country's agriculture flow larger than this factor (e.g. 1.5 means +-50%),
# when the flow is large enough to matter.
YEAR_ON_YEAR_WARNING_FACTOR = 1.5
YEAR_ON_YEAR_WARNING_MINIMUM_TJ = 10_000


def sanity_check_inputs(tb: Table) -> None:
    """Check meadow data before processing."""
    assert set(tb["commodity"]) == EXPECTED_COMMODITIES, (
        f"Commodities changed at the source: {set(tb['commodity']) ^ EXPECTED_COMMODITIES}"
    )
    for transaction in [TRANSACTION_AGRICULTURE, TRANSACTION_FINAL_ENERGY_CONSUMPTION]:
        assert transaction in set(tb["transaction"]), f"Transaction {transaction!r} missing from the data."
    # The energy balances express all flows in terajoules.
    assert set(tb["unit"]) == {"Terajoules"}, f"Unexpected units: {set(tb['unit'])}"
    assert not tb.duplicated(subset=["country", "year", "commodity", "transaction"]).any(), (
        "Duplicated (country, year, commodity, transaction) rows."
    )
    # The flows used for the share indicator must not be negative (other flows, like stock changes or statistical
    # differences, can legitimately be negative).
    flows_used = tb[tb["transaction"].isin([TRANSACTION_AGRICULTURE, TRANSACTION_FINAL_ENERGY_CONSUMPTION])]
    assert (flows_used["value"].dropna() >= 0).all(), "Negative consumption flow found."


def find_reporting_breaks(tb: Table) -> list[tuple[str, int]]:
    """Find country-years where, in one of the last two years of data, the dominant fuel of the agriculture flow
    drops out (to exactly zero, or missing) while the total agriculture flow collapses and total final energy
    consumption continues to be reported.

    This catches artifacts like India 2023, where agricultural electricity (867,000 TJ in 2022, and the dominant
    fuel of the flow) disappears from the data while other fuels and total consumption continue, which collapses the
    computed share by ~97%. A break in the latest years is most likely an incomplete data delivery that the producer
    will revise. The masking is propagated to subsequent years for as long as the broken fuel remains zero or
    missing.

    NOTE: The same kind of dropout also happens further back in several countries' series (e.g. oil products vanish
    from Germany's agriculture flow in 1999-2017, and from Switzerland's from 2000 onwards). Those are long-standing
    sector-allocation conventions in national reporting rather than incomplete deliveries; following the scoping
    decision, they are kept in the data, warned about (see warn_on_large_year_on_year_changes) and documented in the
    indicator's description_key.
    """
    # Commodity-level agriculture flows (excluding the total and the renewables memo item, which overlap with fuels).
    fuels = tb[
        (tb["transaction"] == TRANSACTION_AGRICULTURE)
        & (~tb["commodity"].isin([COMMODITY_TOTAL_ENERGY, COMMODITY_RENEWABLES_MEMO]))
    ][["country", "year", "commodity", "value"]].dropna(subset=["value"])

    # Total agriculture flow per country-year.
    totals = (
        tb[(tb["transaction"] == TRANSACTION_AGRICULTURE) & (tb["commodity"] == COMMODITY_TOTAL_ENERGY)]
        .dropna(subset=["value"])[["country", "year", "value"]]
        .rename(columns={"value": "total"})
    )

    # Country-years where total final energy consumption is reported.
    fec = tb[
        (tb["transaction"] == TRANSACTION_FINAL_ENERGY_CONSUMPTION) & (tb["commodity"] == COMMODITY_TOTAL_ENERGY)
    ].dropna(subset=["value"])
    fec_reported = set(zip(fec["country"], fec["year"]))

    # Compare each fuel reported in one year with its value in the following year (missing if not reported).
    previous = fuels.rename(columns={"value": "value_previous"}).copy()
    previous["year"] = previous["year"] + 1
    current = fuels.rename(columns={"value": "value_current"})
    compared = previous.merge(current, on=["country", "year", "commodity"], how="left")

    # Add the total agriculture flow of the previous and current year.
    totals_previous = totals.rename(columns={"total": "total_previous"}).copy()
    totals_previous["year"] = totals_previous["year"] + 1
    compared = compared.merge(totals_previous, on=["country", "year"], how="left")
    compared = compared.merge(totals, on=["country", "year"], how="left")

    # Share of each fuel in the previous year's total agriculture flow.
    compared["dominance_previous"] = compared["value_previous"] / compared["total_previous"]

    # A break happens when a fuel that dominated the flow (more than half of it) drops to exactly zero or goes
    # missing, and the total agriculture flow (still reported) collapses accordingly. Only breaks starting in one of
    # the last two years of data are masked (older dropouts are long-standing reporting conventions, kept as
    # published).
    breaks = compared[
        (compared["dominance_previous"] > 0.5)
        & (compared["value_previous"] > 0)
        & ((compared["value_current"] == 0) | compared["value_current"].isna())
        & compared["total"].notna()
        & (compared["total"] < 0.5 * compared["total_previous"])
        & (compared["year"] >= tb["year"].max() - 1)
    ]

    # Propagate each break to subsequent years (with a reported total agriculture flow) for as long as the broken
    # fuel remains exactly zero or missing.
    fuel_values = fuels.set_index(["country", "commodity", "year"])["value"]
    masked = set()
    for _, row in breaks.iterrows():
        years_with_total = sorted(totals[totals["country"] == row["country"]]["year"])
        for year in [y for y in years_with_total if y >= row["year"]]:
            value = fuel_values.get((row["country"], row["commodity"], year))
            if value is not None and value != 0:
                break
            masked.add((row["country"], int(year)))

    # Only mask country-years where total final energy consumption continues to be reported (otherwise the share is
    # already missing).
    masked = sorted(pair for pair in masked if pair in fec_reported)
    if masked:
        log.warning(f"Masking country-years with a broken agriculture flow (dominant fuel drops out): {masked}")
    return masked


def warn_on_large_year_on_year_changes(tb_wide: Table) -> None:
    """Warn about large year-over-year changes in a country's agriculture flow.

    Known and accepted cases (left in the data, and documented in the indicator's description_key):
    - Germany's agriculture flow rises 19-fold between 2004 (8,200 TJ) and 2021 (157,000 TJ), a sector reallocation
      in German reporting.
    - India's oil-products line spikes to 341,000 TJ (2006) and 407,000 TJ (2013), against ~0-30,000 TJ in normal
      years.
    """
    current = tb_wide[["country", "year", "agriculture"]].dropna(subset=["agriculture"])
    previous = current.rename(columns={"agriculture": "agriculture_previous"}).copy()
    previous["year"] = previous["year"] + 1
    compared = current.merge(previous, on=["country", "year"], how="inner")
    compared["factor"] = compared["agriculture"] / compared["agriculture_previous"]
    large = compared[
        ((compared["factor"] > YEAR_ON_YEAR_WARNING_FACTOR) | (compared["factor"] < 1 / YEAR_ON_YEAR_WARNING_FACTOR))
        & (compared[["agriculture", "agriculture_previous"]].max(axis=1) > YEAR_ON_YEAR_WARNING_MINIMUM_TJ)
    ]
    if len(large) > 0:
        largest = large.sort_values("factor", ascending=False).head(10)
        cases = [f"{row['country']} {row['year']} (x{row['factor']:.2f})" for _, row in largest.iterrows()]
        log.warning(
            f"{len(large)} large year-over-year changes (>+-50%) in the agriculture flow; largest: {'; '.join(cases)}"
        )


def create_share_table(tb: Table) -> Table:
    """Create the indicator table with the share of final energy consumption used by agriculture, forestry and
    fishing, including region aggregates."""
    # Select the two total-energy flows and pivot them into columns.
    tb_total = tb[
        (tb["commodity"] == COMMODITY_TOTAL_ENERGY)
        & (tb["transaction"].isin([TRANSACTION_AGRICULTURE, TRANSACTION_FINAL_ENERGY_CONSUMPTION]))
    ][["country", "year", "transaction", "value"]].copy()
    tb_wide = tb_total.pivot(
        index=["country", "year"], columns="transaction", values="value", join_column_levels_with="_"
    ).rename(
        columns={
            TRANSACTION_AGRICULTURE: "agriculture",
            TRANSACTION_FINAL_ENERGY_CONSUMPTION: "final_energy_consumption",
        },
        errors="raise",
    )

    # Mask country-years with a broken agriculture flow (see find_reporting_breaks); they are treated as unreported,
    # so they are also excluded from the region aggregates below.
    # NOTE: If the following assertion fails after a data update, UNSD may have fixed India's 2023 agricultural
    # electricity (reported as exactly zero as of the 2025 release). Check the data and, if so, remove the assertion.
    reporting_breaks = find_reporting_breaks(tb)
    assert ("India", 2023) in reporting_breaks, "India 2023 was expected to be masked as a reporting break."
    breaks_set = set(reporting_breaks)
    break_mask = [(country, year) in breaks_set for country, year in zip(tb_wide["country"], tb_wide["year"])]
    tb_wide.loc[break_mask, "agriculture"] = None

    # Warn about large year-over-year changes in the agriculture flow (known cases are documented in the metadata).
    warn_on_large_year_on_year_changes(tb_wide)

    # For region aggregates, the denominator is restricted to countries that report the agriculture flow (otherwise
    # non-reporting countries would bias the share downwards); the total is kept to measure coverage.
    tb_wide["final_energy_consumption_of_agriculture_reporters"] = tb_wide["final_energy_consumption"].where(
        tb_wide["agriculture"].notna()
    )
    tb_wide = paths.regions.add_aggregates(
        tb=tb_wide,
        regions=REGIONS,
        index_columns=["country", "year"],
        aggregations={
            "agriculture": "sum",
            "final_energy_consumption": "sum",
            "final_energy_consumption_of_agriculture_reporters": "sum",
        },
    )

    # Share of final energy consumption used by agriculture, forestry and fishing. For countries, the restricted
    # denominator equals their own reported final energy consumption; for regions, it implements the restriction
    # described above.
    tb_wide[SHARE_COLUMN] = 100 * tb_wide["agriculture"] / tb_wide["final_energy_consumption_of_agriculture_reporters"]

    # Coverage guard: only publish a region-year when countries reporting the agriculture flow account for most of
    # the region's total reported final energy consumption.
    coverage = tb_wide["final_energy_consumption_of_agriculture_reporters"] / tb_wide["final_energy_consumption"]
    insufficient = tb_wide["country"].isin(REGIONS) & (coverage < MINIMUM_COVERAGE_OF_FINAL_CONSUMPTION)
    if insufficient.any():
        dropped = tb_wide[insufficient].groupby("country")["year"].agg(["min", "max", "count"])
        log.warning(f"Region-years dropped for insufficient coverage of the agriculture flow:\n{dropped}")
    tb_wide.loc[insufficient, SHARE_COLUMN] = None

    tb_share = tb_wide[["country", "year", SHARE_COLUMN]].dropna(subset=[SHARE_COLUMN]).reset_index(drop=True)

    return tb_share


def sanity_check_outputs(tb_share: Table) -> None:
    """Check the share indicator before saving, including reference values computed independently during scoping."""
    shares = tb_share.set_index(["country", "year"])[SHARE_COLUMN]
    assert ((shares >= 0) & (shares < 100)).all(), "Shares outside the [0, 100) range."

    # Reference values (in %), computed independently from the same API during scoping.
    references = {
        ("World", 1990): 2.76,
        ("United States", 2009): 1.58,
        ("Spain", 2009): 2.80,
        ("Brazil", 2009): 5.22,
        ("Netherlands", 1990): 9.46,
        ("India", 2009): 2.72,
        ("Germany", 1991): 1.25,
        ("Germany", 2023): 2.08,
    }
    for (country, year), expected in references.items():
        actual = shares.loc[country, year]
        assert abs(actual - expected) < 0.15, f"{country} {year}: expected ~{expected}, got {actual:.2f}"

    # India 2023 must be masked (broken agricultural electricity reporting).
    assert ("India", 2023) not in shares.index, "India 2023 should be masked as a reporting break."

    # Germany starts in 1991 in the source data.
    assert ("Germany", 1990) not in shares.index, "Germany 1990 was not expected in the source data."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_balances")

    # Read table from meadow dataset.
    tb = ds_meadow.read("energy_balances")

    #
    # Process data.
    #
    # Sanity check inputs.
    sanity_check_inputs(tb)

    # All flows are expressed in terajoules (asserted above), so the unit column is redundant; the estimate flag is
    # not used downstream.
    tb = tb.drop(columns=["unit", "estimate"])

    # Harmonize country names (composite reporters are mapped to their main country, e.g. "France-Monaco" to
    # "France").
    tb = paths.regions.harmonize_names(tb=tb)

    # Create the indicator table with the share of final energy consumption used by agriculture, forestry and
    # fishing, including region aggregates.
    tb_share = create_share_table(tb)

    # Sanity check outputs.
    sanity_check_outputs(tb_share)

    # Improve table formats.
    tb = tb.format(["country", "year", "commodity", "transaction"])
    tb_share = tb_share.format(["country", "year"], short_name=SHARE_COLUMN)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_share], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
