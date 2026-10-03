"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels of the commodities and transactions used from the UNSD Energy Balances.
COMMODITY_TOTAL_ENERGY = "Total energy"
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
    "Memo: Renewables",
    COMMODITY_TOTAL_ENERGY,
}

# Country-years dropped from the share indicator because the sector's dominant energy source is missing from the
# source data (while total final energy consumption keeps being reported), which collapses the computed share. These
# are the two most recent years as of the 2025 release; the assertion in create_share_table fails if UNSD revises
# them. Older reporting quirks in other countries (e.g. Germany's fuel reallocation) are left as published.
INCOMPLETE_AGRICULTURE_REPORTING = [
    ("India", 2023),
    ("Sri Lanka", 2023),
]

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

    # Drop the country-years with an incomplete agriculture flow (see INCOMPLETE_AGRICULTURE_REPORTING). We check the
    # flow has collapsed to less than half of the previous year, so the list is revisited if UNSD revises these years.
    # Dropped points are treated as unreported, so they are also excluded from the region aggregates below.
    agriculture = tb_wide.set_index(["country", "year"])["agriculture"]
    for country, year in INCOMPLETE_AGRICULTURE_REPORTING:
        current = agriculture.get((country, year), float("nan"))
        previous = agriculture.get((country, year - 1), float("nan"))
        assert current < 0.5 * previous, (
            f"{country} {year} agriculture flow no longer looks incomplete; revisit INCOMPLETE_AGRICULTURE_REPORTING."
        )
    incomplete = {tuple(pair) for pair in INCOMPLETE_AGRICULTURE_REPORTING}
    mask = [(country, year) in incomplete for country, year in zip(tb_wide["country"], tb_wide["year"])]
    tb_wide.loc[mask, "agriculture"] = None

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
    # the region's total reported final energy consumption (otherwise the regional share is not representative). This
    # drops the earliest years of a few sparsely-reported income groups.
    coverage = tb_wide["final_energy_consumption_of_agriculture_reporters"] / tb_wide["final_energy_consumption"]
    insufficient = tb_wide["country"].isin(REGIONS) & (coverage < MINIMUM_COVERAGE_OF_FINAL_CONSUMPTION)
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

    # The country-years with an incomplete agriculture flow must have been dropped.
    for country, year in INCOMPLETE_AGRICULTURE_REPORTING:
        assert (country, year) not in shares.index, f"{country} {year} should have been dropped."


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
