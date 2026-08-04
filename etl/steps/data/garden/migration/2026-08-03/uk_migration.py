"""Long-run migration flows for the United Kingdom, 1853-2025.

This step builds an OWID-maintained series by joining two sources at 2012:

- Up to 2011: the Bank of England's spliced historical series (sheet A19a of "A millennium of
  macroeconomic data"), which itself combines passenger statistics, Mitchell's historical
  statistics, and the ONS's survey-based estimates. Converted from thousands to people.
  The Bank's own share-of-population figures are kept, since their denominator matches the UK's
  borders of the time (including all of Ireland before 1922).
- From 2012: the ONS's administrative-data-based estimates of long-term international migration
  (year-ending-December values, which cover calendar years). The ONS considers these its best
  measure from 2012 onward; the Bank's 2012-2016 values came from the older survey-based method,
  which undercounted both immigration and emigration, and are discarded. Shares of population for
  this period are computed from OWID's long-run population estimates.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The year from which the ONS administrative estimates replace the Bank of England series.
JOIN_YEAR = 2012

# Years whose net migration figure was revised by the ONS after the 2011 census. In these years the
# published net migration ("headline") figure does not equal immigration minus emigration, because
# the components were not revised alongside the balance.
CENSUS_REVISED_YEARS = set(range(2001, JOIN_YEAR))

FLOWS = ["immigration", "emigration", "net_migration"]


def sanity_check_inputs(tb_boe: Table, tb_ons: Table) -> None:
    # The two sources must overlap at the join, and the administrative estimates must sit above the
    # survey-based ones there (the survey undercounted both directions).
    overlap = set(tb_boe["year"]) & set(tb_ons["year"])
    assert overlap == {2012, 2013, 2014, 2015, 2016}, f"Unexpected overlap between sources: {sorted(overlap)}"
    boe_2012 = tb_boe.loc[tb_boe["year"] == JOIN_YEAR].iloc[0]
    ons_2012 = tb_ons.loc[tb_ons["year"] == JOIN_YEAR].iloc[0]
    assert 1 < ons_2012["immigration"] / (boe_2012["immigration"] * 1000) < 1.5, "Unexpected gap at the 2012 join."


def sanity_check_england(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years in the England table."
    assert len(tb) == 330, "Expected 330 years of England data."
    # Wrigley and Schofield's reconstruction shows a net outflow (negative net migration) in every year.
    assert (tb["net_migration"] < 0).all(), "Unexpected net inflow year in the England series."
    assert tb["net_migration_share_of_population"].between(-0.5, 0).all(), "Share outside the plausible range."


def sanity_check_outputs(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years."
    t = tb.set_index("year")

    # Net migration must equal immigration minus emigration, except in the census-revised years.
    residual = (t["net_migration"] - (t["immigration"] - t["emigration"])).dropna()
    inconsistent = set(residual[residual.abs() > 1000].index)
    assert inconsistent == CENSUS_REVISED_YEARS, (
        f"Net migration differs from immigration minus emigration outside 2001-2011: {sorted(inconsistent)}"
    )

    assert (t["immigration"].dropna() > 0).all() and (t["emigration"].dropna() > 0).all()
    assert t[FLOWS].abs().max().max() < 3_000_000, "Flow value outside the plausible range."
    for col in FLOWS:
        share = t[f"{col}_share_of_population"].dropna()
        assert share.abs().max() < 2.5, f"{col} share outside the plausible range."
        # Every year with a flow value must also have its share (the Bank computes shares for all
        # its years; we compute them for all ONS years) — except 1853-1854, which have emigration
        # but no population share in the source.
        missing_share = set(t[col].dropna().index) - set(share.index)
        assert missing_share <= {1853, 1854}, f"Years with {col} but no share: {sorted(missing_share)}"


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("uk_migration")
    tb_boe = ds_meadow.read("bank_of_england_flows")
    tb_ons = ds_meadow.read("ons_flows")
    tb_england = ds_meadow.read("england_net_migration")

    sanity_check_inputs(tb_boe, tb_ons)

    #
    # Process data.
    #
    # Bank of England period: keep years before the join, convert flows from thousands to people.
    # The Bank's share-of-population figures are kept as published; its population column only
    # served as their denominator and is dropped.
    tb_boe = tb_boe[tb_boe["year"] < JOIN_YEAR].copy()
    for col in FLOWS:
        tb_boe[col] = tb_boe[col] * 1000
    tb_boe = tb_boe.drop(columns=["population"])
    tb_boe["country"] = "United Kingdom"

    # ONS period: already in people. Compute shares of population from OWID's population estimates
    # (the UK's borders have not changed in this period, so there is no boundary mismatch).
    tb_ons["country"] = "United Kingdom"
    tb_ons = paths.regions.add_population(tb_ons)
    assert tb_ons["population"].notna().all(), "Missing population for an ONS-period year."
    for col in FLOWS:
        tb_ons[f"{col}_share_of_population"] = tb_ons[col] / tb_ons["population"] * 100
    tb_ons = tb_ons.drop(columns=["population"])

    tb = pr.concat([tb_boe, tb_ons], ignore_index=True).sort_values("year")

    sanity_check_outputs(tb)

    # England, 1541-1870: the source publishes net emigration (positive = people leaving). Flip the
    # sign to net migration, consistent with the UK indicators, and express the source's per-1,000
    # rate as a percentage share of the population.
    tb_england["country"] = "England"
    tb_england["net_migration"] = -tb_england["net_emigration"] * 1000
    tb_england["net_migration_share_of_population"] = -tb_england["net_emigration_per_1000"] / 10
    tb_england = tb_england.drop(columns=["net_emigration", "net_emigration_per_1000"])

    sanity_check_england(tb_england)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[
            tb.format(["country", "year"], short_name="uk_migration_flows"),
            tb_england.format(["country", "year"], short_name="england_net_migration"),
        ],
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()
