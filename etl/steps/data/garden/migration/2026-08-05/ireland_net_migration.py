"""Long-run net migration for Ireland, 1871-2025, on the constant area of today's Republic.

This step builds an annual series from three CSO sources:

- 1871-1926: average yearly net emigration per intercensal period, hand-entered from the 1926
  Census General Report. Every year in a period carries the period's average.
- 1926-1951: net migration totals per intercensal period from the 2022 census (table F1005),
  divided by the period length. Every year in a period carries the period's average.
- From 1951: the CSO's annual estimates (table PEA15, years ending in April), in thousands.

The share of population divides each year's net migration by the population: interpolated census
populations before 1951, and the CSO's annual population estimates from 1951.
"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The year from which the annual estimates replace the intercensal period averages.
ANNUAL_FROM = 1951


def spread_periods(tb: Table, value_col: str) -> Table:
    """Assign each period's average yearly net migration to every year of the period."""
    rows = []
    for _, r in tb.iterrows():
        for year in range(int(r["period_start"]), int(r["period_end"])):
            rows.append({"year": year, "net_migration": r[value_col]})
    out = Table(pd.DataFrame(rows))
    out["net_migration"] = out["net_migration"].copy_metadata(tb[value_col])
    return out


def sanity_check_outputs(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years."
    years = set(tb["year"])
    assert years == set(range(1871, max(years) + 1)), "The series has gaps."
    t = tb.set_index("year")
    # The 1880s were the worst pre-independence decade; the series turns positive only after 1961.
    assert t.loc[1885, "net_migration"] == -59_733
    assert (t.loc[:1960, "net_migration"] < 0).all(), "Unexpected net inflow before 1961."
    assert t["net_migration_share_of_population"].abs().max() < 3, "Share outside the plausible range."
    assert t["net_migration_share_of_population"].notna().all(), "Missing share values."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("ireland_net_migration")
    tb_annual = ds_meadow.read("annual_estimates")
    tb_intercensal = ds_meadow.read("intercensal_1926_2022")
    tb_1926 = ds_meadow.read("intercensal_1871_1926")

    #
    # Process data.
    #
    # 1871-1926: flip hand-entered net emigration (males + females, yearly averages) to net migration.
    tb_1926["net_migration_yearly"] = -(tb_1926["net_emigration_males"] + tb_1926["net_emigration_females"])
    early = spread_periods(tb_1926, "net_migration_yearly")

    # 1926-1951: period totals divided by period length.
    tb_mid = tb_intercensal[tb_intercensal["period_end"] <= ANNUAL_FROM].copy()
    assert len(tb_mid) == 3, "Expected the periods 1926-1936, 1936-1946 and 1946-1951."
    tb_mid["net_migration_yearly"] = tb_mid["net_migration"] / (tb_mid["period_end"] - tb_mid["period_start"])
    mid = spread_periods(tb_mid, "net_migration_yearly")

    # From 1951: annual estimates, converted from thousands to people.
    late = tb_annual[["year", "net_migration"]].copy()
    late["net_migration"] = late["net_migration"] * 1000

    tb = pr.concat([early, mid, late], ignore_index=True).sort_values("year")

    # Denominator: census populations (1871-1926 hand-entered, in thousands; 1936-1951 from the
    # 2022 census table), linearly interpolated between census years; the CSO's annual population
    # estimates from 1951 (in thousands).
    census_years = list(tb_1926["period_start"]) + [1926] + list(tb_mid["period_end"])
    census_pops = (
        list(tb_1926["population_start_thousands"] * 1000)
        + [tb_1926["population_end_thousands"].iloc[-1] * 1000]
        + list(tb_mid["population_end"])
    )
    pre = tb["year"] < ANNUAL_FROM
    tb.loc[pre, "population"] = np.interp(tb.loc[pre, "year"], census_years, census_pops)
    late_pop = tb_annual.set_index("year")["population"] * 1000
    tb.loc[~pre, "population"] = tb.loc[~pre, "year"].map(late_pop)

    tb["net_migration_share_of_population"] = tb["net_migration"] / tb["population"] * 100
    tb["net_migration_share_of_population"] = tb["net_migration_share_of_population"].copy_metadata(tb["net_migration"])
    tb = tb.drop(columns=["population"])
    tb["country"] = "Ireland"

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name="ireland_net_migration")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
