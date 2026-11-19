"""Load the two sources of long-run UK migration flows and create a meadow dataset.

- Bank of England, "A millennium of macroeconomic data", sheet A19a: the Bank's spliced annual
  series of immigration (1855-2016), emigration (1853-2016) and net migration (1855-2016) for the
  United Kingdom, in thousands, with the same flows as a percentage of the UK population.
  There is no data for 1939-1946.
- Office for National Statistics, "Long-term international migration, provisional": estimates of
  immigration, emigration and net migration built from administrative data, covering rolling
  12-month periods from the year ending June 2012 to the year ending December 2025. We keep the
  year-ending-December periods, which correspond to calendar years.
- Bank of England workbook, sheet A19c: net emigration from England (excluding Monmouthshire),
  1541-1870, in thousands and per 1,000 people, from Wrigley and Schofield's parish-register
  reconstruction as interpolated to annual values by the Bank of England.
"""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Columns of sheet A19a in the Bank of England workbook (0-indexed). The spliced headline series
# sit in columns B-D, the UK population used as denominator in F, and the shares in H-J.
A19A_COLUMNS = {
    0: "year",
    1: "immigration",
    2: "emigration",
    3: "net_migration",
    5: "population",
    7: "immigration_share_of_population",
    8: "emigration_share_of_population",
    9: "net_migration_share_of_population",
}


def parse_bank_of_england(snap: Snapshot) -> Table:
    """Parse sheet A19a: keep the spliced headline columns, drop non-data rows."""
    tb = snap.read_excel(sheet_name="A19a. UK Migration flows", header=None, skiprows=7)
    tb = tb[list(A19A_COLUMNS)]
    # Renaming integer column labels scrambles column-level metadata, so restore it explicitly.
    metadata = {name: tb[index].metadata.copy() for index, name in A19A_COLUMNS.items()}
    tb = tb.rename(columns=A19A_COLUMNS)
    for name, meta in metadata.items():
        tb[name].metadata = meta

    tb = tb[pd.to_numeric(tb["year"], errors="coerce").notna()]
    tb["year"] = tb["year"].astype(int)
    for col in tb.columns.drop("year"):
        tb[col] = pr.to_numeric(tb[col], errors="coerce")

    # Keep only years with at least one flow value: this drops 1850-1852 (population only) but
    # keeps 1853-1854, which have emigration but no immigration yet.
    tb = tb.dropna(subset=["immigration", "emigration", "net_migration"], how="all")

    assert tb["year"].min() == 1853 and tb["year"].max() == 2016, "Expected coverage 1853-2016."
    assert set(range(1939, 1947)).isdisjoint(tb["year"]), "Expected no data during 1939-1946."
    assert tb["population"].notna().all(), "Missing population in a year with flow data."
    # Spot-check values against the source (in thousands): the first emigration figure, and the
    # 1913 all-time emigration peak.
    assert tb.loc[tb["year"] == 1853, "emigration"].item() == 330
    assert tb.loc[tb["year"] == 1913, "emigration"].item() == 702
    return tb


def parse_england(snap: Snapshot) -> Table:
    """Parse sheet A19c: net emigration from England, 1541-1870."""
    tb = snap.read_excel(sheet_name="A19c. English Net Migration", header=None, skiprows=7)
    tb = tb[[0, 1, 2]]
    # Renaming integer column labels scrambles column-level metadata, so restore it explicitly.
    columns = {0: "year", 1: "net_emigration", 2: "net_emigration_per_1000"}
    metadata = {name: tb[index].metadata.copy() for index, name in columns.items()}
    tb = tb.rename(columns=columns)
    for name, meta in metadata.items():
        tb[name].metadata = meta

    tb = tb[pd.to_numeric(tb["year"], errors="coerce").notna()]
    tb["year"] = tb["year"].astype(int)
    for col in ["net_emigration", "net_emigration_per_1000"]:
        tb[col] = pr.to_numeric(tb[col], errors="coerce")
    tb = tb.dropna(subset=["net_emigration"])

    assert tb["year"].min() == 1541 and tb["year"].max() == 1870, "Expected coverage 1541-1870."
    assert len(tb) == 330, "Expected a continuous annual series (330 years)."
    # Spot-check the first value of Wrigley and Schofield's series (in thousands).
    assert abs(tb.loc[tb["year"] == 1541, "net_emigration"].item() - 3.3928) < 1e-6
    return tb


def parse_ons(snap: Snapshot) -> Table:
    """Parse Table 1 of the ONS spreadsheet: keep all-nationalities flows for calendar years."""
    tb = snap.read_excel(sheet_name="1", header=None, skiprows=6)
    tb = tb[[0, 1, 2]]
    metadata = {index: tb[index].metadata.copy() for index in tb.columns}
    tb = tb.rename(columns={0: "flow", 1: "period", 2: "value"})
    for index, name in zip(metadata, ["flow", "period", "value"]):
        tb[name].metadata = metadata[index]

    tb = tb.dropna(subset=["flow", "period"])
    # Keep year-ending-December periods, which correspond to calendar years. Recent periods carry
    # "P" (provisional) and "R" (revised) flags after the period label.
    tb = tb[tb["period"].astype(str).str.contains("YE Dec")].copy()
    # .str.extract returns a plain Series, so restore the column metadata afterwards.
    tb["year"] = 2000 + tb["period"].astype(str).str.extract(r"YE Dec (\d\d)")[0].astype(int)
    tb["year"] = tb["year"].copy_metadata(tb["period"])
    tb["value"] = pr.to_numeric(tb["value"], errors="raise")

    flows = {"Immigration": "immigration", "Emigration": "emigration", "Net migration": "net_migration"}
    observed = set(tb["flow"])
    assert observed == set(flows), f"Unexpected flow labels in the ONS table: {observed}"
    tb["flow"] = tb["flow"].map(flows)

    tb = tb.pivot(index="year", columns="flow", values="value").reset_index()
    tb = tb[["year", "immigration", "emigration", "net_migration"]]

    # The series starts in 2012; the last year will move forward with future ONS releases.
    assert tb["year"].min() == 2012 and tb["year"].max() >= 2025, "Expected coverage from 2012 to at least 2025."
    assert tb.notna().all().all(), "Missing values in the ONS series."
    residual = tb["net_migration"] - (tb["immigration"] - tb["emigration"])
    assert residual.abs().max() <= 1000, "Net migration does not match immigration minus emigration."
    # Loose spot-check on a settled year (exact values shift slightly with ONS revisions).
    assert 1_300_000 < tb.loc[tb["year"] == 2023, "immigration"].item() < 1_600_000
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap_boe = paths.load_snapshot("millennium_of_macroeconomic_data.xlsx")
    snap_ons = paths.load_snapshot("long_term_international_migration.xlsx")

    #
    # Process data.
    #
    tb_boe = parse_bank_of_england(snap_boe)
    tb_ons = parse_ons(snap_ons)
    tb_england = parse_england(snap_boe)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(
        tables=[
            tb_boe.format(["year"], short_name="bank_of_england_flows"),
            tb_ons.format(["year"], short_name="ons_flows"),
            tb_england.format(["year"], short_name="england_net_migration"),
        ],
        default_metadata=snap_boe.metadata,
    )
    ds_meadow.save()
