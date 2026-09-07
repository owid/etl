"""Load snapshots of the DHS Yearbook of Immigration Statistics and create a meadow dataset.

Two snapshots are used:
- The 2024 yearbook workbook, whose Table 1 gives annual totals for fiscal years 1820-2024.
- The 2020 yearbook tables, whose Table 2 gives flows by region and country of last
  residence for each decade from the 1820s to the 2010s (later editions dropped this
  historical breakdown).
"""

import re

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def parse_annual_totals(snap: Snapshot) -> Table:
    """Parse Table 1 of the 2024 workbook: annual totals laid out as four Year|Number column pairs."""
    tb = snap.read(sheet_name="Table 1", skiprows=5)

    # Stack the four Year|Number column pairs into one long table.
    pairs = []
    for i in range(4):
        suffix = "" if i == 0 else f".{i}"
        pair = tb[[f"Year{suffix}", f"Number{suffix}"]].rename(
            columns={f"Year{suffix}": "year", f"Number{suffix}": "immigrants"}
        )
        pairs.append(pair)
    tb = pr.concat(pairs, ignore_index=True)

    # Extract the year, dropping footnote markers inside cells (e.g. "1976 1", the 15-month
    # transition year) and the footnote rows below the data (text in the "Year" column).
    tb["year"] = pr.to_numeric(tb["year"].astype(str).str.extract(r"^(\d{4})", expand=False), errors="coerce")
    tb = tb.dropna(subset=["year"])
    tb["year"] = tb["year"].astype(int)
    tb["immigrants"] = pr.to_numeric(tb["immigrants"])

    # The table covers every fiscal year from 1820 to 2024, with no gaps.
    assert tb["year"].min() == 1820 and tb["year"].max() == 2024, "Unexpected year range in Table 1."
    assert len(tb) == 2024 - 1820 + 1, "Unexpected number of years in Table 1."
    assert not tb["year"].duplicated().any(), "Duplicate years in Table 1."
    assert tb["immigrants"].notna().all() and (tb["immigrants"] > 0).all(), "Missing or non-positive values in Table 1."

    return tb


def parse_by_country_of_origin(snap: Snapshot) -> Table:
    """Parse Table 2 of the 2020 yearbook: decadal flows by region and country of last residence.

    All rows are kept as published (including the Total row, DHS's own region rows, and
    residual "Other ..." rows); the garden step decides how to use them.
    """
    with snap.extracted():
        tb = snap.read_from_archive("fy2020_table2.xlsx", sheet_name="Table 2", skiprows=3)

    tb = tb.rename(columns={tb.columns[0]: "country"})

    # Drop footnote rows below the data (they have no values in the decade columns).
    decade_cols = [c for c in tb.columns if re.match(r"^\d{4} to \d{4}$", str(c))]
    assert len(decade_cols) == 20, "Expected 20 decade columns (1820s to 2010s) in Table 2."
    tb = tb.dropna(subset=decade_cols, how="all")

    # Drop the trailing single-year column ("2020"): it is not a decade, and annual data
    # comes from Table 1 of the 2024 yearbook instead.
    tb = tb[["country"] + decade_cols]

    # Clean row labels: strip footnote markers (e.g. "Russia3,6,10") and surrounding spaces.
    tb["country"] = tb["country"].astype(str).str.strip().str.replace(r"[\d,\s]+$", "", regex=True)

    # Reshape to long format, using the first year of each decade.
    tb = tb.melt(id_vars=["country"], value_vars=decade_cols, var_name="decade", value_name="immigrants")
    tb["decade"] = tb["decade"].str[:4].astype(int)
    tb["immigrants"] = pr.to_numeric(tb["immigrants"], errors="coerce")

    assert set(tb["decade"]) == set(range(1820, 2020, 10)), "Unexpected decades in Table 2."
    assert "Total" in set(tb["country"]), "Total row missing from Table 2."

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap_2024 = paths.load_snapshot("yearbook_lawful_permanent_residents_2024.xlsx")
    snap_2020 = paths.load_snapshot("yearbook_lawful_permanent_residents_2020.zip")

    #
    # Process data.
    #
    tb_total = parse_annual_totals(snap_2024)
    tb_by_country = parse_by_country_of_origin(snap_2020)

    tables = [
        tb_total.format(["year"], short_name="annual_totals"),
        tb_by_country.format(["country", "decade"], short_name="by_country_of_origin"),
    ]

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_2024.metadata)
    ds_meadow.save()
