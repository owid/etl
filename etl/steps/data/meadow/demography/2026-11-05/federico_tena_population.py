"""Load Federico–Tena (1991 borders, November 2026 revision) and the companion Quality Assessment file.

Two snapshots, two meadow tables:

- ``federico_tena_population``: annual population 1800-1938 at 1991 borders.
  Source layout (xlsx, sheet "COUNTRIES BY CONTINENT"):
      Row 0: continent labels per column ("POPULATION" | "AFRICA" | ... )
      Row 1: country names at 1991 borders
      Row 2: unit marker in column 0 ("(000's omitted)"), plus labels for the diagnostic
             columns on the right (country sum, world totals, reconciliation checks)
      Rows 3..141: year (1800-1938) and population values in thousands

  The January 2026 release carried a third header row of alternative / historical names; the
  November 2026 workbook dropped it, so this step has no ``historical_name`` column.

- ``federico_tena_population_quality``: per (polity, year) reliability classes A-E (plus
  "NE" = not estimated), at 1938 historical borders (note: different from the main table's
  1991 borders, so country lists don't align one-to-one). Source layout (xlsx, sheet
  "Quality Assessment"):
      Row 0: continent labels
      Row 1: country name at 1938 borders
      Row 2: 1991 / current-name hint in parentheses
      Rows 3..141: year and quality class

  The November 2026 package did not include a new Quality Assessment file, so this step reads
  the one snapshotted on 2026-05-20, which is still the current published version.

Both sheets interleave real polity columns with blank spacer columns and, on the right,
aggregate/diagnostic columns (continent and world totals, reconciliation checks). We keep only
columns whose row-0 label is one of the five real continents and whose country name (row 1) is
non-blank — this drops every spacer and every aggregate column in one go.
"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

CONTINENTS = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "OCEANIA"}
YEAR_MIN = 1800
YEAR_MAX = 1938

POP_SHEET = "COUNTRIES BY CONTINENT"
SUMMARY_SHEET = "SUMMARY WORLD AN CONTINENTS"
QA_SHEET = "Quality Assessment"
QA_VALID_CLASSES = {"A", "B", "C", "D", "E", "NE"}


def _blank(x) -> bool:
    """True for None, NaN, or whitespace-only cells (header cells come through as str or NaN)."""
    return x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip() == ""


def _clean(x):
    """Stripped string, or None if blank."""
    return None if _blank(x) else str(x).strip()


def run() -> None:
    #
    # Load inputs.
    #
    snap_pop = paths.load_snapshot("federico_tena_population.xlsx")
    snap_qa = paths.load_snapshot("federico_tena_population_quality.xlsx")

    tb_pop_raw = snap_pop.read_excel(sheet_name=POP_SHEET, header=None, dtype=object)
    tb_summary_raw = snap_pop.read_excel(sheet_name=SUMMARY_SHEET, header=None, dtype=object)
    tb_qa_raw = snap_qa.read_excel(sheet_name=QA_SHEET, header=None, dtype=str)

    #
    # Process data.
    #
    tb_pop = _reshape_population(tb_pop_raw)
    tb_pop["population"] = (pr.to_numeric(tb_pop["population"], errors="raise") * 1000).round().astype("int64")
    _check_year_range(tb_pop, "population")
    assert tb_pop["population"].ge(0).all(), "negative population values"
    _check_against_summary(tb_pop, tb_summary_raw)

    tb_qa = _reshape_quality(tb_qa_raw)
    _check_year_range(tb_qa, "quality_class")
    unexpected = set(tb_qa["quality_class"].unique()) - QA_VALID_CLASSES
    assert not unexpected, f"unexpected quality classes: {unexpected}"

    tables = [
        tb_pop.format(["country", "year"], short_name="federico_tena_population"),
        tb_qa.format(["country", "year"], short_name="federico_tena_population_quality"),
    ]

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_pop.metadata)
    ds_meadow.save()


def _reshape_population(tb_raw: Table) -> Table:
    """Reshape the wide sheet layout into (country, year, population, continent)."""
    continents = tb_raw.iloc[0].tolist()
    countries_1991 = tb_raw.iloc[1].tolist()

    body = tb_raw.iloc[3:].copy()
    body = body[body[0].astype(str).str.match(r"^\d{4}(\.0)?$", na=False)]
    body[0] = body[0].astype(float).astype(int)
    body = body[body[0].between(YEAR_MIN, YEAR_MAX)]

    country_cols = [
        i for i in range(1, tb_raw.shape[1]) if _clean(continents[i]) in CONTINENTS and not _blank(countries_1991[i])
    ]

    body = body.rename(columns={0: "year"})
    long = body.melt(id_vars="year", value_vars=country_cols, var_name="col_idx", value_name="population")

    long["country"] = long["col_idx"].map(lambda i: str(countries_1991[i]).strip())
    long["continent"] = long["col_idx"].map(lambda i: _clean(continents[i]))
    long = long.drop(columns=["col_idx"])

    long = long.dropna(subset=["population"])
    long = long[long["population"].astype(str).str.strip() != ""]

    return long[["country", "year", "population", "continent"]]


def _reshape_quality(tb_raw: Table) -> Table:
    """Reshape the QA wide xlsx layout into (country, year, quality_class, continent, current_name_hint).

    Country names here are 1938 historical labels. Row 2 of the source contains a parenthetical
    current-name hint (e.g. "(Lesotho)" under "Basutoland") which we surface as ``current_name_hint``.
    """
    continents = tb_raw.iloc[0].tolist()
    historical_names = tb_raw.iloc[1].tolist()
    current_hints = tb_raw.iloc[2].tolist()

    body = tb_raw.iloc[3:].copy()
    body = body[body[0].astype(str).str.match(r"^\d{4}$", na=False)]
    body[0] = body[0].astype(int)
    body = body[body[0].between(YEAR_MIN, YEAR_MAX)]

    country_cols = [
        i for i in range(1, tb_raw.shape[1]) if continents[i] in CONTINENTS and not _blank(historical_names[i])
    ]

    body = body.rename(columns={0: "year"})
    long = body.melt(id_vars="year", value_vars=country_cols, var_name="col_idx", value_name="quality_class")

    long["country"] = long["col_idx"].map(lambda i: str(historical_names[i]).strip())
    long["continent"] = long["col_idx"].map(lambda i: continents[i])
    long["current_name_hint"] = long["col_idx"].map(
        lambda i: (_clean(current_hints[i]) or "").strip("()").strip() or None
    )
    long = long.drop(columns=["col_idx"])

    # drop blank / not-estimated-as-blank cells (kept "NE" cells are explicit and valid)
    long = long.dropna(subset=["quality_class"])
    long["quality_class"] = long["quality_class"].astype(str).str.strip()
    long = long[long["quality_class"].str.len() > 0]

    return long[["country", "year", "quality_class", "continent", "current_name_hint"]]


def _check_year_range(tb: Table, value_col: str) -> None:
    assert tb["year"].min() == YEAR_MIN, f"unexpected min year in {value_col}: {tb['year'].min()}"
    assert tb["year"].max() == YEAR_MAX, f"unexpected max year in {value_col}: {tb['year'].max()}"
    assert tb["country"].notna().all(), f"missing country labels in {value_col}"


def _check_against_summary(tb_pop: Table, tb_summary_raw: Table) -> None:
    """Reconcile the parsed country columns against the workbook's own summary sheet.

    The summary sheet publishes, per year, the number of polities and the world total. If our
    column filter ever picked up an aggregate column or dropped a real polity, these disagree.
    """
    summary = tb_summary_raw.iloc[4:].copy()
    summary = summary[summary[0].astype(str).str.match(r"^\d{4}(\.0)?$", na=False)]
    summary[0] = summary[0].astype(float).astype(int)
    summary = summary[summary[0].between(YEAR_MIN, YEAR_MAX)]

    # Column 6 is the "WORLD" polity count; column 14 (second block) is the world population total.
    summary = summary.set_index([0])

    n_polities = summary[6].astype(int)
    parsed_counts = tb_pop.groupby("year")["country"].nunique()
    mismatch = {
        int(y): (int(parsed_counts[y]), int(n_polities[y]))
        for y in n_polities.index
        if parsed_counts[y] != n_polities[y]
    }
    assert not mismatch, f"polity count differs from the summary sheet (year: parsed vs published): {mismatch}"

    world_published = pr.to_numeric(summary[14], errors="raise").astype(float) * 1000
    world_parsed = tb_pop.groupby("year")["population"].sum().astype(float)
    rel = ((world_parsed - world_published) / world_published).abs()
    assert rel.max() < 1e-6, f"world total differs from the summary sheet by up to {rel.max():.2%}"
