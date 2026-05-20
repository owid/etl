"""Load Federico–Tena V2 (1991 borders) and the companion Quality Assessment file.

Two snapshots, two meadow tables:

- ``federico_tena_population``: annual population 1800–1938 at 1991 borders.
  Source layout (tab-separated):
      Row 0: continent labels per column ("POPULATION" | "AFRICA" | ... | "BG" | ...)
      Row 1: country names at 1991 borders
      Row 2: alternative / historical names (column 0 holds "(000's omitted)")
      Rows 3..141: year (1800–1938) and population values in thousands

- ``federico_tena_population_quality``: per (polity, year) reliability classes A–E (plus
  "NE" = not estimated), at 1938 historical borders (note: different from the main table's
  1991 borders, so country lists don't align one-to-one). Source layout (xlsx, sheet
  "Quality Assessment"):
      Row 0: continent labels
      Row 1: country name at 1938 borders
      Row 2: 1991 / current-name hint in parentheses
      Rows 3..141: year and quality class

The continent label "BG" marks blank separator columns and is dropped.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

SEPARATOR_LABEL = "BG"
YEAR_MIN = 1800
YEAR_MAX = 1938
QA_SHEET = "Quality Assessment"
QA_VALID_CLASSES = {"A", "B", "C", "D", "E", "NE"}


def run() -> None:
    #
    # Load inputs.
    #
    snap_pop = paths.load_snapshot("federico_tena_population.tab")
    snap_qa = paths.load_snapshot("federico_tena_population_quality.xlsx")

    tb_pop_raw = snap_pop.read_csv(sep="\t", header=None, dtype=str, na_values=[""], keep_default_na=False)
    tb_qa_raw = snap_qa.read_excel(sheet_name=QA_SHEET, header=None, dtype=str)

    #
    # Process data.
    #
    tb_pop = _reshape_population(tb_pop_raw)
    tb_pop["population"] = (pr.to_numeric(tb_pop["population"], errors="raise") * 1000).round().astype("int64")
    _check_year_range(tb_pop, "population")
    assert tb_pop["population"].ge(0).all(), "negative population values"

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
    """Reshape the wide tab layout into (country, year, population, continent, historical_name)."""
    continents = tb_raw.iloc[0].tolist()
    countries_1991 = tb_raw.iloc[1].tolist()
    historical_names = tb_raw.iloc[2].tolist()

    body = tb_raw.iloc[3:].copy()
    body = body[body[0].astype(str).str.match(r"^\d{4}$", na=False)]
    body[0] = body[0].astype(int)
    body = body[body[0].between(YEAR_MIN, YEAR_MAX)]

    country_cols = [
        i for i in range(1, tb_raw.shape[1]) if continents[i] != SEPARATOR_LABEL and countries_1991[i] not in (None, "")
    ]

    body = body.rename(columns={0: "year"})
    long = body.melt(id_vars="year", value_vars=country_cols, var_name="col_idx", value_name="population")

    long["country"] = long["col_idx"].map(lambda i: str(countries_1991[i]).strip())
    long["continent"] = long["col_idx"].map(lambda i: continents[i])
    long["historical_name"] = long["col_idx"].map(lambda i: (historical_names[i] or "").strip() or None)
    long = long.drop(columns=["col_idx"])

    long = long[long["population"].astype(str).str.len() > 0]
    long = long.dropna(subset=["population"])

    return long[["country", "year", "population", "continent", "historical_name"]]


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
        i
        for i in range(1, tb_raw.shape[1])
        if continents[i] not in (None, "", SEPARATOR_LABEL) and historical_names[i] not in (None, "")
    ]

    body = body.rename(columns={0: "year"})
    long = body.melt(id_vars="year", value_vars=country_cols, var_name="col_idx", value_name="quality_class")

    long["country"] = long["col_idx"].map(lambda i: str(historical_names[i]).strip())
    long["continent"] = long["col_idx"].map(lambda i: continents[i])
    long["current_name_hint"] = long["col_idx"].map(
        lambda i: (current_hints[i] or "").strip().strip("()").strip() or None
    )
    long = long.drop(columns=["col_idx"])

    long["quality_class"] = long["quality_class"].astype(str).str.strip()
    long = long[long["quality_class"].str.len() > 0]

    return long[["country", "year", "quality_class", "continent", "current_name_hint"]]


def _check_year_range(tb: Table, value_col: str) -> None:
    assert tb["year"].min() == YEAR_MIN, f"unexpected min year in {value_col}: {tb['year'].min()}"
    assert tb["year"].max() == YEAR_MAX, f"unexpected max year in {value_col}: {tb['year'].max()}"
    assert tb["country"].notna().all(), f"missing country labels in {value_col}"
