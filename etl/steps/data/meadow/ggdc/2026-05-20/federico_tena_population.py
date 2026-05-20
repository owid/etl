"""Load the Federico–Tena V2 (1991 borders) tab snapshot and reshape it to a long table.

Source file layout (tab-separated):
    Row 0: continent labels per column ("POPULATION" | "AFRICA" | ... | "BG" | "AMERICA" | ...)
    Row 1: country names at 1991 borders ("Borders 1991" | "Burundi" | ...)
    Row 2: alternative / historical names ("(000's omitted)" | "" | "French Somalia" | ...)
    Rows 3..141: year (1800–1938) and population values in thousands
    Rows 142+: blank/footer rows ("WORD COUNTRY Nº" enumeration)

The continent label "BG" marks blank separator columns between continents — they are dropped.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Continent labels that separate empty buffer columns in the source spreadsheet.
SEPARATOR_LABEL = "BG"

# Expected year range of the data block.
YEAR_MIN = 1800
YEAR_MAX = 1938


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("federico_tena_population.tab")

    # Read the whole sheet as raw text — the first 3 rows are a multi-row header.
    tb_raw = snap.read_csv(sep="\t", header=None, dtype=str, na_values=[""], keep_default_na=False)

    #
    # Process data.
    #
    tb = _reshape_to_long(tb_raw)

    # Values are reported in thousands of people; convert to absolute count.
    tb["population"] = (pr.to_numeric(tb["population"], errors="raise") * 1000).round().astype("int64")

    # Sanity checks.
    assert tb["year"].min() == YEAR_MIN, f"unexpected min year: {tb['year'].min()}"
    assert tb["year"].max() == YEAR_MAX, f"unexpected max year: {tb['year'].max()}"
    assert tb["population"].ge(0).all(), "negative population values"
    assert tb["country"].notna().all(), "missing country labels"

    tables = [tb.format(["country", "year"], short_name=paths.short_name)]

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)
    ds_meadow.save()


def _reshape_to_long(tb_raw: Table) -> Table:
    """Pivot the wide source layout into (country, year, population, continent)."""
    # Header rows.
    continents = tb_raw.iloc[0].tolist()
    countries_1991 = tb_raw.iloc[1].tolist()
    historical_names = tb_raw.iloc[2].tolist()

    # Data block: rows where the first column parses as a 4-digit year.
    body = tb_raw.iloc[3:].copy()
    body = body[body[0].astype(str).str.match(r"^\d{4}$", na=False)]
    body[0] = body[0].astype(int)
    body = body[body[0].between(YEAR_MIN, YEAR_MAX)]

    # Identify country columns (skip column 0 = year, and skip continent separators).
    country_cols = []
    for i in range(1, tb_raw.shape[1]):
        if continents[i] == SEPARATOR_LABEL:
            continue
        if not countries_1991[i] or countries_1991[i] == "":
            continue
        country_cols.append(i)

    # Melt to long form.
    body = body.rename(columns={0: "year"})
    long = body.melt(id_vars="year", value_vars=country_cols, var_name="col_idx", value_name="population")

    # Map the column index to its country and continent labels.
    long["country"] = long["col_idx"].map(lambda i: countries_1991[i])
    long["continent"] = long["col_idx"].map(lambda i: continents[i])
    long["historical_name"] = long["col_idx"].map(lambda i: historical_names[i] or None)
    long = long.drop(columns=["col_idx"])

    # Drop rows with no observation (Dataverse exports empty cells as "").
    long = long[long["population"].astype(str).str.len() > 0]
    long = long.dropna(subset=["population"])

    return long[["country", "year", "population", "continent", "historical_name"]]
