"""Load snapshots of the foreign-born population of the United States and create a meadow dataset.

Three snapshots are used, all with the same definition of foreign-born:
- Table 1 of the Census Bureau report "Historical Census Statistics on the Foreign-Born
  Population of the United States: 1850 to 2000": total and foreign-born population at each
  census.
- Table 4 of the same report: foreign-born population by world region and country of birth,
  at each census from 1850 to 1930 and from 1960 to 2000 (the 1940 and 1950 censuses only
  published this for the white population).
- American Community Survey 1-year estimates (table B05002), with the total and foreign-born
  population annually from 2005. The snapshot stores the Census API's own variable codes,
  which are renamed to readable column names here.
"""

import re

from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Census API variable codes of ACS table B05002, as stored in the snapshot.
ACS_COLUMNS = {
    "B05002_001E": "total_population",
    "B05002_013E": "foreign_born_population",
}


def parse_totals(snap: Snapshot) -> Table:
    """Parse Table 1: one row per census year, 1850 to 2000."""
    tb = snap.read_excel(header=None)

    # Data rows have a year (possibly with a "*" sample marker) in the first column; the
    # first block of rows holds counts, the second block percentages — keep counts only.
    rows = []
    for _, row in tb.iterrows():
        match = re.match(r"^(\d{4})\*?\.*", str(row[0]))
        if match:
            year = int(match.group(1))
            if any(r["year"] == year for r in rows):
                break  # the PERCENT block repeats the years; stop at the first repeat
            rows.append({"year": year, "total_population": int(row[1]), "foreign_born_population": int(row[7])})
    tb = snap.read_from_records(rows)

    assert set(tb["year"]) == set(range(1850, 2001, 10)), "Expected one row per census year, 1850-2000."
    # Spot-check against values printed in the report.
    assert tb.loc[tb["year"] == 1850, "total_population"].item() == 23_191_876
    assert tb.loc[tb["year"] == 2000, "foreign_born_population"].item() == 31_107_889
    return tb


def parse_by_country_of_birth(snap: Snapshot) -> Table:
    """Parse Table 4: foreign-born population by region and country of birth.

    All rows are kept as published, including region rows and residual rows; a `depth` column
    records each row's level in the source's hierarchy (number of leading dots). The garden
    step decides which rows to use.
    """
    tb = snap.read_excel(header=None)

    header_row = 4
    years = [int(str(c).replace("*", "")) for c in tb.iloc[header_row, 3:17]]
    assert years == [2000, 1990, 1980, 1970, 1960, 1930, 1920, 1910, 1900, 1890, 1880, 1870, 1860, 1850]

    rows = []
    for i in range(header_row + 1, len(tb)):
        label = str(tb.iloc[i, 2])
        if label == "nan" or label.startswith("Footnotes"):
            continue
        depth = len(re.match(r"^\s*(\.*)", label.replace(" ", "")).group(1))
        # Strip leading dots, trailing dot/ellipsis leaders, and footnote markers.
        country = re.sub(r"[.…\s]+$", "", label).lstrip(" .")
        country = re.sub(r"\s*\d+$", "", country).strip()
        for year, value in zip(years, tb.iloc[i, 3:17]):
            value = str(value).strip()
            if value in ("(NA)", "(X)", "nan"):
                value = None
            elif value == "-":
                value = 0  # the report uses "-" for zero
            else:
                value = int(float(value))
            rows.append({"country": country, "depth": depth, "year": year, "foreign_born_population": value})

    tb = snap.read_from_records(rows)

    assert tb.loc[(tb["country"] == "Total") & (tb["year"] == 1850), "foreign_born_population"].item() == 2_244_602
    assert tb.loc[(tb["country"] == "Mexico") & (tb["year"] == 2000), "foreign_born_population"].item() == 9_177_487
    return tb


def parse_acs_by_country_of_birth(snap: Snapshot) -> Table:
    """Parse the ACS by-country snapshot: one row per table line and year.

    The label hierarchy (e.g. "Estimate!!Total:!!Europe:!!Northern Europe:!!Denmark") is kept
    as a "path" column; the garden step decides which rows to use.
    """
    tb = snap.read_csv()

    def to_path(label: str) -> str:
        parts = [part.rstrip(":") for part in str(label).split("!!")]
        if parts[0] == "Estimate":
            parts = parts[1:]
        return " / ".join(parts)

    tb["path"] = tb["label"].apply(to_path)
    tb["country"] = tb["path"].str.split(" / ").str[-1]
    tb = tb[["year", "path", "country", "value"]].rename(columns={"value": "foreign_born_population"})
    tb["foreign_born_population"] = tb["foreign_born_population"].astype("Int64")

    assert tb.loc[(tb["path"] == "Total") & (tb["year"] == 2024), "foreign_born_population"].item() == 50_234_841
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap_totals = paths.load_snapshot("us_foreign_born_population_census.xls")
    snap_by_country = paths.load_snapshot("us_foreign_born_population_census_by_country.xls")
    snap_acs = paths.load_snapshot("us_foreign_born_population_american_community_survey.csv")
    snap_acs_by_country = paths.load_snapshot("us_foreign_born_population_by_country_american_community_survey.csv")

    #
    # Process data.
    #
    tb_totals = parse_totals(snap_totals)
    tb_by_country = parse_by_country_of_birth(snap_by_country)

    tb_acs = snap_acs.read_csv()
    tb_acs = tb_acs.rename(columns=ACS_COLUMNS)[["year", *ACS_COLUMNS.values()]].astype(int)

    tb_acs_by_country = parse_acs_by_country_of_birth(snap_acs_by_country)

    tables = [
        tb_totals.format(["year"], short_name="census"),
        tb_by_country.format(["country", "year"], short_name="census_by_country_of_birth"),
        tb_acs.format(["year"], short_name="american_community_survey"),
        tb_acs_by_country.format(["path", "year"], short_name="acs_by_country_of_birth"),
    ]

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_totals.metadata)
    ds_meadow.save()
