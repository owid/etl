"""Load snapshots of the foreign-born population of the United States and create a meadow dataset.

Two snapshots are used:
- The Census Bureau's working paper "Historical Census Statistics on the Foreign-Born Population
  of the United States: 1850 to 2000" (a PDF), whose Table 1 gives the total and foreign-born
  population at each census.
- American Community Survey 1-year estimates (table B05002), with the same two series annually
  from 2005. The snapshot stores the Census API's own variable codes, which are renamed to
  readable column names here.
"""

import re

import pdfplumber
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

paths = PathFinder(__file__)

# Census API variable codes of ACS table B05002, as stored in the snapshot.
ACS_COLUMNS = {
    "B05002_001E": "total_population",
    "B05002_013E": "foreign_born_population",
}


def parse_census_table(snap: Snapshot) -> Table:
    """Parse Table 1 of the working paper: one row per census year, 1850 to 2000."""
    rows = []
    with pdfplumber.open(snap.path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text.lstrip().startswith("Table 1. Nativity of the Population"):
                continue
            in_numbers = False
            for line in text.split("\n"):
                if line.strip() == "NUMBER":
                    in_numbers = True
                    continue
                if line.strip().startswith("PERCENT"):
                    break
                match = re.match(r"^(\d{4})", line.strip())
                if in_numbers and match:
                    # e.g. "1970* 3 ......... 203,210,158 ... 9,619,302": the first large number is
                    # the total population, the last is the foreign-born population.
                    numbers = re.findall(r"\d[\d,]{6,}", line)
                    rows.append(
                        {
                            "year": int(match.group(1)),
                            "total_population": int(numbers[0].replace(",", "")),
                            "foreign_born_population": int(numbers[-1].replace(",", "")),
                        }
                    )
            break

    tb = snap.read_from_records(rows)

    assert set(tb["year"]) == set(range(1850, 2001, 10)), "Expected one row per census year, 1850-2000."
    # Spot-check against values printed in the report.
    assert tb.loc[tb["year"] == 1850, "total_population"].item() == 23_191_876
    assert tb.loc[tb["year"] == 2000, "foreign_born_population"].item() == 31_107_889
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    snap_census = paths.load_snapshot("us_foreign_born_population_census.pdf")
    snap_acs = paths.load_snapshot("us_foreign_born_population_american_community_survey.csv")

    #
    # Process data.
    #
    tb_census = parse_census_table(snap_census)
    tb_acs = snap_acs.read_csv()
    tb_acs = tb_acs.rename(columns=ACS_COLUMNS)[["year", *ACS_COLUMNS.values()]].astype(int)

    tables = [
        tb_census.format(["year"], short_name="census"),
        tb_acs.format(["year"], short_name="american_community_survey"),
    ]

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_census.metadata)
    ds_meadow.save()
