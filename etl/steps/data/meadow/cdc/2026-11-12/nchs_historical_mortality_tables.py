"""Load a snapshot and create a meadow dataset.

The snapshot holds NCHS's historical tables HIST290 (death rates) and HIST290A (deaths), one PDF per period of the
International Classification of Diseases. Every table has the same structure: for each cause, a block per race and
sex group, with one row per year and the total for all ages in the first number column. This step reads the total
for all races and both sexes, for every cause and year, and keeps the cause names as printed in each file.
"""

import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pdfplumber

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Header of the block that holds the totals for all races and both sexes.
BLOCK_ALL = re.compile(r"^(ALL RACES, |TOTAL, )?BOTH SEXES\b")
# Header of any other race or sex block.
BLOCK_OTHER = re.compile(
    r"^(ALL RACES, |TOTAL, )?(WHITE|ALL OTHER|BLACK|NONWHITE|MALE|FEMALE)\b|^(ALL RACES|TOTAL), (MALE|FEMALE)"
)
# A data row: year (with dot leaders in the 1979–98 files), then the all-ages total.
YEAR_ROW = re.compile(r"^(19\d\d)[\s\.]*\s+(\d{1,3}(?:,\d{3})*(?:\.\d)?)(\s|$)")
# Lines that are never cause names: page and column headers, notes, ICD revision tables.
NOT_A_CAUSE = re.compile(
    r"CAUSE OF DEATH|RACE, SEX|STATISTICAL WORKSHEET|TABLE|^\[|^\(|HIST290|PAGE|YEARS|ICD REVISION|"
    r"^(FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH)\s|^SOURCE|PERSON WITH|^NOTE|^\*",
    re.I,
)

# Number of PDF pages each file is expected to have; a different count means NCHS replaced the file.
EXPECTED_PAGES = {
    "hist290_death_rates_1900_1939.pdf": 154,
    "hist290_death_rates_1940_1949.pdf": 178,
    "hist290_death_rates_1950_1959.pdf": 210,
    "hist290_death_rates_1960_1967.pdf": 216,
    "hist290_death_rates_1968_1978.pdf": 427,
    "hist290_death_rates_1979_1998.pdf": 1005,
    "hist290a_deaths_1900_1939.pdf": 154,
    "hist290a_deaths_1940_1949.pdf": 178,
    "hist290a_deaths_1950_1959.pdf": 210,
    "hist290a_deaths_1960_1967.pdf": 213,
    "hist290a_deaths_1968_1978.pdf": 431,
    "hist290a_deaths_1979_1998.pdf": 1005,
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("nchs_historical_mortality_tables.zip")

    #
    # Process data.
    #
    # Parse every PDF in the archive (in parallel, since the files are long).
    with snap.extracted() as archive:
        files = sorted(archive.path.glob("*.pdf"))
        assert {f.name for f in files} == set(EXPECTED_PAGES), "Unexpected set of files in the archive."
        with ProcessPoolExecutor() as executor:
            records = [r for file_records in executor.map(parse_pdf, files) for r in file_records]

    # Build the table from the parsed rows.
    tb = snap.read_from_records(records)
    tb["cause"] = tb["cause"].astype("category")
    tb["period"] = tb["period"].astype("category")
    tb["measure"] = tb["measure"].astype("category")

    sanity_check_outputs(tb)

    tb = tb.format(["measure", "period", "cause", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def parse_pdf(path: Path) -> list[dict]:
    """Read the all-ages total for all races and both sexes, for every cause and year in one table."""
    measure = "death_rate" if path.name.startswith("hist290_") else "deaths"
    period = re.search(r"(\d{4}_\d{4})", path.name).group(1).replace("_", "-")  # type: ignore

    with pdfplumber.open(path) as pdf:
        expected_pages = EXPECTED_PAGES[path.name]
        assert len(pdf.pages) == expected_pages, f"{path.name}: {len(pdf.pages)} pages, expected {expected_pages}."
        lines = [line.strip() for page in pdf.pages for line in (clean_page(page).extract_text() or "").splitlines()]

    records = []
    cause, in_block, candidate, previous_year = None, False, None, None
    for line in lines:
        if not line:
            continue
        if BLOCK_ALL.match(line):
            # The cause name is the last text line before the block.
            in_block, cause, previous_year = True, candidate, None
            continue
        if BLOCK_OTHER.match(line):
            in_block = False
            continue
        match = YEAR_ROW.match(line)
        if match:
            if in_block and cause:
                year = int(match.group(1))
                # NOTE: The 1960–67 tables print "1961" twice for heart disease, where the second row is 1960. Rows
                # within a block run from the latest year down, so a repeated year is the next one down.
                if year == previous_year:
                    year = previous_year - 1
                previous_year = year
                records.append(
                    {
                        "measure": measure,
                        "period": period,
                        "cause": cause,
                        "year": year,
                        "value": float(match.group(2).replace(",", "")),
                    }
                )
            continue
        if re.search(r"[A-Za-z]{4,}", line) and not NOT_A_CAUSE.search(line):
            # Normalize the cause line: drop the printer control "0" that starts some lines in the 1979–98 files.
            candidate = re.sub(r"^0\s*(?=[A-Z])", "", re.sub(r"\s+", " ", line))

    # A cause block can repeat across pages; keep the first occurrence of each (cause, year).
    seen, unique = set(), []
    for r in records:
        key = (r["cause"], r["year"])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def clean_page(page):
    """Remove the characters that garble the text of some rows.

    Some rows are printed in bold as two overlapping copies of each character, less than 0.1 points apart ("1939"
    reads "11993399"). Real neighboring characters are more than 1 point apart, so a tight tolerance removes only the
    copies. Pages also hold tiny invisible spaces above and below the text, which can split a row into two lines.
    """
    page = page.filter(lambda obj: not (obj.get("object_type") == "char" and obj.get("size", 5) < 2))
    return page.dedupe_chars(tolerance=0.2)


def sanity_check_outputs(tb) -> None:
    # Every file contributes, and covers exactly its period's years.
    for (measure, period), group in tb.groupby(["measure", "period"], observed=True):
        start, end = (int(y) for y in period.split("-"))
        years = set(group["year"])
        assert years <= set(range(start, end + 1)), f"{measure} {period}: years outside the period."
        assert (group["cause"] == "ALL CAUSES").any(), f"{measure} {period}: no all-causes total found."
        totals = group[group["cause"] == "ALL CAUSES"]
        assert set(totals["year"]) == set(range(start, end + 1)), f"{measure} {period}: missing all-causes years."
    assert set(tb["period"]) == {"1900-1939", "1940-1949", "1950-1959", "1960-1967", "1968-1978", "1979-1998"}
    # Values are positive, and no cause exceeds the all-causes total in the same year.
    assert (tb["value"] >= 0).all(), "Negative values parsed."
    total = tb[tb["cause"] == "ALL CAUSES"].set_index(["measure", "year"])["value"]
    joined = tb.join(total.rename("total"), on=["measure", "year"])
    over = joined[joined["value"] > joined["total"] * 1.0001]
    assert over.empty, f"Cause values above the all-causes total: {over.head().to_dict('records')}"
