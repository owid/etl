"""Load a snapshot and create a meadow dataset."""

from bs4 import BeautifulSoup

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns of the table, from the class of each cell ("td-flat-rate", etc.).
COLUMNS = {
    "td-country": "country",
    "td-flat-rate": "contributory_flat_rate",
    "td-earnings-related": "contributory_earnings_related",
    "td-means-tested": "non_contributory_means_tested",
    "td-universal": "non_contributory_universal",
    "td-provident-funds": "provident_fund",
    "td-mandatory-occupational-pension": "mandatory_occupational_pension",
    "td-individual-accounts": "individual_accounts",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("old_age_pension_types.html")
    soup = BeautifulSoup(snap.path.read_text(), "html.parser")

    #
    # Process data.
    #
    # Read the table: one row per country, with the raw text of each cell (e.g. "X", "a X", "c g X" or "").
    rows = []
    for tr in soup.select("table tr"):
        cells = {cls: td for td in tr.find_all("td") for cls in td.get("class", []) if cls in COLUMNS}
        if "td-country" not in cells:
            continue
        rows.append({COLUMNS[cls]: td.get_text(" ", strip=True) for cls, td in cells.items()})
    tb = snap.read_from_records(rows)
    assert set(tb.columns) == set(COLUMNS.values()), "The columns of the table have changed."

    # Read the footnotes panel: the source note, the definition of each program type, and the footnote letters.
    tb_footnotes = snap.read_from_records(read_footnotes(soup))

    # Improve tables format.
    tables = [
        tb.format(["country"]),
        tb_footnotes.format(["key"], short_name="footnotes"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def read_footnotes(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Read the "Footnotes" panel next to the table into (key, text) records.

    The panel lists the source note ("SOURCE:"), a definition per program type ("Contributory flat-rate pension:"
    followed by its text), and footnotes "a." to "g.". Keys are "source", the program name, or the footnote letter.
    """
    heading = soup.find(
        lambda tag: tag.name in ("h2", "h3", "h4", "div", "span") and tag.get_text(strip=True) == "Footnotes"
    )
    assert heading is not None, "The footnotes panel was not found."
    # The panel's text comes after the heading, until the end of its container.
    container = heading.find_parent(lambda tag: "SOURCE:" in tag.get_text())
    assert container is not None, "The footnotes panel has no source note."
    lines = [line for line in container.get_text("\n", strip=True).split("\n") if line and line != "Footnotes"]

    records = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line == "SOURCE:":
            records.append({"key": "source", "text": lines[i + 1]})
            i += 2
        elif line == "NOTES:":
            records.append({"key": "notes", "text": lines[i + 1]})
            i += 2
        elif line.endswith(":"):
            records.append({"key": line.rstrip(":"), "text": lines[i + 1]})
            i += 2
        elif len(line) > 2 and line[1] == "." and line[0].isalpha():
            records.append({"key": line[0], "text": line[2:].strip()})
            i += 1
        else:
            raise AssertionError(f"Unexpected line in the footnotes panel: {line!r}")
    return records
