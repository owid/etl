"""Snapshot of Our World in Data's compilation of births and fertility rates from national statistics.

The data is compiled outside ETL and published as a public Google Sheet (see url_main in the .dvc file). This script
downloads that sheet as an xlsx file, and builds the origin's citation_full from its Data and Sources tabs: one line
per country, with each source, the years it covers, and the notes on that country's data.

NOTE: To update the data, rebuild and re-import the Google Sheet first (File > Import > Replace spreadsheet), then
rerun this script. The sheet's tabs and columns must stay as they are, or the meadow step and this script break.
"""

from pathlib import Path

import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

CITATION_INTRO = (
    "Our World in Data (2026). Births and fertility rates from national statistics. Compiled from the published "
    "figures of national statistics offices, civil registries, and health ministries: [spreadsheet with the data, "
    "sources, and notes for each year, and the reason each country is left out]({url}).\n\n"
    "Sources for each country, with the years each one covers, and notes on the data:"
)


def year_ranges(years) -> str:
    """Write a set of years as compact ranges, e.g. {2010, 2011, 2012, 2015} -> '2010–2012, 2015'."""
    years = sorted(set(int(y) for y in years))
    ranges = []
    start = prev = years[0]
    for year in years[1:]:
        if year != prev + 1:
            ranges.append((start, prev))
            start = year
        prev = year
    ranges.append((start, prev))
    return ", ".join(str(a) if a == b else f"{a}–{b}" for a, b in ranges)


def build_citation_full(data: pd.DataFrame, sources: pd.DataFrame, url: str) -> str:
    sources = sources.set_index("Source ID")
    lines = [CITATION_INTRO.format(url=url), ""]
    for country, tb in data.groupby("Country", sort=True):
        # Sources, in order of first use, with the years each covers.
        by_source = (
            tb.assign(source=tb["Sources"].str.split("; "))
            .explode("source")
            .dropna(subset=["source"])
            .groupby("source", sort=False)["Year"]
        )
        parts = []
        for source_id, years in by_source:
            producer, title, link = sources.loc[source_id, ["Producer", "Title", "URL"]]
            parts.append(f"[{producer}, {title}]({link}) ({year_ranges(years)})")
        line = f"- {country}: " + "; ".join(parts) + "."
        # Notes, with the years each applies to.
        notes = tb.dropna(subset=["Notes"]).groupby("Notes", sort=False)["Year"]
        if len(notes):
            line += " Notes: " + " ".join(f"{year_ranges(years)}: {note}" for note, years in notes)
        lines.append(line)
    return "\n".join(lines)


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"demography/{SNAPSHOT_VERSION}/births_fertility_national_vs_un.xlsx")

    # Download the public spreadsheet.
    snap.download_from_source()

    # Build the full citation from the spreadsheet, and rewrite the metadata (.dvc) file.
    sheets = pd.read_excel(snap.path, sheet_name=["Data", "Sources"])
    snap.metadata.origin.citation_full = build_citation_full(  # ty: ignore
        data=sheets["Data"],
        sources=sheets["Sources"],
        url=snap.metadata.origin.url_main,  # ty: ignore
    )

    # Add file to DVC and upload to S3.
    snap.create_snapshot(filename=snap.path, upload=upload)
