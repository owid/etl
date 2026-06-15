"""Parse the CDC Ebola outbreak chronology (HTML) into one row per outbreak.

The CDC page is an HTML accordion: one ``h3`` per year, and within each year one ``h4`` per
country/outbreak followed by ``li`` items labelled "Species:", "Reported number of cases:" and
"Reported number of deaths and percentage of fatal cases:". We walk the elements in document
order, starting a new outbreak at each ``h4`` and attaching the labelled values that follow.

This step does the scraping, so the integrity checks that guard against silent parser drift
(page restructured, labels renamed) live here.
"""

import re

import pandas as pd
from bs4 import BeautifulSoup
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def _to_int(text: str) -> int | None:
    """First integer in a string.

    Collapses separators that sit *between* digits — thousands commas and the stray spaces the page
    sometimes leaves where a footnote superscript split a number (e.g. '3,470*' -> 3470,
    '1 2 confirmed' -> 12) — then reads the first integer.
    """
    cleaned = re.sub(r"(?<=\d)[\s,](?=\d)", "", text)
    match = re.search(r"\d+", cleaned)
    return int(match.group()) if match else None


def _parse(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for accordion in soup.select("h3.accordion-header"):
        year_match = re.search(r"(?:19|20)\d{2}", accordion.get_text(" ", strip=True))
        if not year_match:
            continue
        year = int(year_match.group())
        item = accordion.find_parent(class_="accordion-item")
        if item is None:
            continue

        current: dict | None = None
        for el in item.find_all(["h4", "li"]):
            if el.name == "h4":
                if current is not None:
                    rows.append(current)
                current = {
                    "year": year,
                    "country": el.get_text(" ", strip=True),
                    "species": None,
                    "cases": None,
                    "deaths": None,
                    "cfr_reported": None,
                }
            elif current is not None:
                text = el.get_text(" ", strip=True)
                low = text.lower()
                value = text.split(":", 1)[1].strip() if ":" in text else text
                if low.startswith("species"):
                    current["species"] = value
                elif "death" in low:
                    current["deaths"] = _to_int(value)
                    pct = re.search(r"\((\d+)\s*%\)", value)
                    current["cfr_reported"] = int(pct.group(1)) if pct else None
                elif "case" in low:
                    current["cases"] = _to_int(value)
        if current is not None:
            rows.append(current)

    return pd.DataFrame(rows)


def _sanity_check(df: pd.DataFrame) -> None:
    assert len(df) >= 50, f"Parsed only {len(df)} outbreaks; the CDC page structure may have changed."
    assert df["species"].notna().all(), "Some outbreaks have no species — the 'Species:' label may have changed."
    assert df["country"].notna().all() and (df["country"].str.len() > 0).all(), "Empty country name parsed."
    assert df["year"].between(1976, 2100).all(), "Outbreak year outside the plausible range."
    # All known Ebola species are 'Orthoebolavirus ...'; flags relabelling/leakage from narrative text.
    assert df["species"].str.startswith("Orthoebolavirus").all(), f"Unexpected species value(s): {set(df['species'])}"
    # Deaths cannot exceed cases for any outbreak where both are reported.
    both = df.dropna(subset=["cases", "deaths"])
    assert (both["deaths"] <= both["cases"]).all(), "An outbreak reports more deaths than cases — parser misalignment."


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("ebola_outbreaks.html")
    with open(snap.path, encoding="utf-8") as f:
        df = _parse(f.read())

    _sanity_check(df)

    #
    # Process data.
    #
    # A few country-years contain two distinct outbreaks; index them so the meadow key is unique.
    df["outbreak_index"] = df.groupby(["country", "year"]).cumcount()

    tb = Table(df, short_name="ebola_outbreaks")
    # Built from a fresh DataFrame, so columns carry no origin — attach the snapshot's.
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]
    tb = tb.format(["country", "year", "outbreak_index"], short_name="ebola_outbreaks")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
