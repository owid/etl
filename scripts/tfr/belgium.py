"""Belgium: Statbel's own fertility rate, from one workbook per year.

Statbel publishes a births-and-fertility workbook for each year since 2011. Its last sheet gives the
fertility rate for the country and each region, split by whether the mother is Belgian or foreign —
and the split is wide, 1.33 against 1.89 in 2024.

The filenames are not systematic: the suffix has changed several times between definitive and
provisional editions, so the list is scraped from the catalogue page rather than generated.

The rate itself can be rebuilt — Statbel publishes births by single year of the mother's age in the
same workbook, and population by single year of age in its open data. Doing that for one year
reproduces its published figure, but the population files are hundred-megabyte register extracts, one
per year, so the series here is the published one.
"""

import os
import re
import urllib.parse

import pandas as pd

from fetch import fetch

DATA = os.path.join(os.path.dirname(__file__), "data", "be")
CATALOG = "https://statbel.fgov.be/fr/themes/population/natalite-et-fecondite"
COUNTRY, ALL_MOTHERS = "Belgique", "Mères belges et étrangères"
FIRST = 2011


def _workbooks():
    """{year: url} for every yearly workbook the catalogue links to."""
    path = fetch(CATALOG, os.path.join(DATA, "catalog.html"))
    html = open(path, encoding="utf-8", errors="replace").read()
    out = {}
    for href in set(re.findall(r'href="([^"]*?\.xlsx?)"', html)):
        name = urllib.parse.unquote(href)
        if "condit" not in name:
            continue
        m = re.search(r"_(20\d{2})", name)
        if not m:
            continue
        # the catalogue's own links are already percent-encoded, so they are used as they are
        out[int(m.group(1))] = href if href.startswith("http") else "https://statbel.fgov.be" + href
    return out


def _indicator(url, year):
    """The country's own fertility rate for the year, from the workbook's indicator sheet.

    Which sheet that is has moved between editions, so every sheet is checked for the heading
    instead, starting from the last — where it sits in the recent ones.
    """
    suffix = ".xlsx" if url.endswith(".xlsx") else ".xls"
    book = pd.ExcelFile(fetch(url, os.path.join(DATA, f"y{year}{suffix}")))
    for sheet in reversed(book.sheet_names):
        d = book.parse(sheet_name=sheet, header=None)
        header = next((i for i in range(min(8, len(d)))
                       if any("conjoncturel" in str(v) for v in d.iloc[i])), None)
        if header is None:
            continue
        column = next(j for j in range(d.shape[1]) if "conjoncturel" in str(d.iloc[header, j]))
        for i in range(header + 1, len(d)):
            if str(d.iloc[i, 0]).strip() != COUNTRY:
                continue
            if str(d.iloc[i, 1]).strip() not in (ALL_MOTHERS, "nan"):
                continue
            v = pd.to_numeric(d.iloc[i, column], errors="coerce")
            if pd.notna(v):
                return float(v)
    return None


def belgium_tfr():
    rows = []
    for year, url in sorted(_workbooks().items()):
        if year < FIRST:
            continue
        value = _indicator(url, year)
        if value:
            rows.append({"year": year, "value": value})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    t = belgium_tfr()
    print(t.to_string(index=False))
    print("Statbel publishes 1.44 for 2024 — 1.33 for Belgian mothers and 1.89 for foreign ones")
