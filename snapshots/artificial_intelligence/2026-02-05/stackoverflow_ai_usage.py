"""Script to create a snapshot of Stack Overflow Developer Survey data on AI tool usage.

Scrapes the "Do you currently use AI tools in your development process?" question
from the Professional Developers tab of the Stack Overflow Developer Survey results
for 2023, 2024, and 2025.

The survey pages use different rendering approaches per year:
    - 2023: HTML <table> with data-percentage attributes
    - 2024/2025: Inline SVG bar charts; percentages in <text> elements inside <g> groups

Sources:
    2023: https://survey.stackoverflow.co/2023/
    2024: https://survey.stackoverflow.co/2024/ai
    2025: https://survey.stackoverflow.co/2025/ai
"""

from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

SNAPSHOT_VERSION = Path(__file__).parent.name

SURVEY_URLS = {
    2023: "https://survey.stackoverflow.co/2023/",
    2024: "https://survey.stackoverflow.co/2024/ai",
    2025: "https://survey.stackoverflow.co/2025/ai",
}

# The anchor / element id for the Professional Developers chart in each year
PROF_DEV_ID = "sentiment-and-usage-ai-sel-prof"



def _scrape_2023(soup: BeautifulSoup) -> tuple[list[dict], int]:
    """Parse the 2023 survey page (HTML table with data-percentage attributes)."""
    fig = soup.find("figure", id=lambda x: x and PROF_DEV_ID in x)
    if not fig:
        raise ValueError("Could not find Professional Developers figure in 2023 survey")

    # Response count
    resp_span = fig.find("span", class_="p-ff-roboto-slab")
    n_total = int(resp_span.text.strip().replace(",", "")) if resp_span else None

    rows = []
    table = fig.find("table")
    for tr in table.find_all("tr"):
        label_td = tr.find("td", class_="label")
        bar_td = tr.find("td", class_="bar")
        if label_td and bar_td:
            label = label_td.text.strip()
            pct = float(bar_td["data-percentage"])
            rows.append({"response": label, "share_pct": pct})

    return rows, n_total


def _scrape_svg_chart(soup: BeautifulSoup) -> tuple[list[dict], int]:
    """Parse 2024/2025 survey pages (inline SVG charts inside #sentiment-and-usage-ai-sel-prof)."""
    target = soup.find(id=PROF_DEV_ID)
    if not target:
        raise ValueError("Could not find #sentiment-and-usage-ai-sel-prof div")

    # Response count: a <span> whose text is a number with commas
    n_total = None
    for span in target.find_all("span"):
        txt = (span.string or "").strip()
        if txt.replace(",", "").isdigit() and "," in txt:
            n_total = int(txt.replace(",", ""))
            break

    # Each bar is a <g id="..."> containing <text aria-label="Response"> (label)
    # and <text aria-label="Unit"> (percentage).
    rows = []
    for g in target.find_all("g", id=True):
        label_el = g.find("text", attrs={"aria-label": "Response"})
        pct_el = g.find("text", attrs={"aria-label": "Unit"})
        if label_el and pct_el:
            label = label_el.get_text().strip()
            pct = float(pct_el.get_text().strip().rstrip("%"))
            rows.append({"response": label, "share_pct": pct})

    if not rows:
        raise ValueError("No percentage rows found in SVG chart")

    return rows, n_total


def scrape_survey() -> pd.DataFrame:
    """Scrape all years and return a combined DataFrame."""
    all_rows = []

    for year, url in SURVEY_URLS.items():
        log.info("scraping", year=year, url=url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        if year == 2023:
            rows, n_total = _scrape_2023(soup)
        else:
            rows, n_total = _scrape_svg_chart(soup)

        for row in rows:
            row["year"] = year
            row["n_total_responses"] = n_total
        all_rows.extend(rows)
        log.info("scraped", year=year, n_rows=len(rows), n_total=n_total)

    df = pd.DataFrame(all_rows, columns=["year", "response", "share_pct", "n_total_responses"])
    return df


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.

    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/stackoverflow_ai_usage.csv")
    df = scrape_survey()

    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
