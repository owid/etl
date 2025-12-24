"""Script to create a snapshot of TWD/USD exchange rates from Central Bank of Taiwan.

This script scrapes daily exchange rate data from 2006 to present.
Each year's data is available at a different URL following the pattern:
https://www.cbc.gov.tw/en/cp-4237-{year_id}-40064-2.html
"""

from pathlib import Path

import click
import pandas as pd
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL for each year from Central Bank of Taiwan
# URLs extracted from https://www.cbc.gov.tw/en/lp-4237-2.html
YEAR_URLS = {
    2006: "https://www.cbc.gov.tw/en/cp-4237-128173-40064-2.html",
    2007: "https://www.cbc.gov.tw/en/cp-4237-128174-38c29-2.html",
    2008: "https://www.cbc.gov.tw/en/cp-4237-128176-85038-2.html",
    2009: "https://www.cbc.gov.tw/en/cp-4237-128175-b5b3e-2.html",
    2010: "https://www.cbc.gov.tw/en/cp-4237-128177-059db-2.html",
    2011: "https://www.cbc.gov.tw/en/cp-4237-128178-57f82-2.html",
    2012: "https://www.cbc.gov.tw/en/cp-4237-128179-9cf48-2.html",
    2013: "https://www.cbc.gov.tw/en/cp-4237-128180-56a85-2.html",
    2014: "https://www.cbc.gov.tw/en/cp-4237-128181-af2bb-2.html",
    2015: "https://www.cbc.gov.tw/en/cp-4237-128182-a65ff-2.html",
    2016: "https://www.cbc.gov.tw/en/cp-4237-128183-fe982-2.html",
    2017: "https://www.cbc.gov.tw/en/cp-4237-128184-c5e55-2.html",
    2018: "https://www.cbc.gov.tw/en/cp-4237-128185-df5d4-2.html",
    2019: "https://www.cbc.gov.tw/en/cp-4237-128186-6004c-2.html",
    2020: "https://www.cbc.gov.tw/en/cp-4237-128187-e13f5-2.html",
    2021: "https://www.cbc.gov.tw/en/cp-4237-145242-a8428-2.html",
    2022: "https://www.cbc.gov.tw/en/cp-4237-157271-bd861-2.html",
    2023: "https://www.cbc.gov.tw/en/cp-4237-157403-d0a00-2.html",
    2024: "https://www.cbc.gov.tw/en/cp-4237-165072-15ec2-2.html",
}


def scrape_exchange_rate_data(url: str) -> list:
    """Scrape exchange rate data from a CBC Taiwan webpage.

    Parameters
    ----------
    url : str
        URL of the CBC Taiwan exchange rate page

    Returns
    -------
    list
        List of dicts with keys: date, exchange_rate
    """
    import requests

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table containing the exchange rate data
    # The table usually has class 'table' or is within a specific div
    table = soup.find("table")

    if not table:
        raise ValueError(f"No table found at {url}")

    # Parse table rows
    rows = []
    for tr in table.find_all("tr")[1:]:  # Skip header row
        cells = tr.find_all("td")
        if len(cells) >= 2:
            date_text = cells[0].get_text(strip=True)
            rate_text = cells[1].get_text(strip=True)

            if date_text and rate_text:
                try:
                    # Parse date (format: YYYY/MM/DD)
                    date = pd.to_datetime(date_text, format="%Y/%m/%d")
                    # Parse exchange rate
                    rate = float(rate_text)
                    rows.append({"date": date, "exchange_rate": rate})
                except (ValueError, AttributeError):
                    continue

    return rows


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    """Create snapshot of TWD/USD exchange rates from 2006 to present."""
    # Create a new snapshot.
    snap = Snapshot(f"cbc_taiwan/{SNAPSHOT_VERSION}/twd_usd_exchange_rate.csv")

    # Scrape data from all years
    all_data = []
    for year, url in sorted(YEAR_URLS.items()):
        print(f"Scraping data for {year}...")
        try:
            rows = scrape_exchange_rate_data(url)
            if rows:
                all_data.extend(rows)
                print(f"  Found {len(rows)} records for {year}")
            else:
                print(f"  No data found for {year}")
        except Exception as e:
            print(f"  Error scraping {year}: {e}")
            continue

    # Combine all years
    if not all_data:
        raise ValueError("No exchange rate data could be scraped")

    df_combined = pd.DataFrame(all_data)
    df_combined = df_combined.sort_values("date").reset_index(drop=True)

    print(f"\nTotal records scraped: {len(df_combined)}")
    print(f"Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")

    # Save to snapshot
    snap.create_snapshot(data=df_combined, upload=upload)


if __name__ == "__main__":
    main()
