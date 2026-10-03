"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import lxml.html
import requests
from bs4 import BeautifulSoup
from owid.catalog import Table

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"gavi/{SNAPSHOT_VERSION}/eligibility.csv")
    df = get_data_from_gavi()
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


def get_data_from_gavi() -> Table:
    # URL of the page to scrape
    url = "https://www.gavi.org/types-support/sustainability/eligibility"

    # Fetch the page content
    response = requests.get(url)
    response.raise_for_status()
    html = response.content

    # Parse the HTML using BeautifulSoup with the lxml parser
    soup = BeautifulSoup(html, "lxml")

    # Convert the BeautifulSoup object to an lxml element for XPath support
    doc = lxml.html.fromstring(str(soup))

    # Use the provided XPath to locate the main data block
    xpath = '//*[@id="block-views-block-modules-main-content-modules-content"]/div[2]'
    elements = doc.xpath(xpath)
    if not elements:
        raise Exception("Could not find the element with the provided XPath.")
    main_block = elements[0]

    # Convert the extracted block back into a BeautifulSoup object for easier parsing
    block_html = lxml.html.tostring(main_block, pretty_print=True, encoding="unicode")
    block_soup = BeautifulSoup(block_html, "lxml")

    # Find all columns containing the country lists (each column corresponds to a phase)
    columns = block_soup.find_all("div", class_="col_1-3 col col_main_body")

    # Create a dictionary to store the phase names and their corresponding countries
    data = {}
    for col in columns:
        header_tag = col.find("h4")
        if header_tag:
            phase = header_tag.get_text(strip=True)
        else:
            continue

        # Extract the country names from the <li> tags within the <ul>
        li_tags = col.find_all("li")
        countries = [li.get_text(strip=True) for li in li_tags]
        data[phase] = countries

    # Build a list of records where each record is a dictionary
    # with the Year (2024), Phase, and Country.
    records = []
    year = 2024  # The year is fixed to 2024
    for phase, countries in data.items():
        for country in countries:
            records.append({"year": year, "phase": phase, "country": country})

    # Create a Table from the records
    tb = Table(records)

    # Optionally, reorder the columns if desired
    tb = tb[["country", "year", "phase"]]

    return tb


if __name__ == "__main__":
    main()
