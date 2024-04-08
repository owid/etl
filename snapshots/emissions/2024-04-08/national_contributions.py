"""Script to create a snapshot of dataset National contributions to climate change (Jones et al.).

NOTE: All metadata fields are automatically updated by this script. However, the dataset description may change a bit
(for example they may cite more recent papers). Visually inspect the dataset description and manually make small
modifications, if needed.

"""

from datetime import datetime
from pathlib import Path
from typing import Dict

import click
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files to snapshot.
DATA_FILES = [
    "annual_emissions.csv",
    "cumulative_emissions.csv",
    "temperature_response.csv",
]


def extract_metadata_from_main_page(snap: Snapshot) -> Dict[str, str]:
    """Extract the publication date."""
    # Get the full HTML content of the main page.
    response = requests.get(snap.metadata.origin.url_main)  # type: ignore

    # The "latest" url redirects to the new record (which we need to extract other fields).
    response_final = response.url

    # Parse the HTML content of the main page.
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract the publication date, which is given in one of the first sentences as in, e.g. "Published March 19, 2024".
    date_published_str = [line.split("Published")[1].strip() for line in soup.text.split("\n") if "Published" in line][
        0
    ]

    # Convert to ISO format.
    date_published = datetime.strptime(date_published_str, "%B %d, %Y").strftime("%Y-%m-%d")

    # Extract the version of the data producer.
    version_producer = [line.split("| Version ")[1].strip() for line in soup.text.split("\n") if "| Version " in line][
        0
    ]

    # The download links have the years hardcoded in the url, so we need to update them.
    file_name = snap.metadata.origin.url_download.split("/")[-1]  # type: ignore
    # Assume that the latest informed year in the data is 2 years before the current version.
    file_name_new = file_name.split("-")[0] + "-" + str(int(version_producer.split(".")[0]) - 2) + ".csv"
    # Create the new download url (using the new token for the latest version, and the latest year in the file name).
    url_download = response_final + "/files/" + file_name_new

    # The full citation is not included in the HTML and is fetched from an API.
    response_citation = requests.get(
        response_final.replace("records/", "api/records/"), headers={"Accept": "text/x-bibliography"}
    )

    # Extract the full citation.
    citation_full = response_citation.text

    # Gather all extracted fields.
    extracted_fields = {
        "date_published": date_published,
        "version_producer": version_producer,
        "url_download": url_download,
        "citation_full": citation_full,
    }

    return extracted_fields


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    for data_file in DATA_FILES:
        # Create a new snapshot.
        snap = Snapshot(f"emissions/{SNAPSHOT_VERSION}/national_contributions_{data_file}")

        # Get the publication date (it needs to be done only once).
        extracted_fields = extract_metadata_from_main_page(snap)

        for field in extracted_fields:
            # Replace metadata fields with the new extracted fields.
            setattr(snap.metadata.origin, field, extracted_fields[field])

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
