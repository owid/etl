"""Script to create a snapshot of dataset 'Certification status of dracunculiasis eradication (WHO, 2018)'."""

from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/guinea_worm.csv")
    df = get_certification_table()
    df_to_file(df, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_certification_table() -> pd.DataFrame:
    url = "https://web.archive.org/web/20211024081702/https://apps.who.int/dracunculiasis/dradata/html/report_Countries_t0.html"
    html_doc = requests.get(url).content
    soup = BeautifulSoup(html_doc, "html.parser")
    table = soup.find_all("table")[1]
    df = pd.read_html(StringIO(str(table)))[0]
    return df


if __name__ == "__main__":
    main()
