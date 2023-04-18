"""Script to create a snapshot of dataset 'Aviation statistics'.

In total, 3 snapshots will be created:
* Data downloaded from their public Google spreadsheet.
* Data extracted from their HTML page on aviation statistics by period.
* Data extracted from their HTML page on aviation statistics by nature.

It would be better to extract all data from the spreadsheet, but some important variables are not included there.

"""

from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def get_aviation_data(url: str) -> pd.DataFrame:
    """Extract data table from a specific page of the Aviation Safety Network web site.

    Parameters
    ----------
    url : str
        URL of the page containing a data table.

    Returns
    -------
    df : pd.DataFrame
        Extracted data.

    """
    # Extract HTML content from the URL.
    html_content = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).content
    # Parse HTML content.
    soup = BeautifulSoup(html_content, "html.parser")

    # The page contains a table with data, extract table header.
    columns = [column.get_text() for column in soup.find_all("th", attrs={"class": "defaultheader"})]

    # Extract all data points from the table.
    raw_data = [row.get_text() for row in soup.find_all("td", attrs={"class": ["listcaption", "listdata"]})]

    # Reshape data points to be in rows, and create a dataframe.
    df = pd.DataFrame(
        np.array(raw_data).reshape(int(len(raw_data) / len(columns)), len(columns)), columns=columns
    ).astype(int)

    return df


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot for data from the spreadsheet.
    snap = Snapshot(f"aviation_safety_network/{SNAPSHOT_VERSION}/aviation_statistics.csv")

    # Download data from source.
    snap.download_from_source()

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)

    # Create two additional snapshots for the data taken directly from their website.
    snap_by_period = Snapshot(f"aviation_safety_network/{SNAPSHOT_VERSION}/aviation_statistics_by_period.csv")
    snap_by_nature = Snapshot(f"aviation_safety_network/{SNAPSHOT_VERSION}/aviation_statistics_by_nature.csv")

    # Fetch data on total accidents/fatalities, by period or by nature.
    period_df = get_aviation_data(url=snap_by_period.metadata.url)
    nature_df = get_aviation_data(url=snap_by_nature.metadata.url)

    # Create local snapshot files.
    df_to_file(period_df, file_path=snap_by_period.path)
    df_to_file(nature_df, file_path=snap_by_nature.path)

    # Add files to DVC and upload to S3.
    snap_by_period.dvc_add(upload=upload)
    snap_by_nature.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
