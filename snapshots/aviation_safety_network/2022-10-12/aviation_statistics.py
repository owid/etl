"""Get data from the Aviation Safety Network (extracted from the HTML).

Specifically, extract data from two pages:
* Statistics by period: https://aviation-safety.net/statistics/period/stats.php
* Statistics by nature: https://aviation-safety.net/statistics/nature/stats.php

"""

import click
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Name of snapshot for aviation statistics by period.
SNAPSHOT_NAME_PERIOD = "aviation_statistics_by_period"
# Name of snapshot for aviation statistics by nature.
SNAPSHOT_NAME_NATURE = "aviation_statistics_by_nature"


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
    help="Upload dataset to S3",
)
def main(upload: bool) -> None:
    snap_by_period = Snapshot(f"aviation_safety_network/2022-10-12/{SNAPSHOT_NAME_PERIOD}.csv")
    snap_by_nature = Snapshot(f"aviation_safety_network/2022-10-12/{SNAPSHOT_NAME_NATURE}.csv")

    # Fetch data on total accidents/fatalities, by period or by nature.
    period_df = get_aviation_data(url=snap_by_period.metadata.source.url)  # type: ignore
    nature_df = get_aviation_data(url=snap_by_nature.metadata.source.url)  # type: ignore

    df_to_file(period_df, file_path=snap_by_period.path)
    df_to_file(nature_df, file_path=snap_by_nature.path)

    snap_by_period.dvc_add(upload=upload)
    snap_by_nature.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
