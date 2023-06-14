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
from shared import CURRENT_DIR, add_dataframe_with_metadata_to_catalog

from owid.walden import Dataset

# Name of Walden dataset for aviation statistics by period.
WALDEN_DATASET_NAME_PERIOD = "aviation_statistics_by_period"
# Name of Walden dataset for aviation statistics by nature.
WALDEN_DATASET_NAME_NATURE = "aviation_statistics_by_nature"


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
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    # Get walden metadata.
    dataset_by_period = Dataset.from_yaml(CURRENT_DIR / f"{WALDEN_DATASET_NAME_PERIOD}.meta.yml")
    dataset_by_nature = Dataset.from_yaml(CURRENT_DIR / f"{WALDEN_DATASET_NAME_NATURE}.meta.yml")

    # Fetch data on total accidents/fatalities, by period or by nature.
    period_df = get_aviation_data(url=dataset_by_period.url)
    nature_df = get_aviation_data(url=dataset_by_nature.url)

    # Add the two data files to Walden catalog and metadata to Walden index.
    add_dataframe_with_metadata_to_catalog(df=period_df, metadata=dataset_by_period, upload=upload)
    add_dataframe_with_metadata_to_catalog(df=nature_df, metadata=dataset_by_nature, upload=upload)

    # Update Walden datasets.
    dataset_by_period.save()
    dataset_by_nature.save()


if __name__ == "__main__":
    main()
