"""The data for this for this snapshot is obtained from multiple links on the UNAIDS website.

In particular, we follow these steps:

- Get the file with all API links: "UNAIDS_API_links.csv"
    - This file was obtained by directly reaching the UNAIDS via mail.
    - This file contains the list of all UNAIDS indicators, along with their respective API links to access their values.
- Select those indicators of interest
    - Together with a researcher, we selected the indicators of interest.
- Access the data, combine them into a CSV and store it.
    - For that, we can use the function `get_all_data_from_api` from this module.
"""

import tempfile
from pathlib import Path

import click
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from structlog import get_logger
from urllib3.util.retry import Retry

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Logger
log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local file with the API links.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/unaids.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Get data
    df = get_all_data_from_api(path_to_file)

    with tempfile.NamedTemporaryFile(mode="w") as csvfile:
        df.to_csv(csvfile, index=False)  # type: ignore
        with open(csvfile.name) as csvfile:
            # Copy local data file to snapshots data folder.
            snap.path.write_bytes(Path(csvfile.name).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def get_all_data_from_api(path: str) -> pd.DataFrame:
    """Download relevant data from UNAIDS API."""
    # Relevant indicators in UNAIDS data
    FIELDS_RELEVANT = {
        "EPI": [
            "HIV Prevalence",
            "Deaths averted due to ART",
            "AIDS-related deaths",
            "HIV Incidence per 1000 population",
            "New HIV Infections averted due to PMTCT",
            "New HIV Infections",
            "AIDS Orphans",
            "Gap to reaching the target percent of people receiving ART",
        ],
        "GAM": [
            "Country-reported HIV expenditure by funding source",
            "Condom use at last high-risk sex",
            "Condom use among men who have sex with men",
            "Discriminatory attitudes towards people living with HIV",
            "Knowledge about HIV prevention in young people",
            "Estimated TB-related deaths among people living with HIV",
            "TB patients living with HIV receiving ART",
            "TB patients tested positive for HIV",
            "Estimated HIV resource availability for low- and middle-income countries in constant 2019 USD",
        ],
    }

    # Load all api links
    df_links = pd.read_excel(path, sheet_name=None)

    # Load relevant fields, and concatenate
    dfs = []
    for category, fields in FIELDS_RELEVANT.items():
        df_ = df_links[category]
        df_ = df_[df_["Indicator_Name"].isin(fields)]
        dfs.append(df_)
    df = pd.concat(dfs, ignore_index=True)

    # Define requests session
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    def _get_data_from_api_links(df_links: pd.DataFrame) -> pd.DataFrame:
        dfs = []
        for url in df_links["API"].values:
            log.info(f"health.unaids: downloading from {url}")
            data = session.get(url).json()
            df_ = pd.DataFrame.from_records(data["Data"][0]["Observation"])
            dfs.append(df_)
            # time.sleep(2)
        df: pd.DataFrame = pd.concat(dfs, ignore_index=True)  # type: ignore
        return df

    # Get data
    df = _get_data_from_api_links(df)  # type: ignore

    return df


if __name__ == "__main__":
    main()
