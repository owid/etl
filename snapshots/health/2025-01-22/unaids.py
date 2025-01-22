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

from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Logger
log = get_logger()

# Relevant indicators in UNAIDS data
## Key values correspond to sheet tab names in the API links file. Values correspond to indicator names within each sheet.
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
        "Estimated resource needs",
    ],
}


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

    # Manually upload df to S3
    snap.create_snapshot(data=df, upload=upload)


def get_all_data_from_api(path: str) -> pd.DataFrame:
    """Download relevant data from UNAIDS API."""

    # Load all api links
    df_links = pd.read_excel(path, sheet_name=None)

    # Load relevant fields, and concatenate
    dfs = []
    for tab_name, fields in FIELDS_RELEVANT.items():
        df_tab = df_links[tab_name]
        df_tab = df_tab[df_tab["Indicator_Name"].isin(fields)]
        dfs.append(df_tab)
    df_links = pd.concat(dfs, ignore_index=True)

    # Get data
    data = []
    for url in df_links["API"].values:
        log.info(f"health.unaids: downloading from {url}")
        data_ = requests.get(url).json()
        # df_ = pd.DataFrame.from_records(data["Data"][0]["Observation"])
        data.extend(data_["Data"][0]["Observation"])

    df = pd.DataFrame.from_records(data)
    # df = pd.concat(dfs, ignore_index=True)  # type: ignore

    return df


def safe_request():
    """Alternative if requests.get is failing.

    Use the returning object as you would use standard requests.

    ```
    request = safe_request()
    request.get(...)
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Define requests session
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)  # type: ignore[reportArgumentType]
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


if __name__ == "__main__":
    main()

# df_ = pd.concat(objs=[d for d in df_links.values()])
# link = df_.loc[df_.Indicator_GId == "MSM_CONDOM_USE", "API"].values
# data = requests.get(link[0]).json()
# df = pd.DataFrame.from_records(data["Data"][0]["Observation"])


# df_ = pd.concat(objs=[d for d in df_links.values()])
# links = df_.loc[df_.Indicator_GId == "MSM_CONDOM_USE", "API"].values
# subgroups = df_.loc[df_.Indicator_GId == "MSM_CONDOM_USE", "Subgroup_Val_GId"].values
# for link, subgroup in zip(links, subgroups):
#     data = requests.get(link).json()
#     df = pd.DataFrame.from_records(data["Data"][0]["Observation"])
#     print(subgroup)
#     if df.empty:
#         print("!! ERROR !!")
#     else:
#         print(df.TIME_PERIOD.min())
#     print("------------")
