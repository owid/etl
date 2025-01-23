"""The data for this for this snapshot is obtained from multiple links on the UNAIDS website.

In particular, we follow these steps:

- Get the file with all API links: "UNAIDS_API_links.csv"
    - This file was obtained by directly reaching the UNAIDS via mail.
    - This file contains the list of all UNAIDS indicators, along with their respective API links to access their values.
- Access each indicator and save the data.

NOTE:

1) There are four main categories in the data:
    EPI -- Epidemic indicators (https://aidsinfo.unaids.org/):
    These indicators provide data on the status and trends of the HIV epidemic within populations. They include metrics such as HIV prevalence, incidence rates, and AIDS-related mortality. Epidemiological indicators are essential for understanding the scope and dynamics of the epidemic, informing public health strategies, and allocating resources effectively.

    GAM -- Global AIDS Monitoring (https://indicatorregistry.unaids.org):
    This framework encompasses a comprehensive set of indicators designed to assess progress towards achieving global HIV targets, as outlined in political declarations and strategic plans. The GAM indicators cover various aspects of the HIV response, including prevention, treatment, and support services. Countries report on these indicators annually to UNAIDS, facilitating global monitoring and accountability.


    KPA -- Key Population Atlas (https://kpatlas.unaids.org/dashboard):
    This category focuses on indicators related to key populations that are at higher risk of HIV infection, such as sex workers, men who have sex with men, people who inject drugs, transgender individuals, and prisoners. The Key Population Atlas provides data on the size, prevalence, and service coverage for these groups, supporting targeted interventions and resource allocation to address their specific needs.

    NCPI -- National Commitments and Policy Instrument (https://lawsandpolicies.unaids.org/)
    The NCPI is a tool used to evaluate the policy, legal, and programmatic environment of national HIV responses. It includes questions related to the implementation of laws, policies, and strategies that affect the effectiveness of HIV interventions. The NCPI helps identify structural barriers and enablers within countries, guiding improvements in the national response to HIV.

2) We import all data, we will filter in later stages.

3) Importing all indicators, in parallel, takes ~3 minutes on M1 MAX PRO.
"""

from concurrent.futures import ThreadPoolExecutor
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


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local file with the API links.")
def main(path_to_file: str, upload: bool) -> None:
    categories = [
        "epi",
        "gam",
        "kpa",
        "ncpi",
    ]
    for category in categories:
        # log.info(" =================================================================================== ")
        # log.info(f" ======================== health.unaids: {category.upper()} ======================== ")
        # log.info(" =================================================================================== ")

        # Create a new snapshot.
        snap = Snapshot(f"health/{SNAPSHOT_VERSION}/unaids_{category}.csv")

        # Get data
        df = get_all_data_from_api(path_to_file, sheet_name=category.upper())

        # Manually upload df to S3
        snap.create_snapshot(data=df, upload=upload)


def get_all_data_from_api(path: str, sheet_name: str) -> pd.DataFrame:
    """Download relevant data from UNAIDS API."""

    # Load all api links
    df_links = pd.read_excel(path, sheet_name=sheet_name)

    # Get data from all links
    links = df_links["API"].values
    data = []

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(api_query, links))

    for result in results:
        if result is not None:
            data.extend(result)

    # Combine all records into single dataframe
    df = pd.json_normalize(data)

    return df


def api_query(link):
    """Query UNAIDS API and return the data."""
    log.info(f"health.unaids: downloading from {link}")

    # Query API
    response = requests.get(link)

    # Sanity check and return data if available, otherwise return None
    if response.status_code == 200:
        data_ = response.json()
        if "Data" in data_ and len(data_["Data"]) > 0 and "Observation" in data_["Data"][0]:
            return data_["Data"][0]["Observation"]
        else:
            log.error(f"health.unaids: unexpected data format in response from {link}")
    else:
        log.error(f"health.unaids: failed to download from {link}, status code: {response.status_code}")

    return


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
