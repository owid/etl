"""Script to create a snapshot of dataset.

To update snapshots, run this script as-is.
If warnings are raised, update the numbers in QUERY_PARAMETERS (used as sanity checks).
NOTE: In some cases, the number of signatories and parties that are counted from the site differ from the ones that appear at the top of the page. Rely on the counted numbers instead of the ones at the top of the pages (which may not be accurate).

I couldn't find out a simple way to get the publication date. One would be to see the latest action in the treaty, but that could be a very old date, even though there has been no other actions since then. So, for now, I'll use date_acess as date_published.

"""

from pathlib import Path

import click
import pandas as pd
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL of the API the data is fetched from.
URL = "https://gql-api-dataportal.unoda.org"

# Define the query variable name for each treaty.
# Also, add some numerical information about the treaties, just for sanity checks.
QUERY_PARAMETERS = {
    "geneva_protocol": {"query_variable": "1925", "n_signed": 38, "n_parties": 146},
    "partial_test_ban": {"query_variable": "test_ban", "n_signed": 104, "n_parties": 125},
    "comprehensive_test_ban": {"query_variable": "ctbt", "n_signed": 187, "n_parties": 178},
    "non_proliferation": {"query_variable": "npt", "n_signed": 93, "n_parties": 191},
    "prohibition": {"query_variable": "tpnw", "n_signed": 94, "n_parties": 73},
}

# Parameters to update if assertions below fail.
QUERY_PARAMETERS = {
    # Geneva Protocol: https://treaties.unoda.org/t/1925
    "geneva_protocol": {"query_variable": "1925", "n_signed": 38, "n_parties": 146},
    # Partial Test Ban Treaty: https://treaties.unoda.org/t/test_ban
    # NOTE: At the top of the page it mentions 125 parties.
    "partial_test_ban": {"query_variable": "test_ban", "n_signed": 104, "n_parties": 135},
    # Comprehensive Nuclear-Test-Ban Treaty: https://treaties.unoda.org/t/ctbt
    # NOTE: At the top of the page it mentions 178 parties.
    "comprehensive_test_ban": {"query_variable": "ctbt", "n_signed": 187, "n_parties": 187},
    # Nuclear Non-Proliferation Treaty: https://treaties.unoda.org/t/npt
    "non_proliferation": {"query_variable": "npt", "n_signed": 93, "n_parties": 191},
    # Treaty on the Prohibition of Nuclear Weapons: https://treaties.unoda.org/t/tpnw
    # NOTE: At the top of the page it mentions 73 parties.
    "prohibition": {"query_variable": "tpnw", "n_signed": 94, "n_parties": 98},
}


def fetch_data(query_variable: str) -> pd.DataFrame:
    # Define inputs for the request.
    variables = {"input": {"id": None, "short_name": query_variable, "type": "GET_TREATY"}}

    query = """query Treaty($input: TreatyRequestInput_) {
        treaty_(input: $input) {
            ... on ReadResponse {
            data {
                ... on Treaty_ {
                actions_ {
                    date
                    action_type_
                    state {
                        name
                    }
                    depositary {
                        short_name
                    }
                }
                }
            }
            }
        }
        }
    """

    # Make the request.
    response = requests.post(URL, json={"query": query, "variables": variables})

    # Parse the response JSON.
    data = response.json()

    # Extract the relevant events from the returned data.
    events = [
        (event["date"], event["state"]["name"], event["action_type_"], event["depositary"]["short_name"])
        for event in data["data"]["treaty_"]["data"]["actions_"]
    ]

    # Construct a dataframe with the events.
    df = pd.DataFrame(events, columns=["date", "state", "action", "depositary"])

    # Sanity check.
    error = "Date format may have changed, inspect this issue."
    assert set(df["date"].str.split(" ").str[1]) == {"00:00:00"}, error

    # Keep only date (and not time).
    df["date"] = df["date"].str.split(" ").str[0]

    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    for treaty in QUERY_PARAMETERS:
        # Fetch data for this treaty.
        df = fetch_data(query_variable=QUERY_PARAMETERS[treaty]["query_variable"])

        # Sanity checks.
        error = f"Number of participant or signed countries in {treaty} treaty may have changed, update those numbers."
        n_parties_found = len(set(df["state"]))
        n_parties_expected = QUERY_PARAMETERS[treaty]["n_parties"]
        if n_parties_expected != n_parties_found:
            log.warning(
                f"In {treaty}, expected {n_parties_expected} parties, found {n_parties_found}. Update QUERY_PARAMETERS."
            )
        n_signed_found = len(set(df[df["action"] == "Signatory"]["state"]))
        n_signed_expected = QUERY_PARAMETERS[treaty]["n_signed"]
        if n_signed_expected != n_signed_found:
            log.warning(
                f"In {treaty}, expected {n_signed_expected} signatories, found {n_signed_found}. Update QUERY_PARAMETERS."
            )

        error = f"Expected actions in {treaty} treaty may have changed, inspect this issue."
        assert set(df["action"]) <= {"Accession", "Ratification", "Signatory", "Succession", "Withdrawal"}, error

        # Initialize a new snapshot for the data of the current treaty.
        snap = Snapshot(f"war/{SNAPSHOT_VERSION}/nuclear_weapons_treaties__{treaty}.csv")
        # Use accessed date as publication date, as justified above.
        snap.metadata.origin.date_published = snap.metadata.origin.date_accessed
        # Update metadata .dvc file.
        snap.metadata.save()
        # Create snapshot.
        snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    run()
