"""Script to create a snapshot of dataset.

The data is directly extracted from the website.

"""

from pathlib import Path

import click
import pandas as pd
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL of the API the data is fetched from.
URL = "https://gql-api-dataportal.unoda.org"

# Define the query variable name for each treaty.
# Also, add some numerical information about the treaties, just for sanity checks.
QUERY_PARAMETERS = {
    # Note that at the top of the page (https://treaties.unoda.org/t/tpnw/participants) it mentions 70 parties.
    "geneva_protocol": {"query_variable": "1925", "n_parties": 146, "n_signed": 38},
    # Note that at the top of the page (https://treaties.unoda.org/t/test_ban) it mentions 125 parties.
    "partial_test_ban": {"query_variable": "test_ban", "n_parties": 135, "n_signed": 104},
    # Note that at the top of the page (https://treaties.unoda.org/t/ctbt) it mentions 177 parties.
    "comprehensive_test_ban": {"query_variable": "ctbt", "n_parties": 187, "n_signed": 187},
    "non_proliferation": {"query_variable": "npt", "n_parties": 191, "n_signed": 93},
    "prohibition": {"query_variable": "tpnw", "n_parties": 97, "n_signed": 93},
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
def main(upload: bool) -> None:
    for treaty in QUERY_PARAMETERS:
        print(treaty)
        # Fetch data for this treaty.
        df = fetch_data(query_variable=QUERY_PARAMETERS[treaty]["query_variable"])

        # Sanity checks.
        error = f"Number of participant or signed countries in {treaty} treaty may have changed, update those numbers."
        assert len(set(df["state"])) == QUERY_PARAMETERS[treaty]["n_parties"], error
        assert len(set(df[df["action"] == "Signatory"]["state"])) == QUERY_PARAMETERS[treaty]["n_signed"], error
        error = f"Expected actions in {treaty} treaty may have changed, inspect this issue."
        assert set(df["action"]) <= {"Accession", "Ratification", "Signatory", "Succession", "Withdrawal"}, error

        # Create snapshot for the data of the different treaties.
        snap = Snapshot(f"war/{SNAPSHOT_VERSION}/{treaty}.csv")
        snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
