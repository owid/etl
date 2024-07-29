"""Load a snapshot and create a meadow dataset."""

import json

import pandas as pd
from owid.catalog.tables import Table, _add_table_and_variables_metadata_to_table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("sequence.json")

    # Load data from snapshot.
    tb = read_table(snap)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "week"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_table(snap: Snapshot) -> Table:
    """Read snapshot as Table.

    Income data is a dictionary.
    """
    # Read snapshot dictionary
    with open(snap.path, "r") as file:
        data = json.load(file)
    # Convert to DataFrame (data -> df)
    data = list(filter(lambda x: x["region"] == "World", data["regions"]))[0]["distributions"]
    df = pd.json_normalize(data=data, record_path=["distribution"], meta=["country"])
    # Convert to Table (df -> tb)
    tb = _add_table_and_variables_metadata_to_table(
        table=Table(df),
        metadata=snap.to_table_metadata(),
        origin=snap.metadata.origin,
    )
    return tb
