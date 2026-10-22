"""Load a snapshot and create a meadow dataset.
Data from this is extracted from the WHO's Weekly Epidemiological Record pdf in snapshot and subsequent WHO/UNICEF announcements and compiled into a json living in this folder"""

import json

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COUNTRIES = json.loads((paths.directory / "maternal_tetanus_data.json").read_text())


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maternal_tetanus.pdf")

    # Load the JSON data containing country information as a Table
    tb = Table(pd.DataFrame(COUNTRIES))

    tb.metadata = snap.to_table_metadata()
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]

    # Improve tables format.
    tables = [tb.format(["country"], short_name="maternal_tetanus")]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
