"""Extract data from UNEP's Global trends in renewable energy investment.

Since the data is given as an image of a table (not suitable for OCR) the data has been manually copied into a file,
next to this script.
"""

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# naming conventions
paths = PathFinder(__file__)

EXTRACTED_DATA_FILE = paths.directory / "renewable_energy_investments.data.csv"


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_dependency("global_trends_in_renewable_energy_investment.pdf")

    # Load file with manually extracted data.
    df = pd.read_csv(EXTRACTED_DATA_FILE)

    #
    # Prepare data.
    #
    # Transpose data to have a column per energy source.
    df = df.set_index("sector").transpose().rename_axis("year").reset_index()
    # Add column for region.
    df = df.assign(**{"country": "World"})

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = paths.version
    ds.metadata.short_name = paths.short_name

    # Create a new table with metadata and underscore all columns.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Add table to dataset and save dataset.
    ds.add(tb)
    ds.save()
