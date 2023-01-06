import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

N = Names(__file__)


def run(dest_dir: str) -> None:
    snap = Snapshot("technology/2023-01-05/gravitational_waves.csv")
    df = pd.read_csv(snap.path)

    # Initialise dataset.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # In case events are recorded multiple times
    # we keep only the earliest GPS time for each commonName
    df = df.groupby("commonName", as_index=False).GPS.min()

    # Origin for GPS time is on 1980-01-06
    df["time"] = pd.to_datetime(df["GPS"], unit="s", origin="1980-01-06", utc=True)

    # Count events by year, and calculate the cumsum
    df["year"] = df["time"].dt.year
    df = df.groupby("year").size().reset_index(name="N")
    df["cumulative_gwt"] = df["N"].cumsum()

    # Add entity and select columns
    df = df.assign(entity="World")[["entity", "year", "cumulative_gwt"]]

    # Add table and save dataset
    ds.add(Table(df, short_name="gravitational_waves"))
    ds.update_metadata(N.metadata_path)
    ds.save()
