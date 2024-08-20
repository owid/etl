"""
The data has been obtained from covid-19-data repository.

Run the lines from https://github.com/owid/covid-19-data/blob/d8fae5631e2130270532951400fa449a9b20d00b/scripts/src/cowidev/cmd/vax/generate/utils.py#L712-L734, and export df_vaccinations.
"""
from datetime import date
from pathlib import Path
from typing import cast

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# WHO URL
URL_WHO = "https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/vaccination-data.csv"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # MANUAL
    ## This is a snapshot from github.com/owid/covid-19-data
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/vaccinations_global.csv")
    snap.create_snapshot(filename=path_to_file, upload=upload)

    # AUTOMATED
    ## This is extracted from WHO, and updates current snapshot

    ### Creat snapshot
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/vaccinations_global_who.csv")
    ### Load new data from WHO
    df = pd.read_csv(URL_WHO)
    ### Merge if applicable
    if snap.path.exists():
        ### Read if existing
        df_current = cast(pd.DataFrame, snap.read_csv())
        df = pd.concat([df_current, df])
        df = df.drop_duplicates()

        if not df.equals(df_current):
            snap = modify_metadata(snap)

    ### Push
    snap.create_snapshot(data=df, upload=upload)


def modify_metadata(snap: Snapshot) -> Snapshot:
    """Modify metadata"""
    # Get access date
    snap.metadata.origin.date_accessed = date.today()  # type: ignore
    # Set publication date
    snap.metadata.origin.publication_date = date.today()  # type: ignore
    # Save
    snap.metadata.save()
    return snap


if __name__ == "__main__":
    main()
