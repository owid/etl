"""Script to create a snapshot of dataset 'Long-term crop yields in UK, Brassley (2000)'."""

from pathlib import Path

import click
import numpy as np
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/brassley_2000.csv")

    # Manually extracted data.
    df_yields_in_uk = pd.DataFrame.from_records(
        columns=["Year", "Wheat", "Barley", "Oats", "Potatoes", "Sugar Beet"],
        data=[
            ("1885-1889", 2.06, 1.96, 1.66, 14.7, np.nan),
            ("1910-1914", 2.17, 1.96, 1.71, 15.8, np.nan),
            ("1930-1934", 2.23, 2.02, 1.97, 16.5, 20.2),
            ("1942-1946", 2.56, 2.37, 2.16, 17.8, 26.4),
            ("1965-1969", 3.93, 3.61, 3.22, 25.4, 37.4),
            ("1985-1985", 6.33, 4.95, 4.59, 35.8, 38.3),
        ],
    )

    df_to_file(df_yields_in_uk, file_path=snap.path)

    # Add files to DVC and upload data.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
