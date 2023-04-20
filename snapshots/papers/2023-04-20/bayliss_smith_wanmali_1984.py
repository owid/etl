"""Script to create a snapshot of dataset 'Long-term wheat yields, Understading Green Revolutions, Bayliss-Smith &
Wanmali (1984)'.

The data from the book ("Table 1.2. Wheat yields in Europe, 1850 to 1977-79 (100 kg/hectare)") is manually copied below.

"""

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
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/bayliss_smith_wanmali_1984.csv")

    # Manually extracted data.
    df_wheat_yields_in_europe = pd.DataFrame.from_records(
        columns=["country", "1850-1850", "1909-1913", "1934-1935", "1948-1952", "1977-1979"],
        data=[
            ("Denmark", 12.0, 33.1, 28.1, 36.5, 52.3),
            ("Belgium", 10.5, 25.3, 27.7, 32.2, 48.2),
            ("Netherlands", 10.5, 23.5, 30.9, 36.5, 59.0),
            ("Germany", 9.9, 24.2, 20.0, 26.2, 45.6),
            ("United Kingdom", 9.9, 21.2, 21.5, 27.2, 51.2),
            ("Austria", 7.7, 13.7, 18.0, 17.1, 36.9),
            ("France", 7.0, 13.1, 14.1, 18.3, 46.7),
            ("Italy", 6.7, 10.5, 10.1, 15.2, 25.4),
            ("Norway", 5.7, 16.6, 19.5, 20.6, 40.7),
            ("Romania", np.nan, 12.9, 8.0, 10.2, 25.8),
            ("Hungary", np.nan, 13.2, 11.3, 13.8, 38.6),
            ("Bulgaria", np.nan, 6.2, 9.2, 12.4, 38.3),
            ("Spain", 4.6, 9.2, 14.7, 8.7, 16.3),
            ("Greece", 4.6, 9.8, 9.2, 10.2, 23.9),
            ("Russia", 4.5, 6.9, 8.1, 8.4, 16.5),
        ],
    )

    df_to_file(df_wheat_yields_in_europe, file_path=snap.path)

    # Add files to DVC and upload data.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
