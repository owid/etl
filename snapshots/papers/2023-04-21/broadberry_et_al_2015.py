"""Script to create a snapshot of dataset 'Long-term crop yields in UK, Broadberry et al. (2015)'.

The data was manually extracted from the book "British Economic Growth" (Broadberry et al. (2015)), specifically from
Table 3.06 "Weighted national average crop yields per acre, gross of tithes and net of seed, 1270s-1860s (bushels;
10-year averages)", and copied below.

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
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/broadberry_et_al_2015.csv")

    # Manually extracted data.
    df_yields_in_uk = pd.DataFrame.from_records(
        columns=["Decade", "Wheat", "Rye", "Barley", "Oats", "Pulses", "Potatoes"],
        data=[
            (1270, 8.38, 12.83, 11.70, 9.86, 2.86, np.nan),
            (1300, 7.80, 9.19, 11.73, 8.69, 6.36, np.nan),
            (1350, 6.32, 6.60, 8.92, 6.74, 4.04, np.nan),
            (1400, 6.36, 5.77, 10.74, 6.76, 4.35, np.nan),
            (1450, 5.00, 7.88, 8.41, 8.85, 3.67, np.nan),
            (1500, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan),
            (1550, 9.99, 6.35, 9.02, 10.56, 5.74, np.nan),
            (1600, 11.06, 10.34, 12.44, 13.17, 9.77, np.nan),
            (1650, 13.46, 9.83, 17.87, 12.10, 9.35, np.nan),
            (1700, 14.09, 16.04, 19.66, 10.76, 11.56, 150.00),
            (1750, 15.54, 27.14, 26.53, 23.28, 12.80, 150.00),
            (1800, 18.70, 21.81, 28.58, 25.19, 18.65, 150.00),
            (1850, 26.17, 19.74, 29.74, 33.09, 18.54, 150.00),
            (1860, 29.43, 18.66, 29.78, 35.05, 19.39, 150.00),
        ],
    )

    df_to_file(df_yields_in_uk, file_path=snap.path)

    # Add files to DVC and upload data.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
