"""Script to create a snapshot of dataset.

The data for the US was obtained from snapshot https://github.com/owid/covid-19-data/blob/master/scripts/grapher/COVID-19%20-%20Decoupling%20of%20metrics.csv.

```
import pandas as pd
df = pd.read_csv(url)
df[df["Country"]=="United States"].to_csv("file.csv", index=False)
```
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--usa", type=str, help="Path to USA file.")
def main(upload: bool, usa: str) -> None:
    # Auto
    snap_names = [
        "spain",
        "israel",
    ]
    for name in snap_names:
        # Create a new snapshot.
        snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/decoupling_{name}.csv")

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)

    # Manual
    snap_manual = [
        ("usa", usa),
    ]
    for snap_details in snap_manual:
        # Create a new snapshot.
        snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/decoupling_{snap_details[0]}.csv")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(filename=snap_details[1], upload=upload)


if __name__ == "__main__":
    main()
