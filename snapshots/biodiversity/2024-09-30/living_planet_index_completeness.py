"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    df = pd.DataFrame(
        {
            "taxon": ["Mammals", "Amphibians", "Birds", "Reptiles", "Fishes"],
            "year": [2022, 2022, 2022, 2022, 2022],
            "species_in_lpi": [724, 307, 1496, 238, 2078],
            "species_in_not_in_lpi": [4447, 7375, 7854, 11675, 32555],
            "species_total": [5171, 7682, 9350, 11913, 34633],
        }
    )
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/living_planet_index_completeness.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
