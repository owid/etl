"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"emissions/{SNAPSHOT_VERSION}/global_warming_potential_factors.csv")

    # Manually extract data on global warming potential (GWP-100) from Table 7.SM.7.
    tb = snap.read_from_records(
        [
            ("Carbon dioxide", "CO₂", 1),
            ("Methane", "CH₄", 27.9),
            ("Nitrous oxide", "N₂O", 273),
            ("PFC-14", "CF₄", 7380),
            ("HFC-152a", "CH₃CHF₂", 164),
            ("Sulphur hexafluoride", "SF₆", 24300),
        ],
        columns=["Name", "Formula", "GWP-100"],
    )

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=tb)


if __name__ == "__main__":
    main()
