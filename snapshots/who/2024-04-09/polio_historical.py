"""Script to create a snapshot of dataset.

The data is no longer available from the WHO but it is available on web archive e.g. here "https://web.archive.org/web/20200713223806/http://www.who.int/immunization/monitoring_surveillance/data/incidence_series.xls"

It can be downloaded and then used to create a snapshot from the local file.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/polio_historical.xls")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
