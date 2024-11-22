"""To be able to download the dataset, you need to be registered at HFD.

Just go to their site: https://www.humanfertility.org/
Or directly try to login: https://www.humanfertility.org/Account/Login / register: https://www.humanfertility.org/Account/Auth

Only after that, you'll be allowed to download the dataset: https://www.humanfertility.org/File/Download/Files/zip/HFD.zip
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
    snap = Snapshot(f"hmd/{SNAPSHOT_VERSION}/hfd.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
