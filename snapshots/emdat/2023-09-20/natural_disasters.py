"""Ingest EM-DAT raw data on natural disasters.

Before running this script, the data needs to be downloaded:
* Register at https://public.emdat.be/ and verify email.
* Access https://public.emdat.be/data and select:
  * "Natural" in the list of "Disaster Classification".
  * All regions in "Location".
  * All years.
* Then click on "Download".
* Run this script using the argument --path-to-file followed by the path to the downloaded file.

"""

from pathlib import Path

import click

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
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"emdat/{SNAPSHOT_VERSION}/natural_disasters.xlsx")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
