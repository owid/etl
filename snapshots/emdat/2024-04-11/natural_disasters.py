"""Ingest EM-DAT raw data on natural disasters.

Before running this script, the data needs to be downloaded:
* Register at https://public.emdat.be/ and verify email.
* Access https://public.emdat.be/data and select:
  * "Natural" in the list of "Classification".
  * All regions in "Countries".
  * All years.
* Activate "Include Historical events (pre-2000)".
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
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
