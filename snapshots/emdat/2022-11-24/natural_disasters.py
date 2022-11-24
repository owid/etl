"""Ingest EM-DAT raw data on natural disasters.

Before running this script, the data needs to be downloaded:
* Register at https://public.emdat.be/ and verify email.
* Access https://public.emdat.be/data and select:
  * "Natural" in the list of "Disaster Classification".
  * All regions in "Location".
  * All years.
* Then click on "Download".
* Move the file to the current directory (where this file is located) and rename it as "natural_disasters.xlsx".

TODO: It might be better to download the file in data/snapshots/emdat/2022-11-24/natural_disasters.xlsx instead of here.

"""

import pathlib

import click

from etl.snapshot import Snapshot

CURRENT_DIR = pathlib.Path(__file__).parent


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot(CURRENT_DIR / "natural_disasters.xlsx")
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
