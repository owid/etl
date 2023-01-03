"""Get data from UNDP Human Development Report (2021-22).

This data is provided under "Documentation and Downloads" section, at https://hdr.undp.org/data-center/documentation-and-downloads.
In there you can find more details about their methodology, region definitions, etc.


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
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Data
    snap = Snapshot("un/2022-11-29/undp_hdr.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)

    # Metadata
    snap = Snapshot("un/2022-11-29/undp_hdr.xlsx")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
