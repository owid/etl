"""This script should be manually adapted and executed on the event of an update of the Maddison Project Database.

"""

import click

from etl.snapshot import Snapshot


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot("technology/2023-01-05/gravitational_waves.csv")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
