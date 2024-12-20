"""This script should be manually adapted and executed on the event of an update of the Maddison Project Database."""

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
    snap = Snapshot("ggdc/2020-10-01/ggdc_maddison.xlsx")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
