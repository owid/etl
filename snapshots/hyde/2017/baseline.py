import click

from etl.snapshot import Snapshot


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    snap = Snapshot("hyde/2017/baseline.zip")
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
