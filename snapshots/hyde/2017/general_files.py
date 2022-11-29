<<<<<<< HEAD
import pathlib

=======
>>>>>>> fc256ac5a0f3c030e6053ca16acfec4070e07e66
import click

from etl.snapshot import Snapshot

<<<<<<< HEAD
CURRENT_DIR = pathlib.Path(__file__).parent

=======
>>>>>>> fc256ac5a0f3c030e6053ca16acfec4070e07e66

@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
<<<<<<< HEAD
    snap = Snapshot(CURRENT_DIR / "general_files.zip")
=======
    snap = Snapshot("hyde/2017/general_files.zip")
>>>>>>> fc256ac5a0f3c030e6053ca16acfec4070e07e66
    snap.download_from_source()
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
