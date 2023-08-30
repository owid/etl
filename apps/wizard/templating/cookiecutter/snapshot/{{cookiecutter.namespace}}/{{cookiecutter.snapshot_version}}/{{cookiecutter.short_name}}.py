"""Script to create a snapshot of dataset."""

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
){% if cookiecutter.dataset_manual_import == "True" %}
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"{{cookiecutter.namespace}}/{SNAPSHOT_VERSION}/{{cookiecutter.short_name}}.{{cookiecutter.file_extension}}")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


{% else %}
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"{{cookiecutter.namespace}}/{SNAPSHOT_VERSION}/{{cookiecutter.short_name}}.{{cookiecutter.file_extension}}")

    # Download data from source.
    snap.download_from_source()

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


{% endif -%}


if __name__ == "__main__":
    main()
