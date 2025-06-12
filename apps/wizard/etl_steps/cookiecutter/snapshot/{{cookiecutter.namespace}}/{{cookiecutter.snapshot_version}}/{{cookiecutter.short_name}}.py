"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
{% if cookiecutter.dataset_manual_import == True %}
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"{{cookiecutter.namespace}}/{SNAPSHOT_VERSION}/{{cookiecutter.short_name}}.{{cookiecutter.file_extension}}")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)

{% else %}
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"{{cookiecutter.namespace}}/{SNAPSHOT_VERSION}/{{cookiecutter.short_name}}.{{cookiecutter.file_extension}}")

    # Save snapshot.
    snap.create_snapshot(upload=upload)

{% endif -%}


if __name__ == "__main__":
    run()
