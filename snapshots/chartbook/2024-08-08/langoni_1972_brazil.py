"""
Script to create a snapshot of dataset.

The file comes from the original book, available here: https://edisciplinas.usp.br/pluginfile.php/5447307/mod_resource/content/1/Langoni%20dist%20de%20renda%20e%20desenvolvimento%20estudos%20economicos%201972.pdf.
I use a csv file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1IaA-lvbRlixYMLy5nW6xxJolaZyO4DK_0XvLQuLjJJs/edit?gid=1799504649#gid=1799504649
After creating the file, run
    python snapshots/chartbook/2024-08-08/langoni_2005_brazil.py --path-to-file <path-to-file>
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/langoni_1972_brazil.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
