"""
The file was provided by Christoph Lakner from the World Bank via email on June 10, 2025.
The unprocessed data can be found in the World Bank's Reproducibility Package, https://reproducibility.worldbank.org/index.php/catalog/285/
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/harmonized_national_poverty_lines.dta")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
