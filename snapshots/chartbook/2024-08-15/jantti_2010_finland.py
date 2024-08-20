"""
Script to create a snapshot of dataset.

The file comes from Appendix 8A in the original book, available here: http://piketty.pse.ens.fr/fichiers/AtkinsonPiketty2010.pdf.
I use a csv file from the data extracted in the past by the Chartbook team, with the columns year, country, gini. gini is the column Taxable income/
population.

See https://docs.google.com/spreadsheets/d/1ZakjK-hP6s4tLJZCEFjR7NVVqTkb6AXwgpvfpSwsT-I/edit?gid=1888715824#gid=1888715824
After creating the file, run
    python snapshots/chartbook/2024-08-15/jantti_2010_finland.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/jantti_2010_finland.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
