"""
Script to create a snapshot of dataset.

The file originally comes from the Huberman and Minns paper, available here

Final version
https://www.sciencedirect.com/science/article/abs/pii/S0014498307000058
https://personal.lse.ac.uk/minns/huberman_minns_eeh_2007.pdf

Working paper
https://ideas.repec.org/p/iis/dispap/iiisdp95.html

For convenience, I only downloaded a csv from an old version of the data available in OWID:
https://admin.owid.io/admin/datasets/234

I renamed the columns have more manageable names:
country,year,working_hours_week,working_hours_year,vacation_days

Upload that file by running this script with the `--path-to-file` option pointing to the csv file.
    python snapshots/working_hours/{version}/huberman_minns.py --path-to-file {path_to_file}

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
    snap = Snapshot(f"working_hours/{SNAPSHOT_VERSION}/huberman_minns.csv")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
