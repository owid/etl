"""
STEPS TO OBTAIN THE DATA:
    1. Go to the OECD Data Explorer: https://data-explorer.oecd.org/
    2. In the section "Public governance", select "Public finance and budgets".
    3. Select "Public finance main indicators - Government at a glance, Yearly updates".
    5. Click the Download button.
    6. Select "Unfiltered data in tabular text (CSV)".
    7. Just for convenience, copy the file to this directory and rename it govt_glance_public_finance.csv.
    8. Run the script with the `--path-to-file` option:
        ```
        python snapshots/oecd/{version}/social_expenditure.py --path-to-file <path_to_file>
        ```

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
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/govt_glance_public_finance.csv")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
