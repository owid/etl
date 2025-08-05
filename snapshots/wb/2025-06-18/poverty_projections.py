"""
HOW TO OBTAIN THE FILE:

Follow the instructions at the Reproducibility package (https://reproducibility.worldbank.org/catalog/285):

(I didn't need to follow steps 1 and 2, I just downloaded the data and changed the path in the do-file)
    1. Secure Access to Data: Download the datasets not included in the package. See subsection Datasets and the README for more details.
    2. Download and Place Data: Once the data is downloaded, users should place it in the appropriate folder.
    3. Run the Package: After placing the data in the folder:
        Open the do-file "master"
        Update the globals in line 34 to your folder's location and run the do-file.

    After these steps, you will find the file `pip_2021_projections_202505.dta` in the data/02_processed/ folder.

    Copy that file to this folder and run the script:
    python snapshots/wb/{version}/poverty_projections.py --path-to-file snapshots/wb/{version}/pip_2021_projections_202505.dta

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
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/poverty_projections.dta")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
