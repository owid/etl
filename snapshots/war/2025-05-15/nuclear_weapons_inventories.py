"""Script to create a snapshot of dataset.

The data needs to be manually downloaded from a Tableau dashboard:
1. Open an incognito window (to avoid Tableau from changing the default language of the data columns).
2. Go to: https://public.tableau.com/app/profile/kate.kohn/viz/EstimatedGlobalNuclearWarheadInventories1945-2021/Dashboard1
3. Click on the "Download" button on the upper right corner of the dashboard.
4. Select "Crosstab" on the small pop-up window ("Data" used to be available, but now it's not).
5. Run this script using the --path-to-file argument followed by the path to the downloaded file.
6. Change the field date_published in the .dvc metadata file, with the "Updated" date shown below the chart.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/nuclear_weapons_inventories.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
