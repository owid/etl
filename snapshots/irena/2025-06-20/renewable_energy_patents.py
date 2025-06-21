"""Script to create a snapshot of dataset.

* The data was manually downloaded from:
https://irena.sharepoint.com/:x:/s/statistics-public/ET2l9BUWLM5EsH4mZBnqpl4BpU3run3MJbwgYWW64PQR7A
Click on the second sheet "INSPIRE_data"
Then click on "File" -> "Export" -> "Download as CSV UTF-8".

This data is also shown in their public Tableau dashboard:
https://public.tableau.com/views/IRENARenewableEnergyPatentsTimeSeries_2_0/ExploreMore

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
    snap = Snapshot(f"irena/{SNAPSHOT_VERSION}/renewable_energy_patents.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
