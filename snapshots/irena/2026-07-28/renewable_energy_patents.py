"""Script to create a snapshot of dataset.

* The data was manually downloaded from:
https://irena.sharepoint.com/:x:/s/statistics-public/ET2l9BUWLM5EsH4mZBnqpl4BpU3run3MJbwgYWW64PQR7A
Click on the second sheet "INSPIRE_data"
Then click on "File" -> "Export" -> "Download as CSV UTF-8".

This data is also shown in their public Tableau dashboard:
https://public.tableau.com/views/IRENARenewableEnergyPatentsTimeSeries_2_0/ExploreMore

"""

from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    error = "This snapshot needs the manually downloaded file (see the instructions above): pass --path-to-file."
    assert path_to_file, error

    # Create a new snapshot.
    snap = Snapshot(f"irena/{SNAPSHOT_VERSION}/renewable_energy_patents.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)
