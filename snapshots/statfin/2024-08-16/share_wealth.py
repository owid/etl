"""
This code creates a snapshot of the wealth dataset from Statistics Finland.

The data needs to be uploaded manually following these steps:
    1. Open the StatFin platform https://pxdata.stat.fi/PxWeb/pxweb/en/StatFin/StatFin__vtutk/statfin_vtutk_pxt_136z.px/
    2. In "Information", select "Net wealth".
    3. In "Statistic", select "Mean per all households".
    4. In "Nettovarallisuuskymmenys" [sic], select "All households" and "X (wealthiest)".
    5. In "Year", click on "Select all".
    6. Click on "Show table".
    7. Click on "Pivot manual".
    8. Move "Statistic" and "Nettovarallisuuskymmenys" to the columns panel and click in "Complete".
    9. In the left panel, open "Save result as..." and select "Comma delimited without heading".
    10. Click on "Save".
    11. Copy the downloaded file to this folder.
    12. Run the script:
        python snapshots/statfin/{version}/share_wealth.py --path-to-file {path_to_file}
    13. Delete the downloaded file.
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
    snap = Snapshot(f"statfin/{SNAPSHOT_VERSION}/share_wealth.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
