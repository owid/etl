"""Script to create a snapshot of dataset.

To download the data visit:

https://worldhealthorg.shinyapps.io/glass-dashboard/_w_679389fb/#!/amr

and go to the section called 'Global maps of testing coverage by bacterial pathogen and antibiotic group'

Download each slice of the data (this is quite time-consuming but I tried and failed at automating it using selenium, shiny apps are not fun!) into a folder structured like: bloodstream/acinetobacter_spp/carbapenems/2022.csv

Then zip this up and upload the file to snapshot.


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
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/who_glass_by_antibiotic.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
