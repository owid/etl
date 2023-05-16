"""Script to create a snapshot of dataset 'Global Burden of Disease: Mental disorders, prevalence rates (IHME, 2020)'.

IHME provides the GHDx platform, where you can create your own dataset with the subset of metrics and dimensions you are interested in. The complete dataset is too large to be imported at once.
Follow the steps below to obtain the file for this step:
- Visit https://vizhub.healthdata.org/gbd-results
    - NOTE: You need to register in the portal. It is free and fast.
- Select:
    - GBD Estimate: "Cause of death or injury"
    - Measure: "Prevalence"
    - Metric: "Rate"
    - Location: "select all countries and territories", then also add "Global", different "World Bank" income groups, entities in "Four World regions".
    - Age: 15-19, 20-24, 25-29, 30-34, 35-39, 40-44, 45-49, 50-54, 55-59, 60-64, 65-69, 70+
    - Sex: Both, Female, Male
    - Year: all
- Click on "Download"
- A link with all files to be downloaded will be sent to your email
- Download all files, and unarchive them.
- Create a new folder and move all CSVs files in there. Additionally, put the citation.txt file (all of them should be identical)
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"ihme_gbd/{SNAPSHOT_VERSION}/gbd_mental_health_prevalence_rate.zip")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
