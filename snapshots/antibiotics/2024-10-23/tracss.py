"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Hash out for now - I was contemplating adding multiple years but I'm not sure how comparable they are.
# files = {
#    2024: "https://new.amrcountryprogress.org/uploads/TrACSS_Survey_2024_Dataset_v2.xlsx",
#    2023: "https://new.amrcountryprogress.org/uploads/AMR-self-assessment-survey-responses-TrACSS-2023.xlsx",
#    2022: "https://new.amrcountryprogress.org/uploads/AMR-self-assessment-survey-responses-2020-2021.xlsx",
#    2021: "https://new.amrcountryprogress.org/uploads/Year%20five%20TrACSS%20complete%20data%20for%20publication.xlsx",
#    2020: "https://new.amrcountryprogress.org/uploads/AMR%20self%20assessment%20survey%20responses%202019-2020%20(Excel%20format).xls",
#    2019: "https://new.amrcountryprogress.org/uploads/AMR-self-assessment-survey-country-responses-2018-19.xls",
#    2018: "https://new.amrcountryprogress.org/uploads/AMR-self-assessment-survey-country-responses-2017-18.xlsx",
# }


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/tracss.xlsx")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
