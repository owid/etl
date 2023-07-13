"""Script to create a snapshot of dataset 'World Bank Education Statistics: Learning Outcomes (2018)'."""

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
    """
    Instructions:
    1. Visit the following URL:  https://databank.worldbank.org/reports.aspx?source=Education%20Statistics#
    2. Select the following indicators:
        'Gross enrolment ratio for tertiary education, adjusted gender parity index (GPIA)'
        'Gross enrolment ratio for tertiary education, female (%)'
        'Gross enrolment ratio for tertiary education, both sexes (%)'
        'Gross enrolment ratio for tertiary education, male (%)'
        'Percentage of graduates from programmes other than Science, Technology, Engineering and Mathematics in tertiary education, both sexes (%)'
        'Percentage of graduates from Science, Technology, Engineering and Mathematics programmes in tertiary education, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Agriculture, Forestry, Fisheries and Veterinary programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Arts and Humanities programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Business, Administration and Law programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Education programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Engineering, Manufacturing and Construction programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Health and Welfare programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Information and Communication Technologies programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Natural Sciences, Mathematics and Statistics programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from programmes in unspecified fields, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Services programmes, both sexes (%)'
        'Percentage of graduates from tertiary education graduating from Social Sciences, Journalism and Information programmes, both sexes (%)'
        'Outbound mobility ratio, all regions, both sexes (%)'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education_tertiary.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
