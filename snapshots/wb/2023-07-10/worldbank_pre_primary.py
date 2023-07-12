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
        'Adjusted net enrolment rate, one year before the official primary entry age, adjusted gender parity index (GPIA)'
        'Adjusted net enrolment rate, one year before the official primary entry age, female (%)'
        'Adjusted net enrolment rate, one year before the official primary entry age, male (%)'
        'Adjusted net enrolment rate, one year before the official primary entry age, both sexes (%)'
        'Percentage of enrolment in pre-primary education in private institutions (%)'
        'Gross enrolment ratio, pre-primary, both sexes (%)'
        'Gross enrolment ratio, pre-primary, male (%)'
        'Gross enrolment ratio, pre-primary, female (%)'
        'Gross enrolment ratio, pre-primary, adjusted gender parity index (GPIA)'
        'Enrolment in pre-primary education, female (number)'
        'Enrolment in pre-primary education, male (number)'
        'Enrolment in pre-primary education, both sexes (number)'
        'Theoretical duration of pre-primary education (years)'
        'Number of years of free pre-primary education guaranteed in legal frameworks'
        'Number of years of compulsory pre-primary education guaranteed in legal frameworks'
        'School life expectancy, pre-primary, both sexes (years)'
        'School life expectancy, pre-primary, gender parity index (GPI)'
        'School life expectancy, pre-primary, male (years)'
        'School life expectancy, pre-primary, female (years)'
        'Out-of-school rate for children one year younger than official primary entrance age, both sexes (%)'
        'Out-of-school rate for children one year younger than official primary entrance age, male (%)'
        'Out-of-school rate for children one year younger than official primary entrance age, female (%)'
        'Out-of-school rate for children one year younger than official age, adjusted gender parity index (GPIA)'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"worldbank_education/{SNAPSHOT_VERSION}/worldbank_pre_primary.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
