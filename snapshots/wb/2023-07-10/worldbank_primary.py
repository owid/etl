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
        'Total net enrolment rate, primary, both sexes (%)'
        'Total net enrolment rate, primary, gender parity index (GPI)'
        'Total net enrolment rate, primary, female (%)'
        'Total net enrolment rate, primary, male (%)'
        'Gross enrolment ratio, primary, both sexes (%)'
        'Enrolment in primary education, both sexes (number)'
        'Enrolment in primary education, male (number)'
        'Enrolment in primary education, female (number)'
        'Out-of-school children of primary school age, both sexes (number)'
        'Out-of-school children of primary school age, male (number)'
        'Out-of-school children of primary school age, female (number)'
        'Survival rate to the last grade of primary education, both sexes (%)'
        'Survival rate to the last grade of primary education, gender parity index (GPI)'
        'Survival rate to the last grade of primary education, female (%)'
        'Survival rate to the last grade of primary education, male (%)'
        'Percentage of enrolment in primary education in private institutions (%)'
        'Proportion of primary schools with access to Internet for pedagogical purposes (%)'
        'Proportion of primary schools with access to computers for pedagogical purposes (%)'
        'Proportion of primary schools with access to adapted infrastructure and materials for students with disabilities (%)'
        'Proportion of primary schools with access to basic drinking water (%)'
        'Proportion of primary schools with access to electricity (%)'
        'Proportion of primary schools with basic handwashing facilities (%)'
        'School life expectancy, primary, gender parity index (GPI)'
        'School life expectancy, primary, both sexes (years)'
        'School life expectancy, primary, female (years)'
        'School life expectancy, primary, male (years)'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"worldbank_education/{SNAPSHOT_VERSION}/worldbank_primary.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
