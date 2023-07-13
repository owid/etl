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
        'Total net enrolment rate, primary, male (%)'
        'Total net enrolment rate, primary, female (%)'
        'Total net enrolment rate, primary, both sexes (%)'
        'Total net enrolment rate, primary, gender parity index (GPI)'
        'Percentage of enrolment in primary education in private institutions (%)'
        'Gross enrolment ratio, primary, both sexes (%)'
        'Out-of-school rate for children of primary school age, both sexes (%)'
        'Out-of-school rate for children of primary school age, female (%)'
        'Out-of-school rate for children of primary school age, adjusted gender parity index (GPIA)'
        'Out-of-school rate for children of primary school age, male (%)'
        'Out-of-school rate for children of primary school age, both sexes (household survey data) (%)'
        'Out-of-school rate for children of primary school age, female (household survey data) (%)'
        'Out-of-school rate for children of primary school age, adjusted gender parity index (household survey data) (GPIA)'
        'Out-of-school rate for children of primary school age, male (household survey data) (%)'
        'Completion rate, primary education, male (%)'
        'Completion rate, primary education, female (%)'
        'Completion rate, primary education, both sexes (%)'
        'Completion rate, primary education, adjusted gender parity index (GPIA)'
        'School life expectancy, primary, both sexes (years)'
        'School life expectancy, primary, gender parity index (GPI)'
        'School life expectancy, primary, male (years)'
        'School life expectancy, primary, female (years)'
        'Proportion of primary schools with access to adapted infrastructure and materials for students with disabilities (%)'
        'Proportion of primary schools with access to basic drinking water (%)'
        'Proportion of primary schools with access to computers for pedagogical purposes (%)'
        'Proportion of primary schools with access to electricity (%)'
        'Proportion of primary schools with access to Internet for pedagogical purposes (%)'
        'Proportion of primary schools with basic handwashing facilities (%)'
        'Proportion of primary schools with single-sex basic sanitation facilities (%)'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/education_primary.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
