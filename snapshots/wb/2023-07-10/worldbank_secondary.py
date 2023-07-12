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
        'School life expectancy, secondary, both sexes (years)'
        'School life expectancy, secondary, female (years)'
        'School life expectancy, secondary, male (years)'
        'School life expectancy, secondary, gender parity index (GPI)'
        'Percentage of upper secondary schools providing life skills-based HIV and sexuality education'
        'Percentage of lower secondary schools providing life skills-based HIV and sexuality education'
        'Percentage of enrolment in secondary education in private institutions (%)'
        'Proportion of secondary schools with access to Internet for pedagogical purposes (%)'
        'Proportion of secondary schools with access to computers for pedagogical purposes (%)'
        'Proportion of lower secondary schools with access to Internet for pedagogical purposes (%)'
        'Proportion of lower secondary schools with access to basic drinking water (%)'
        'Proportion of lower secondary schools with access to electricity (%)'
        'Proportion of lower secondary schools with basic handwashing facilities (%)'
        'Proportion of lower secondary schools with access to adapted infrastructure and materials for students with disabilities (%)'
        'Proportion of upper secondary schools with access to basic drinking water (%)'
        'Proportion of upper secondary schools with access to electricity (%)'
        'Proportion of upper secondary schools with access to adapted infrastructure and materials for students with disabilities (%)'
        'Proportion of upper secondary schools with basic handwashing facilities (%)'
        'Total net enrolment rate, lower secondary, female (%)'
        'Total net enrolment rate, lower secondary, male (%)'
        'Total net enrolment rate, upper secondary, female (%)'
        'Total net enrolment rate, upper secondary, male (%)'
        'Total net enrolment rate, lower secondary, both sexes (%)'
        'Total net enrolment rate, lower secondary, gender parity index (GPI)'
        'Total net enrolment rate, upper secondary, both sexes (%)'
        'Total net enrolment rate, upper secondary, gender parity index (GPI)'
        'Gross enrolment ratio, primary and lower secondary, both sexes (%)'
        'Out-of-school adolescents and youth of secondary school age, both sexes (number)'
        'Out-of-school adolescents and youth of secondary school age, male (number)'
        'Out-of-school adolescents and youth of secondary school age, female (number)'
        'Completion rate, lower secondary education, female (%)'
        'Completion rate, lower secondary education, male (%)'
        'Completion rate, lower secondary education, both sexes (%)'
        'Completion rate, lower secondary education, adjusted gender parity index (GPIA)'
        'Completion rate, upper secondary education, both sexes (%)'
        'Completion rate, upper secondary education, fourth quintile, adjusted gender parity index (GPIA)'
        'Completion rate, upper secondary education, male (%)'
        'Completion rate, upper secondary education, female (%)'
    3. In the top right corner, click on the "Download" button.
    4. Choose the "CSV" format and initiate the download.

    Note: Ensure that the downloaded dataset contains the desired PISA scores and associated information.
    """
    # Create a new snapshot.
    snap = Snapshot(f"worldbank_education/{SNAPSHOT_VERSION}/worldbank_secondary.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
