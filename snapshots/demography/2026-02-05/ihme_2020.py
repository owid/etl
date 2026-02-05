"""Files for this snapshot have been downloaded manually from the IHME website.
Some of the files include retrospective estimates and forecasts together, some are split, some just provide forecasts, etc.

INSTRUCTIONS:
1. Go to https://ghdx.healthdata.org/record/ihme-data/global-population-forecasts-2017-2100
2. Register / Log In
3. Click on "Files" tab, and download relevant files, which as of February 2026 are:
    - Population forecast, all scenarios, all ages and both sexes: 2018-2100 [CSV]
    - Population estimates: 1950-2017 - Data and Codebook [CSV]
    - Total fertility rate (TFR) retrospective estimates and forecasts, all scenarios: 1950-2100 [CSV]
    - Life expectancy retrospective estimates and forecasts, all scenarios: 1990-2100 - Data and Codebook [CSV]
    - Migration forecast: 2018-2100 - Data and Codebook [CSV]

EXTRA: Refer to their "Data Release Information Sheet" pdf file to learn more about the data (https://ghdx.healthdata.org/sites/default/files/record-attached-files/IHME_POP_2017_2100_INFO_SHEET_Y2020M05D01.PDF)
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, help="Upload to S3")
@click.option("--path-to-population", type=str, default=None, help="Path to population data file")
@click.option("--path-to-population-retro", type=str, default=None, help="Path to population_retro data file")
@click.option("--path-to-fertility", type=str, default=None, help="Path to fertility data file")
@click.option("--path-to-life-expectancy", type=str, default=None, help="Path to life_expectancy data file")
@click.option("--path-to-migration", type=str, default=None, help="Path to migration data file")
def run(
    upload: bool = True,
    path_to_population: str | None = None,
    path_to_population_retro: str | None = None,
    path_to_fertility: str | None = None,
    path_to_life_expectancy: str | None = None,
    path_to_migration: str | None = None,
) -> None:
    """Create a new snapshot.

    Example usage:
        IHME_DIR=/home/lucas/repos/etl/z_ihme_demo
python snapshots/demography/2026-02-05/ihme_2020.py \
    --path-to-population $IHME_DIR/population.zip \
    --path-to-population-retro $IHME_DIR/population_retro.zip \
    --path-to-fertility $IHME_DIR/fertility.zip \
    --path-to-life-expectancy $IHME_DIR/life_expectancy.zip \
    --path-to-migration $IHME_DIR/migration.zip

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_population: Path to local population data file.
        path_to_population_retro: Path to local population_retro data file.
        path_to_fertility: Path to local fertility data file.
        path_to_life_expectancy: Path to local life_expectancy data file.
        path_to_migration: Path to local migration data file.
    """
    # Map file names to their paths
    file_paths = {
        "population": path_to_population,
        "population_retro": path_to_population_retro,
        "fertility": path_to_fertility,
        "life_expectancy": path_to_life_expectancy,
        "migration": path_to_migration,
    }

    # Process only files with provided paths
    for name, path_to_file in file_paths.items():
        if path_to_file is None:
            paths.log.info(f"Skipping {name} (no path provided)")
            continue

        filename_dvc = f"ihme_2020_{name}.zip"
        paths.log.info(f"Creating snapshot {filename_dvc}")
        snap = paths.init_snapshot(filename_dvc)

        snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
