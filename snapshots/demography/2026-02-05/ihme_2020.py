"""Script to create a snapshot of dataset."""

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

    print(1)
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
