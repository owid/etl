from pathlib import Path

import click
import structlog
import yaml

from etl import paths

log = structlog.get_logger()


@click.command()
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
)
def move_steps_to_archive_cli(dry_run=False) -> None:
    """Move steps from archive DAG into etl/steps/archive folder."""
    steps = []
    for dag_yaml in paths.DAG_ARCHIVE_FILE.parent.glob("*.yml"):
        with open(dag_yaml, "r") as f:
            steps += yaml.safe_load(f)["steps"].keys()

    for step in steps:
        step_path = paths.STEP_DIR / "data" / step.split("//")[1]
        files_to_move = step_path.parent.glob(step_path.name + "*")
        for file in files_to_move:
            new_path = Path(str(file).replace("/data/", "/archive/"))

            log.info(
                "move_file",
                old_path=str(file.relative_to(paths.BASE_DIR)),
                new_path=str(new_path.relative_to(paths.BASE_DIR)),
            )
            if not dry_run:
                new_path.parent.mkdir(parents=True, exist_ok=True)
                file.rename(new_path)


if __name__ == "__main__":
    move_steps_to_archive_cli()
