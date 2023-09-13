from pathlib import Path
from typing import Optional, Set

import click
import structlog
import yaml

from etl import paths
from etl.steps import load_dag

log = structlog.get_logger()


@click.command()
@click.option(
    "--include",
    type=str,
    help="Move only steps matching pattern",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    type=bool,
)
def move_steps_to_archive_cli(include: Optional[None], dry_run=False) -> None:
    """Move steps from archive DAG into etl/steps/archive folder."""
    active_steps = _load_active_steps()
    active_snapshots_dirs = {snap.split("//")[1].rsplit("/", 1)[0] for snap in active_steps if "snapshot" in snap}

    for dag_yaml in paths.DAG_ARCHIVE_FILE.parent.glob("*.yml"):
        with open(dag_yaml, "r") as f:
            meta = yaml.safe_load(f)
            steps = meta["steps"].keys()

        for step in steps:
            assert step not in active_steps, f"Step {step} has not been archived"

            step_path = paths.STEP_DIR / "data" / step.split("//")[1]

            if include and include not in step:
                continue

            snapshots = [snap for snap in meta["steps"][step] if "snapshot" in snap]

            # move step files
            files_to_move = step_path.parent.glob(step_path.name + "*")
            for file in files_to_move:
                new_path = Path(str(file).replace("/data/", "/archive/"))

                # file has been moved already
                if not file.exists():
                    continue

                log.info(
                    "move_step",
                    old_path=str(file.relative_to(paths.BASE_DIR)),
                    new_path=str(new_path.relative_to(paths.BASE_DIR)),
                )
                if not dry_run:
                    new_path.parent.mkdir(parents=True, exist_ok=True)
                    file.rename(new_path)

            """This is currently disabled, because DVC complains about missing files
            # move snapshots
            for snap in snapshots:
                old_dir = (paths.SNAPSHOTS_DIR / snap.split("//")[1]).parent

                # if the directory of the snapshot is still used, skip archiving
                if str(old_dir.relative_to(paths.SNAPSHOTS_DIR)) in active_snapshots_dirs:
                    continue

                if not old_dir.exists():
                    continue

                # move snapshots
                new_dir = Path(str(old_dir).replace("/snapshots/", "/snapshots/archive/"))

                log.info(
                    "move_snapshot",
                    old_dir=str(old_dir.relative_to(paths.BASE_DIR)),
                    new_dir=str(new_dir.relative_to(paths.BASE_DIR)),
                )
                if not dry_run:
                    new_dir.parent.mkdir(parents=True, exist_ok=True)
                    old_dir.rename(new_dir)
            """


def _load_active_steps() -> Set[str]:
    active_dag = load_dag(paths.DAG_FILE)
    return set(active_dag.keys()) | {dep for deps in active_dag.values() for dep in deps}


if __name__ == "__main__":
    move_steps_to_archive_cli()
