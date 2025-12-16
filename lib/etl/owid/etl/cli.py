"""Command-line interface for the ETL framework."""

from pathlib import Path
from typing import Optional

import click

from .config import ETLConfig, set_config
from .execution import run_dag


@click.command()
@click.argument("steps", nargs=-1)
@click.option(
    "--dag",
    "dag_file",
    required=True,
    type=click.Path(exists=True),
    help="Path to the DAG YAML file",
)
@click.option(
    "--base-dir",
    type=click.Path(exists=True),
    default=".",
    help="Base directory for the ETL project",
)
@click.option(
    "--steps-dir",
    type=click.Path(exists=True),
    help="Directory containing step code (defaults to base-dir/steps/data)",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    help="Directory for output data (defaults to base-dir/data)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print what would be run without executing",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Run steps even if they are not dirty",
)
@click.option(
    "--only",
    "-o",
    is_flag=True,
    help="Only run specified steps (no dependencies)",
)
@click.option(
    "--downstream",
    "-d",
    is_flag=True,
    help="Also run steps that depend on specified steps",
)
@click.option(
    "--exclude",
    "-x",
    "excludes",
    multiple=True,
    help="Patterns of steps to exclude",
)
@click.option(
    "--workers",
    "-w",
    default=1,
    type=int,
    help="Number of parallel workers",
)
def run(
    steps: tuple,
    dag_file: str,
    base_dir: str,
    steps_dir: Optional[str],
    data_dir: Optional[str],
    dry_run: bool,
    force: bool,
    only: bool,
    downstream: bool,
    excludes: tuple,
    workers: int,
) -> None:
    """Run ETL steps.

    STEPS are patterns matching step names in the DAG (supports regex).
    If no steps are specified, all steps will be considered.

    Examples:

        # Run a specific step and its dependencies
        owid-etl run --dag dag.yml data://garden/example/2024/dataset

        # Run all meadow steps
        owid-etl run --dag dag.yml "meadow/"

        # Dry run to see what would be executed
        owid-etl run --dag dag.yml --dry-run data://garden/example/2024/dataset
    """
    base_path = Path(base_dir).resolve()

    # Set up configuration
    config = ETLConfig(
        base_dir=base_path,
        steps_dir=Path(steps_dir).resolve() if steps_dir else base_path / "steps" / "data",
        dag_file=Path(dag_file).resolve(),
        data_dir=Path(data_dir).resolve() if data_dir else None,
    )
    set_config(config)

    # Run the DAG
    run_dag(
        steps=list(steps),
        config=config,
        dry_run=dry_run,
        force=force,
        only=only,
        downstream=downstream,
        excludes=list(excludes) if excludes else None,
        workers=workers,
    )


@click.group()
def cli() -> None:
    """OWID ETL - A framework for building data pipelines."""
    pass


cli.add_command(run)

# Alias for entry point
main = cli


if __name__ == "__main__":
    main()
