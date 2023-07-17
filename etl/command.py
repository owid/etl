#!/usr/bin/env python
#
#  etl.py
#

import re
import resource
import sys
import time
from contextlib import contextmanager
from os import environ
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional, Set

import click
from ipdb import launch_ipdb_on_exception

from etl import config, paths
from etl.snapshot import snapshot_catalog
from etl.steps import DAG, DataStep, Step, compile_steps, load_dag, select_dirty_steps

config.enable_bugsnag()

# if the number of open files allowed is less than this, increase it
LIMIT_NOFILE = 4096


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option("--force", is_flag=True, help="Redo a step even if it appears done and up-to-date")
@click.option("--private", is_flag=True, help="Execute private steps")
# TODO: once grapher channel stops using the grapher db, remove this flag
@click.option(
    "--grapher-channel/--no-grapher-channel",
    default=True,
    type=bool,
    help="Include grapher channel (OWID staff only, needs access to DB)",
)
@click.option(
    "--grapher/--no-grapher",
    default=False,
    type=bool,
    help="Upsert datasets from grapher channel to DB (OWID staff only, needs access to DB)",
)
@click.option("--ipdb", is_flag=True, help="Run the debugger on uncaught exceptions")
@click.option(
    "--backport",
    is_flag=True,
    help="Add steps for backporting OWID datasets",
)
@click.option(
    "--downstream",
    is_flag=True,
    help="Include downstream dependencies (steps that depend on the included steps)",
)
@click.option(
    "--only",
    is_flag=True,
    help="Only run the selected step (no upstream or downstream dependencies). Overrides `downstream` option",
)
@click.option("--exclude", help="Comma-separated patterns to exclude")
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DEFAULT_DAG_FILE,
)
@click.option(
    "--workers",
    type=int,
    help="Thread workers to parallelize which steps need rebuilding (steps execution is not parallelized)",
    default=5,
)
@click.option(
    "--strict/--no-strict",
    is_flag=True,
    help="Force strict or lax validation on DAG steps, e.g. checks for primary keys in data steps.",
    default=None,
)
@click.argument("steps", nargs=-1)
def main_cli(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher_channel: bool = True,
    grapher: bool = False,
    backport: bool = False,
    ipdb: bool = False,
    downstream: bool = False,
    only: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DEFAULT_DAG_FILE,
    workers: int = 5,
    strict: Optional[bool] = None,
) -> None:
    _update_open_file_limit()

    # enable grapher channel when called with --grapher
    grapher_channel = grapher_channel or grapher

    kwargs = dict(
        steps=steps,
        dry_run=dry_run,
        force=force,
        private=private,
        grapher_channel=grapher_channel,
        grapher=grapher,
        backport=backport,
        downstream=downstream,
        only=only,
        exclude=exclude,
        dag_path=dag_path,
        workers=workers,
        strict=strict,
    )

    # propagate workers to grapher upserts
    if workers == 1:
        config.GRAPHER_INSERT_WORKERS = 1

    if ipdb:
        config.IPDB_ENABLED = True
        config.GRAPHER_INSERT_WORKERS = 1
        kwargs["workers"] = 1
        with launch_ipdb_on_exception():
            main(**kwargs)  # type: ignore
    else:
        main(**kwargs)  # type: ignore


def main(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher_channel: bool = True,
    grapher: bool = False,
    backport: bool = False,
    downstream: bool = False,
    only: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DEFAULT_DAG_FILE,
    workers: int = 5,
    strict: Optional[bool] = None,
) -> None:
    """
    Execute all ETL steps listed in dag file.
    """
    if grapher:
        sanity_check_db_settings()

    dag = construct_dag(dag_path, backport=backport, private=private, grapher=grapher)

    excludes = exclude.split(",") if exclude else []

    # Run the steps we have selected, and everything downstream of them
    run_dag(
        dag,
        steps,
        dry_run=dry_run,
        force=force,
        private=private,
        include_grapher_channel=grapher_channel,
        downstream=downstream,
        only=only,
        excludes=excludes,
        workers=workers,
        strict=strict,
    )


def sanity_check_db_settings() -> None:
    """
    Give a nice error if the DB has not been configured.
    """
    if config.GRAPHER_USER_ID is None:
        click.echo("ERROR: No grapher user id has been set in the environment.")
        click.echo("       Did you configure the MySQL connection in .env?")
        sys.exit(1)


def construct_dag(dag_path: Path, backport: bool, private: bool, grapher: bool) -> DAG:
    """Construct full DAG."""

    # Load our graph of steps and the things they depend on
    dag = load_dag(dag_path)

    # If backport is set, add all backport steps. Otherwise add only those used in DAG
    if backport:
        filter_steps = None
    else:
        # Get all backported datasets that are used in the DAG
        filter_steps = {dep for deps in dag.values() for dep in deps if dep.startswith("backport://")}

    backporting_dag = _backporting_steps(private, filter_steps=filter_steps)

    dag.update(backporting_dag)

    # If --grapher is set, add steps for upserting to DB
    if grapher:
        dag.update(_grapher_steps(dag, private))

    return dag


def run_dag(
    dag: DAG,
    includes: Optional[List[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    include_grapher_channel: bool = False,
    downstream: bool = False,
    only: bool = False,
    excludes: Optional[List[str]] = None,
    workers: int = 1,
    strict: Optional[bool] = None,
) -> None:
    """
    Run the selected steps, and anything that needs updating based on them. An empty
    selection means "run all steps".

    By default, data steps do not re-run if they appear to be up-to-date already by
    looking at their checksum.
    """
    excludes = excludes or []
    if not include_grapher_channel:
        # exclude grapher channel
        excludes.append("data://grapher")

    _validate_private_steps(dag)

    if not private:
        excludes.append("-private://")

    steps = compile_steps(dag, includes, excludes, downstream=downstream, only=only)

    if not steps:
        raise ValueError(
            f"No steps matched the given input `{' '.join(includes or [])}`. Check spelling or consult `etl --help` for more options"
        )

    if not force:
        print("Detecting which steps need rebuilding...")
        start_time = time.time()
        steps = select_dirty_steps(steps, workers)
        click.echo(f"{click.style('OK', fg='blue')} ({time.time() - start_time:.1f}s)")

    if not steps:
        print("All datasets up to date!")
        return

    print(f"Running {len(steps)} steps:")
    for i, step in enumerate(steps, 1):
        print(f"{i}. {step}...")
        if not dry_run:
            strict = _detect_strictness_level(step, strict)
            with strictness_level(strict):
                time_taken = timed_run(lambda: step.run())
                click.echo(f"{click.style('OK', fg='blue')} ({time_taken:.1f}s)")
                print()


def _detect_strictness_level(step: Step, strict: Optional[bool] = None) -> bool:
    # honour the command-line argument over anything else
    if strict is not None:
        return strict

    # only data steps can be strict, and only after meadow
    if not isinstance(step, DataStep) or step.channel in ("meadow", "open_numbers"):
        return False

    # now it depends on the version
    # TODO fix the "latest" cases as well
    return step.version != "latest" and step.version >= config.STRICT_AFTER


@contextmanager
def strictness_level(strict: bool) -> Iterator[None]:
    # start from a clean slate
    if "OWID_STRICT" in environ:
        del environ["OWID_STRICT"]

    if strict:
        # enable strict mode
        environ["OWID_STRICT"] = "true"

    yield

    # no need to clean up


def timed_run(f: Callable[[], Any]) -> float:
    start_time = time.time()
    f()
    return time.time() - start_time


def _validate_private_steps(dag: DAG) -> None:
    """Make sure there are no public steps that have private steps as dependency."""
    for step_name, step_dependencies in dag.items():
        # does not apply for private and grapher steps
        if _is_private_step(step_name) or step_name.startswith("grapher://"):
            continue
        for dependency in step_dependencies:
            if _is_private_step(dependency):
                raise ValueError(f"Public step {step_name} has a dependency on private {dependency}")


def _is_private_step(step_name: str) -> bool:
    return bool(re.findall(r".*?-private://", step_name))


def _backporting_steps(private: bool, filter_steps: Optional[Set[str]] = None) -> DAG:
    """Return a DAG of steps for backporting datasets."""
    dag: DAG = {}

    # get all backports, this takes a long time
    if filter_steps is None:
        match = "backport/.*"
    else:
        match = "|".join([step.split("/")[-1] for step in filter_steps])

    # load all backported snapshots
    for snap in snapshot_catalog(match):
        # skip private backported steps
        if not private and not snap.metadata.is_public:
            continue

        # two files are generated for each dataset, skip one
        if snap.metadata.short_name.endswith("_values"):
            short_name = snap.metadata.short_name.removesuffix("_values")

            private_suffix = "" if snap.metadata.is_public else "-private"

            dag[f"backport{private_suffix}://backport/owid/latest/{short_name}"] = {
                f"snapshot{private_suffix}://backport/latest/{short_name}_values.feather",
                f"snapshot{private_suffix}://backport/latest/{short_name}_config.json",
            }

    return dag


def _grapher_steps(dag: DAG, private: bool) -> DAG:
    # dynamically generate a grapher:// step for every grapher data step
    new_dag = {}
    for step in list(dag.keys()):
        # match regex with prefix data or data-private (only if we run it with --private)
        if re.match(r"^data://grapher/", step) or (private and re.match(r"^data-private://grapher/", step)):
            new_dag[re.sub(r"^(data|data-private)://", "grapher://", step)] = {step}

    return new_dag


def _update_open_file_limit() -> None:
    # avoid errors due to not enough allowed open files
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < LIMIT_NOFILE:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(LIMIT_NOFILE, hard_limit), hard_limit))


if __name__ == "__main__":
    main_cli()
