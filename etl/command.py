#!/usr/bin/env python
#
#  etl.py
#

import difflib
import itertools
import json
import re
import resource
import sys
import time
from collections.abc import MutableMapping
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from contextlib import contextmanager
from functools import partial
from graphlib import TopologicalSorter
from multiprocessing import Manager
from os import environ
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

import rich_click as click
import structlog
from ipdb import launch_ipdb_on_exception

from etl import config, files, paths
from etl.dag_helpers import load_dag
from etl.steps import (
    DAG,
    DataStep,
    GrapherStep,
    Step,
    compile_steps,
    parse_step,
    select_dirty_steps,
)

config.enable_sentry()

# NOTE: I tried enabling this, but ran into weird errors with unit tests and inconsistencies
#   with owid libraries. It's better to wait for an official pandas 3.0 release and update
#   it all at once.
# Use string[pyarrow] by default, this will become True in pandas 3.0
# pd.options.future.infer_string = True

# if the number of open files allowed is less than this, increase it
LIMIT_NOFILE = 4096

log = structlog.get_logger()


@click.command(name="run")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview the steps without actually running them.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Re-run the steps even if they appear done and up-to-date",
)
@click.option(
    "--private",
    "-p",
    is_flag=True,
    help="Run private steps.",
)
@click.option(
    "--grapher/--no-grapher",
    "-g/-ng",
    default=False,
    type=bool,
    help="Upsert datasets from grapher channel to DB _(OWID staff only, DB access required)_",
)
@click.option(
    "--export/--no-export",
    default=False,
    type=bool,
    help="Run export steps like saving explorer _(OWID staff only, access required)_",
)
@click.option(
    "--ipdb",
    is_flag=True,
    help="Run the debugger on uncaught exceptions.",
)
@click.option(
    "--downstream",
    "-d",
    is_flag=True,
    help="Include downstream dependencies (steps that depend on the included steps).",
)
@click.option(
    "--only",
    "-o",
    is_flag=True,
    help="Only run the selected step (no upstream or downstream dependencies). Overrides `--downstream` option.",
)
@click.option(
    "--exact-match",
    "-x",
    is_flag=True,
    help="Steps should exactly match the arguments (if so, pass the steps with their full name, e.g. 'data://garden/.../step_name').",
)
@click.option(
    "--exclude",
    "-e",
    help="Comma-separated patterns to exclude",
)
@click.option(
    "--dag-path",
    type=click.Path(exists=True),
    help="Path to DAG yaml file",
    default=paths.DEFAULT_DAG_FILE,
)
@click.option(
    "--workers",
    "-w",
    type=int,
    help="Parallelize execution of steps.",
    default=1,
)
@click.option(
    "--use-threads/--no-threads",
    "-t/-nt",
    type=bool,
    help="Use threads when checking dirty steps and upserting to MySQL. Turn off when debugging.",
    default=True,
)
@click.option(
    "--strict/--no-strict",
    "-s/-ns",
    is_flag=True,
    help="Force strict or lax validation on DAG steps (e.g. checks for primary keys in data steps).",
    default=None,
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    help="Run ETL infinitely and update changed files.",
)
@click.argument(
    "steps",
    nargs=-1,
    type=str,
)
def main_cli(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    grapher: bool = False,
    export: bool = False,
    ipdb: bool = False,
    downstream: bool = False,
    only: bool = False,
    exact_match: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DEFAULT_DAG_FILE,
    workers: int = 1,
    use_threads: bool = True,
    strict: Optional[bool] = None,
    watch: bool = False,
) -> None:
    """Generate datasets by running their corresponding ETL steps.

    Run all ETL steps in the DAG matching the value of `STEPS`. A match is a dataset with an uri that contains the value of any of the words in `STEPS`.

    **Example 1**: Run steps matching "mars" in the DAG file:

    ```
    $ etl run mars
    ```

    **Example 2**: Preview those steps that match "mars" or "prio" (i.e. don't run them):

    ```
    $ etl run mars prio
    ```

    **Example 3**: If you only want to preview what would be executed, use the `--dry-run` flag:

    ```
    $ etl run mars prio --dry-run
    ```
    """
    _update_open_file_limit()

    # make everything single threaded, useful for debugging
    if not use_threads:
        config.GRAPHER_INSERT_WORKERS = 1
        config.DIRTY_STEPS_WORKERS = 1
        workers = 1

    # GRAPHER_INSERT_WORKERS should be split among workers
    if workers > 1:
        config.GRAPHER_INSERT_WORKERS = config.GRAPHER_INSERT_WORKERS // workers

    kwargs = dict(
        steps=steps,
        dry_run=dry_run,
        force=force,
        private=private,
        grapher=grapher,
        export=export,
        downstream=downstream,
        only=only,
        exact_match=exact_match,
        exclude=exclude,
        dag_path=dag_path,
        workers=workers,
        strict=strict,
    )

    if watch:
        runs = itertools.chain([None], files.watch_folder(paths.STEP_DIR))
    else:
        runs = [None]

    for _ in runs:
        if ipdb:
            config.IPDB_ENABLED = True
            config.GRAPHER_INSERT_WORKERS = 1
            config.DIRTY_STEPS_WORKERS = 1
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
    grapher: bool = False,
    export: bool = False,
    downstream: bool = False,
    only: bool = False,
    exact_match: bool = False,
    exclude: Optional[str] = None,
    dag_path: Path = paths.DEFAULT_DAG_FILE,
    workers: int = 1,
    strict: Optional[bool] = None,
) -> None:
    """
    Execute all ETL steps listed in dag file.
    """
    if grapher:
        sanity_check_db_settings()

    dag = construct_dag(dag_path, private=private, grapher=grapher, export=export)

    excludes = exclude.split(",") if exclude else []

    # Run the steps we have selected, and everything downstream of them
    run_dag(
        dag,
        steps,
        dry_run=dry_run,
        force=force,
        private=private,
        downstream=downstream,
        only=only,
        exact_match=exact_match,
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


def construct_dag(dag_path: Path, private: bool, grapher: bool, export: bool) -> DAG:
    """Construct full DAG."""

    # Load our graph of steps and the things they depend on
    dag = load_dag(dag_path)

    # Make sure we don't have both public and private steps in the same DAG
    _check_public_private_steps(dag)

    if export:
        # If there were any "export://multidim" or "export://explorers" steps, keep them in the dag, to be executed.
        for step in list(dag.keys()):
            if step.startswith("export://multidim/") or step.startswith("export://explorers/"):
                # If private is false and any of the dependencies are private, continue
                if not private and any(_is_private_step(dep) for dep in dag[step]):
                    continue

                # We want to execute export steps after any grapher://grapher steps,
                # to ensure that any indicators required by the mdim step are already pushed to DB.
                # To achieve that, replace "data://grapher" dependencies with "grapher://grapher".
                dag[step] = {
                    re.sub(r"^(data|data-private)://", "grapher://", dep)
                    if re.match(r"^data://grapher/", dep) or re.match(r"^data-private://grapher/", dep)
                    else dep
                    for dep in dag[step]
                }

        # Finally, ensure that the added grapher://grapher steps will be executed,
        # by activating the "grapher" flag.
        grapher = True
    else:
        # If there were any "export://" steps in the dag, remove them.
        dag = {step: deps for step, deps in dag.items() if not step.startswith("export://")}

    # If --grapher is set, add all steps for upserting to DB
    if grapher:
        steps = _grapher_steps(dag, private)
        dag.update(steps)
    # Otherwise just add those in dependencies, even if they're private
    else:
        steps = _grapher_steps(dag, True)
        for deps in list(dag.values()):
            for dep in deps:
                if dep.startswith("grapher://"):
                    dag[dep] = steps[dep]

    # Validate the DAG
    _check_dag_completeness(dag)

    return dag


def run_dag(
    dag: DAG,
    includes: Optional[List[str]] = None,
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    downstream: bool = False,
    only: bool = False,
    exact_match: bool = False,
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

    if not private:
        excludes.append("-private://")

    # Exclude grapher regions, they're fetched by owid-grapher as CSV from catalog
    # but are not supposed to be in DB
    excludes.append("grapher://grapher/regions/latest/regions")

    steps = compile_steps(dag, includes, excludes, downstream=downstream, only=only, exact_match=exact_match)

    if not private:
        _validate_private_steps(steps)

    if not steps:
        # If no steps are found, the most likely case is that the step passed as argument was misspelled.
        # Print a short error message, show a list of the closest matches, and exit.
        includes_str = " ".join(includes or [])
        print(f"No steps matched `{includes_str}`. Closest matches:")
        # NOTE: We could use a better edit distance to find the closest matches.
        for match in difflib.get_close_matches(includes_str, list(dag), n=5, cutoff=0.0):
            print(match)
        sys.exit(1)

    # do not run dependencies if `only` is set by setting them to non-dirty
    if only:
        for step in steps:
            _set_dependencies_to_nondirty(step)

    if not force:
        print("--- Detecting which steps need rebuilding...")
        start_time = time.time()
        steps = select_dirty_steps(steps, workers=config.DIRTY_STEPS_WORKERS)
        click.echo(f"{click.style('OK', fg='blue')} ({time.time() - start_time:.1f}s)")

    if not steps:
        print("--- All datasets up to date!")
        return

    # Calculate total expected time for all steps (if run sequentially)
    total_expected_time_seconds = sum(_get_execution_time(str(step)) or 0 for step in steps)

    if dry_run:
        print(
            f"--- Would run {len(steps)} steps{_create_expected_time_message(total_expected_time_seconds, prepend_message=' (at least ')}:"
        )
        return enumerate_steps(steps)
    elif workers == 1:
        print(
            f"--- Running {len(steps)} steps{_create_expected_time_message(total_expected_time_seconds, prepend_message=' (at least ')}:"
        )
        return exec_steps(steps, strict=strict)
    else:
        print(
            f"--- Running {len(steps)} steps with {workers} processes ({config.GRAPHER_INSERT_WORKERS} threads each):"
        )
        return exec_steps_parallel(steps, workers, dag=dag, strict=strict)


def exec_steps(steps: List[Step], strict: Optional[bool] = None) -> None:
    execution_times = {}
    for i, step in enumerate(steps, 1):
        print(f"--- {i}. {step}{_create_expected_time_message(_get_execution_time(step_name=str(step)))}")

        # Determine strictness level for the current step
        strict = _detect_strictness_level(step, strict)

        with strictness_level(strict):
            # Execute the step and measure the time taken
            try:
                time_taken = timed_run(lambda: step.run())
            except Exception:
                # log which step failed and re-raise the exception, otherwise it gets lost
                # in logs and we don't know which step failed
                log.error("step_failed", step=str(step))
                raise
            execution_times[str(step)] = time_taken

            click.echo(f"{click.style('OK', fg='blue')}{_create_expected_time_message(time_taken)}")
            print()

        # Write the recorded execution times to the file after all steps have been executed
        _write_execution_times(execution_times)


def _steps_sort_key(step: Step) -> int:
    """Sort steps by channel, so that grapher steps are executed first, then garden, then meadow, then snapshots."""
    str_step = str(step)
    if "grapher://" in str_step:
        return 0
    elif "garden://" in str_step:
        return 1
    elif "meadow://" in str_step:
        return 2
    elif "snapshot://" in str_step:
        return 3
    else:
        return 4


def exec_steps_parallel(steps: List[Step], workers: int, dag: DAG, strict: Optional[bool] = None) -> None:
    # put grapher steps in front of the queue to process them as soon as possible and lessen
    # the load on MySQL
    steps = sorted(steps, key=_steps_sort_key)

    # Use a Manager dict to collect execution times in parallel execution
    with Manager() as manager:
        execution_times = manager.dict()

        # Create execution graph from steps
        exec_graph = {}
        steps_str = {str(step) for step in steps}
        for step in steps:
            # only add dependencies that are in the list of steps (i.e. are dirty)
            # NOTE: we have to compare their string versions, the actual objects might have
            # different attributes
            exec_graph[str(step)] = {str(dep) for dep in step.dependencies if str(dep) in steps_str}

        # Prepare a function for execution that includes the necessary arguments
        exec_func = partial(_exec_step_job, execution_times=execution_times, dag=dag, strict=strict)

        # Execute the graph of tasks in parallel
        exec_graph_parallel(exec_graph, exec_func, workers)

        # After all tasks have completed, write the execution times to the file
        _write_execution_times(dict(execution_times))


def exec_graph_parallel(
    exec_graph: Dict[str, Any], func: Callable[[str], None], workers: int, use_threads=False, **kwargs
) -> None:
    """
    Execute a graph of tasks in parallel using multiple workers. TopologicalSorter orders nodes in the
    graph in a way that no node depends on a node that comes after it, i.e. all dependencies are
    guaranteed to be completed.
    :param exec_graph: A dictionary representing the execution graph of tasks.
    :param func: The function to be executed for each task.
    :param workers: The number of workers to use for parallel execution.
    :param use_threads: Flag indicating whether to use threads instead of processes for parallel execution.
    :param kwargs: Additional keyword arguments to be passed to the function.
    """
    topological_sorter = TopologicalSorter(exec_graph)
    topological_sorter.prepare()

    pool_factory = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
    with pool_factory(max_workers=workers) as executor:
        # Dictionary to keep track of future tasks
        future_to_task: Dict[Future, str] = {}

        ready_tasks = []

        while topological_sorter.is_active():
            # add new tasks
            ready_tasks += topological_sorter.get_ready()

            # Submit tasks that are ready to the executor
            # NOTE: limit it to `workers`, otherwise it might accept tasks that are not CPU bound
            # and overload our DB
            for task in ready_tasks[:workers]:
                future = executor.submit(func, task, **kwargs)
                future_to_task[future] = task

            # remove ready tasks
            ready_tasks = ready_tasks[workers:]

            # Wait for at least one future to complete
            done, _ = wait(future_to_task.keys(), return_when=FIRST_COMPLETED)

            # Mark completed tasks as done
            for future in done:
                task = future_to_task.pop(future)
                future.result()
                topological_sorter.done(task)


def _create_expected_time_message(
    expected_time: Optional[float], prepend_message: str = " (", append_message: str = ")"
) -> str:
    minutes, seconds = divmod(expected_time or 0, 60)
    if minutes < 1:
        partial_message = f"{seconds:.1f}s"
    else:
        partial_message = f"{int(minutes)}m{seconds: .1f}s"

    if (expected_time is None) or (expected_time == 0):
        return ""
    else:
        return prepend_message + partial_message + append_message


def _exec_step_job(
    step_name: str, execution_times: MutableMapping, dag: Optional[DAG] = None, strict: Optional[bool] = None
) -> None:
    """
    Executes a step.

    :param step_name: The name of the step to execute.
    :param dag: The original DAG used to create Step object. This must be the same DAG as given to ETL.
    :param strict: The strictness level for the step execution.
    """
    print(f"--- Starting {step_name}{_create_expected_time_message(_get_execution_time(step_name))}")
    assert dag
    step = parse_step(step_name, dag)
    strict = _detect_strictness_level(step, strict)
    with strictness_level(strict):
        try:
            execution_times[step_name] = timed_run(lambda: step.run())
        except Exception:
            log.error("step_failed", step=step_name)
            raise
    print(f"--- Finished {step_name} ({execution_times[step_name]:.1f}s)")


def _write_execution_times(execution_times: Dict) -> None:
    # Write the recorded execution times to a hidden json file that contains the time it took to execute each step
    execution_time_file = paths.EXECUTION_TIME_FILE
    if execution_time_file.exists():
        with open(execution_time_file, "r") as file:
            stored_times = json.load(file)
    else:
        stored_times = {}

    stored_times.update(execution_times)
    with open(execution_time_file, "w") as file:
        json.dump(stored_times, file, indent=4, sort_keys=True)


def _get_step_identifier(step_name: str) -> str:
    return step_name.replace(step_name.split("/")[-2] + "/", "")


def _get_execution_time(step_name: str) -> Optional[float]:
    # Read execution time of a given step from the hidden json file
    # If it doesn't exist, try to read another version of the same step, and if no other version exists, return None
    if not paths.EXECUTION_TIME_FILE.exists():
        return None
    else:
        with open(paths.EXECUTION_TIME_FILE, "r") as file:
            execution_times = json.load(file)
        execution_time = execution_times.get(step_name)
        if not execution_time:
            # If the step has not been timed yet, try to find a previous version
            step_identifiers = {_get_step_identifier(step): value for step, value in execution_times.items()}
            execution_time = step_identifiers.get(_get_step_identifier(step_name))
        return execution_time


def enumerate_steps(steps: List[Step]) -> None:
    for i, step in enumerate(steps, 1):
        print(f"{i}. {step}{_create_expected_time_message(_get_execution_time(str(step)))}")


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


def _validate_private_steps(steps: list[Step]) -> None:
    """Make sure there are no public steps that have private steps as dependency."""
    for step in steps:
        for dep in step.dependencies:
            if not dep.is_public:
                raise ValueError(f"Public step {step} depends on private step {dep}. Use --private flag.")


def _is_private_step(step_name: str) -> bool:
    return bool(re.findall(r".*?-private://", step_name))


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


def _set_dependencies_to_nondirty(step: Step) -> None:
    """Set all dependencies of a step to non-dirty."""
    if isinstance(step, DataStep):
        for step_dep in step.dependencies:
            step_dep.is_dirty = lambda: False
    if isinstance(step, GrapherStep):
        for step_dep in step.data_step.dependencies:
            step.data_step.is_dirty = lambda: False


def _check_public_private_steps(dag: DAG) -> None:
    """Make sure we don't mix public and private steps for the same dataset."""
    all_steps = set(dag.keys()) | set(itertools.chain.from_iterable(dag.values()))
    private_steps = {s for s in all_steps if _is_private_step(s)}
    public_steps = all_steps - private_steps

    # strip prefix
    private_steps = {s.split("://")[1] for s in private_steps if not s.startswith("grapher://")}
    public_steps = {s.split("://")[1] for s in public_steps if not s.startswith("grapher://")}

    common = private_steps & public_steps
    if common:
        raise ValueError(f"Dataset has both public and private version: {common}")


def _check_dag_completeness(dag: DAG) -> None:
    """Make sure the DAG is complete, i.e. all dependencies are there."""
    for step, deps in dag.items():
        for dep in deps:
            if re.match(r"^(snapshot|snapshot-private|github|etag)://", dep):
                pass
            elif dep not in dag:
                raise ValueError(f"Step {step} depends on {dep} which is not in the DAG.")


if __name__ == "__main__":
    main_cli()
