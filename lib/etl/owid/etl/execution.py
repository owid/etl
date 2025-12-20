"""Step execution engine for ETL pipelines."""

import resource
import time
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from functools import partial
from graphlib import TopologicalSorter
from multiprocessing import Manager
from typing import Any, Callable, Dict, List, MutableMapping, Optional

import click
import structlog

from .config import ETLConfig, get_config
from .dag import filter_to_subgraph, load_dag
from .steps import Step, compile_steps

log = structlog.get_logger()

# Default limit for number of open files
LIMIT_NOFILE = 4096


def run_dag(
    steps: List[str],
    dag_file: Optional[str] = None,
    config: Optional[ETLConfig] = None,
    dry_run: bool = False,
    force: bool = False,
    only: bool = False,
    downstream: bool = False,
    excludes: Optional[List[str]] = None,
    workers: int = 1,
    dirty_step_workers: int = 1,
) -> None:
    """Run ETL steps from a DAG.

    Args:
        steps: List of step patterns to run.
        dag_file: Path to DAG file (uses config.dag_file if not specified).
        config: ETL configuration.
        dry_run: If True, print what would be run without executing.
        force: If True, run even if steps are not dirty.
        only: If True, only run specified steps (no dependencies).
        downstream: If True, also run steps that depend on specified steps.
        excludes: Patterns of steps to exclude.
        workers: Number of parallel workers for execution.
        dirty_step_workers: Number of workers for checking dirty steps.
    """
    if config is None:
        config = get_config()

    # Update open file limit
    update_open_file_limit()

    # Load DAG
    dag = load_dag(filename=dag_file, config=config)

    # Filter to subgraph
    subdag = filter_to_subgraph(
        graph=dag,
        includes=steps,
        downstream=downstream,
        only=only,
        excludes=excludes,
    )

    if not subdag:
        log.warning("No steps matched the given patterns")
        return

    # Compile steps
    compiled_steps = compile_steps(dag, subdag, config)

    # Run steps
    run_steps(
        compiled_steps,
        dry_run=dry_run,
        force=force,
        only=only,
        workers=workers,
        dirty_step_workers=dirty_step_workers,
    )


def run_steps(
    steps: List[Step],
    dry_run: bool = False,
    force: bool = False,
    only: bool = False,
    workers: int = 1,
    dirty_step_workers: int = 1,
) -> None:
    """Run a list of steps.

    Args:
        steps: Steps to execute (must be in dependency order).
        dry_run: If True, print what would be run without executing.
        force: If True, run even if steps are not dirty.
        only: If True, skip dependency checking.
        workers: Number of parallel workers for execution.
        dirty_step_workers: Number of workers for checking dirty steps.
    """
    # Set dependencies to non-dirty if --only is set
    if only:
        for step in steps:
            _set_dependencies_to_nondirty(step)

    # Filter to dirty steps unless forcing
    if not force:
        print("--- Detecting which steps need rebuilding...")
        start_time = time.time()
        steps = select_dirty_steps(steps, workers=dirty_step_workers)
        click.echo(f"{click.style('OK', fg='blue')} ({time.time() - start_time:.1f}s)")

    if not steps:
        print("--- All datasets up to date!")
        return

    if dry_run:
        print(f"--- Would run {len(steps)} steps:")
        enumerate_steps(steps)
        return

    if workers == 1:
        print(f"--- Running {len(steps)} steps:")
        exec_steps(steps)
    else:
        print(f"--- Running {len(steps)} steps with {workers} workers:")
        exec_steps_parallel(steps, workers)


def exec_steps(
    steps: List[Step],
    on_step_start: Optional[Callable[[Step, int], None]] = None,
    on_step_complete: Optional[Callable[[Step, int, float], None]] = None,
    on_step_error: Optional[Callable[[Step, int, Exception], None]] = None,
    continue_on_failure: bool = False,
) -> None:
    """Execute steps sequentially.

    Args:
        steps: Steps to execute.
        on_step_start: Callback when step starts (step, index).
        on_step_complete: Callback when step completes (step, index, time_taken).
        on_step_error: Callback when step fails (step, index, exception).
        continue_on_failure: If True, continue with other steps after failure.
    """
    failing_steps: List[Step] = []
    skipped_steps: List[Step] = []
    exceptions: List[Exception] = []

    for i, step in enumerate(steps, 1):
        # Skip if depends on failed step
        if continue_on_failure and _depends_on_failed(step, skipped_steps):
            print(f"--- {i}. {step} (skipped)")
            skipped_steps.append(step)
            continue

        print(f"--- {i}. {step}")

        if on_step_start:
            on_step_start(step, i)

        try:
            time_taken = timed_run(lambda: step.run())

            if on_step_complete:
                on_step_complete(step, i, time_taken)

            click.echo(f"{click.style('OK', fg='blue')} ({time_taken:.1f}s)")
            print()

        except Exception as e:
            log.error("step_failed", step=str(step))

            if on_step_error:
                on_step_error(step, i, e)

            if continue_on_failure:
                failing_steps.append(step)
                exceptions.append(e)
                skipped_steps.append(step)
                click.echo(click.style("FAILED", fg="red"))
                continue
            else:
                raise e

    if continue_on_failure and exceptions:
        for step, exception in zip(failing_steps, exceptions):
            log.error("step_exception", step=str(step), exception=str(exception))
        raise exceptions[0]


def exec_steps_parallel(
    steps: List[Step],
    workers: int,
    continue_on_failure: bool = False,
    use_threads: bool = False,
) -> None:
    """Execute steps in parallel respecting dependencies.

    Args:
        steps: Steps to execute.
        workers: Number of parallel workers.
        continue_on_failure: If True, continue with other steps after failure.
        use_threads: If True, use threads instead of processes.
    """
    with Manager() as manager:
        execution_times = manager.dict()

        # Create execution graph
        exec_graph: Dict[str, Any] = {}
        steps_str = {str(step) for step in steps}
        step_lookup = {str(step): step for step in steps}

        for step in steps:
            exec_graph[str(step)] = {str(dep) for dep in step.dependencies if str(dep) in steps_str}

        # Execute
        exec_func = partial(_exec_step_job, execution_times=execution_times, step_lookup=step_lookup)
        exec_graph_parallel(
            exec_graph, exec_func, workers, use_threads=use_threads, continue_on_failure=continue_on_failure
        )


def exec_graph_parallel(
    exec_graph: Dict[str, Any],
    func: Callable[[str], None],
    workers: int,
    use_threads: bool = False,
    continue_on_failure: bool = False,
) -> None:
    """Execute a graph of tasks in parallel using topological ordering.

    Args:
        exec_graph: Dictionary of task -> dependencies.
        func: Function to execute for each task.
        workers: Number of parallel workers.
        use_threads: If True, use threads instead of processes.
        continue_on_failure: If True, continue with other tasks after failure.
    """
    topological_sorter = TopologicalSorter(exec_graph)
    topological_sorter.prepare()

    pool_factory = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
    with pool_factory(max_workers=workers) as executor:
        future_to_task: Dict[Future, str] = {}
        failed_tasks = set()
        skipped_tasks = set()
        exceptions = []
        ready_tasks = []

        while topological_sorter.is_active():
            ready_tasks += topological_sorter.get_ready()

            # Submit tasks that are ready
            tasks_to_submit = []
            for task in ready_tasks[:workers]:
                if continue_on_failure:
                    task_deps = exec_graph.get(task, set())
                    if task_deps & (failed_tasks | skipped_tasks):
                        print(f"--- Skipping {task} (depends on failed task)")
                        skipped_tasks.add(task)
                        topological_sorter.done(task)
                        continue
                tasks_to_submit.append(task)

            for task in tasks_to_submit:
                future = executor.submit(func, task)
                future_to_task[future] = task

            ready_tasks = ready_tasks[workers:]

            # Wait for completion
            if future_to_task:
                done, _ = wait(future_to_task.keys(), return_when=FIRST_COMPLETED)

                for future in done:
                    task = future_to_task.pop(future)
                    try:
                        future.result()
                        topological_sorter.done(task)
                    except Exception as e:
                        if continue_on_failure:
                            failed_tasks.add(task)
                            skipped_tasks.add(task)
                            exceptions.append(e)
                            topological_sorter.done(task)
                            print(f"--- Failed {task} - {click.style('FAILED', fg='red')}")
                        else:
                            raise e

        if continue_on_failure and exceptions:
            for exception in exceptions:
                log.error("step_exception", exception=str(exception))
            raise exceptions[0]


def enumerate_steps(steps: List[Step]) -> None:
    """Print numbered list of steps."""
    for i, step in enumerate(steps, 1):
        print(f"{i}. {step}")


def timed_run(f: Callable[[], Any]) -> float:
    """Run a function and return execution time in seconds."""
    start_time = time.time()
    f()
    return time.time() - start_time


def update_open_file_limit(limit: int = LIMIT_NOFILE) -> None:
    """Increase the open file limit if needed."""
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit < limit:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(limit, hard_limit), hard_limit))


def _exec_step_job(
    step_name: str,
    execution_times: MutableMapping,
    step_lookup: Dict[str, Step],
) -> None:
    """Execute a single step (for parallel execution)."""
    print(f"--- Starting {step_name}")
    step = step_lookup[step_name]
    try:
        execution_times[step_name] = timed_run(lambda: step.run())
    except Exception:
        log.error("step_failed", step=step_name)
        raise
    print(f"--- Finished {step_name} ({execution_times[step_name]:.1f}s)")


def _depends_on_failed(step: Step, failed_steps: List[Step]) -> bool:
    """Check if step depends on any failed step."""
    failed_paths = {s.path for s in failed_steps}
    return any(dep.path in failed_paths for dep in step.dependencies)


def _set_dependencies_to_nondirty(step: Step) -> None:
    """Set all dependencies of a step to non-dirty."""
    for dep in getattr(step, "dependencies", []):
        dep.is_dirty = lambda: False


