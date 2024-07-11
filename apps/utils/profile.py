"""This script runs certain step of the ETL pipeline and profiles memory or CPU usage
of its `run` function line by line. You can additionally specify other functions from
the step module to profile.

Usage:
- Profile CPU usage of `run` function of the step:
    ```
    etl d profile --cpu garden/biodiversity/2024-01-25/cherry_blossom`
    ```
- Profile memory usage of `run` and `calculate_multiple_year_average` functions of the step:
    ```
    etl d profile --mem garden/biodiversity/2024-01-25/cherry_blossom -f calculate_multiple_year_average
    ```

To profile grapher upserts, it is better to use cProfile and run something like this:
```
ssh owid@staging-site-my-branch "cd etl && poetry run python -m cProfile -s cumtime etl/command.py grapher://grapher/biodiversity/2024-01-25/cherry_blossom --grapher --only --force --workers 1" | head -n 100
```
"""

import importlib.util
import sys
from pathlib import Path
from typing import Any

import click
from line_profiler import LineProfiler
from memory_profiler import memory_usage
from memory_profiler import profile as mem_profile
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl import paths

# Initialize logger.
log = get_logger()


@click.command(name="profile-cpu", cls=RichCommand, help=__doc__)
@click.argument(
    "step",
    type=str,
)
@click.option("--cpu", is_flag=True, help="Enable CPU profiling.")
@click.option("--mem", is_flag=True, help="Enable memory profiling.")
@click.option(
    "-f",
    "--functions",
    multiple=True,
    type=str,
    help="Specify additional functions to profile. These must be located in the step function.",
)
def cli(step: str, cpu: bool, mem: bool, functions: tuple[str]) -> None:
    if not cpu and not mem:
        log.error("Please specify either --cpu or --mem or both.")
        return

    # TODO: In theory, it should be possible to profile both CPU and memory
    # at the same time
    if cpu and mem:
        log.error("You can only specify either --cpu or --mem.")
        return

    module_path = Path("etl/steps/data/" + step + ".py")
    if not module_path.exists():
        raise FileNotFoundError(f"Module {module_path} not found.")

    module = _import_module(module_path)

    # Init CPU line profiler
    if cpu:
        lp = LineProfiler()
        lp.add_function(module.run)
        lp_wrapper = lp(module.run)
    else:
        lp_wrapper = module.run

    for f in functions:
        func = _nested_getattr(module, f)
        if cpu:
            lp.add_function(func)  # type: ignore
        if mem:
            _nested_setattr(module, f, mem_profile(func))

    dest_dir = paths.DATA_DIR / step

    # Profile the run function
    if mem:
        memory_usage((mem_profile(lp_wrapper), [dest_dir]))
    else:
        lp_wrapper(dest_dir)

    if cpu:
        lp.print_stats()  # type: ignore


def _nested_getattr(o, name):
    for n in name.split("."):
        o = getattr(o, n)
    return o


def _nested_setattr(o, name, value):
    names = name.split(".")
    for n in names[:-1]:
        o = getattr(o, n)
    setattr(o, names[-1], value)


def _import_module(module_path: Path) -> Any:
    """Import module from given path. This is useful if your module cannot be imported
    with standard `import ...` due to special characters in its path (like date)."""
    module_name = module_path.stem

    # Add module folder to sys.path (module is typically importing from shared.py)
    sys.path.append(str(module_path.parent))

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore

    return module
