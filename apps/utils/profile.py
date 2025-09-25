"""This script runs certain step of the ETL pipeline and profiles memory or CPU usage
of its `run` function line by line. You can additionally specify other functions to profile.

Usage:
- Profile CPU usage of `run` function of the step:
    ```
    etl d profile --cpu garden/biodiversity/2024-01-25/cherry_blossom
    ```
- Profile specific functions (excludes `run` for cleaner output):
    ```
    etl d profile --cpu garden/biodiversity/2024-01-25/cherry_blossom -f calculate_multiple_year_average
    etl d profile --cpu garden/biodiversity/2024-01-25/cherry_blossom -f etl.helpers.PathFinder.load_dataset
    etl d profile --cpu garden/biodiversity/2024-01-25/cherry_blossom -f etl.data_helpers.geo.RegionAggregator.__init__
    ```

To profile grapher upserts, it is better to use cProfile and run something like this:
```
ssh owid@staging-site-my-branch "cd etl && uv run python -m cProfile -s cumtime etl/command.py grapher://grapher/biodiversity/2024-01-25/cherry_blossom --grapher --only --force --workers 1" | head -n 100
```
"""

import importlib
import importlib.util
import inspect
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
    help="Specify functions to profile (step functions or full paths like etl.helpers.PathFinder.load_dataset). Excludes 'run' for cleaner output.",
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

    # Collect additional functions first
    additional_funcs = []
    for f in functions:
        try:
            # Try to get function directly from module first
            func = _nested_getattr(module, f)
        except AttributeError:
            # Handle cases with dot notation by importing the full path
            if "." in f:
                func = _import_from_path(f)
            else:
                raise ValueError(f"Cannot find function {f} in module")

        additional_funcs.append((f, func))

        if mem:
            # For memory profiling of imported functions, we need to handle this differently
            if "." in f and func != _nested_getattr(module, f, None):
                log.warning(f"Memory profiling of {f} not supported yet - use CPU profiling instead")
            else:
                _nested_setattr(module, f, mem_profile(func))

    # Init CPU line profiler
    if cpu:
        lp = LineProfiler()
        if additional_funcs:
            # If -f functions are specified, only profile those (exclude run function)
            for _, func in additional_funcs:
                lp.add_function(func)  # type: ignore
            # Enable profiler but don't profile run itself
            lp.enable()
            lp_wrapper = module.run
        else:
            # If no -f functions specified, profile the run function
            lp.add_function(module.run)
            lp_wrapper = lp(module.run)
    else:
        lp_wrapper = module.run

    dest_dir = paths.DATA_DIR / step

    # Profile the run function
    if mem:
        # Check if run function takes arguments
        sig = inspect.signature(module.run)
        if len(sig.parameters) > 0:
            memory_usage((mem_profile(lp_wrapper), [dest_dir]))  # type: ignore[reportArgumentType]
        else:
            memory_usage((mem_profile(lp_wrapper), []))  # type: ignore[reportArgumentType]
    else:
        # Check if run function takes arguments
        sig = inspect.signature(module.run)
        if len(sig.parameters) > 0:
            lp_wrapper(dest_dir)
        else:
            lp_wrapper()

    if cpu:
        if additional_funcs:
            lp.disable()  # Disable after execution  # type: ignore
        lp.print_stats()  # type: ignore


def _import_from_path(path: str):
    """Import a function or class from a full module path like 'etl.helpers.PathFinder.load_dataset'."""
    parts = path.split(".")

    # Try different splits to find the module vs attribute boundary
    for i in range(1, len(parts)):
        module_path = ".".join(parts[:i])
        attr_path = parts[i:]

        try:
            module = importlib.import_module(module_path)
            # Navigate through the attributes
            obj = module
            for attr in attr_path:
                obj = getattr(obj, attr)
            return obj
        except (ImportError, AttributeError):
            continue

    raise ValueError(f"Cannot import {path}")


def _nested_getattr(o, name, default=None):
    """Get nested attribute, with optional default value."""
    if default is None:
        # Original behavior - raise AttributeError if not found
        for n in name.split("."):
            o = getattr(o, n)
        return o
    else:
        # New behavior with default
        try:
            for n in name.split("."):
                o = getattr(o, n)
            return o
        except AttributeError:
            return default


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
