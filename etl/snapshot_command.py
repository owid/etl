"""Snapshot command for ETL CLI."""

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Optional

import rich_click as click
import structlog
from click.core import Command

from etl import paths
from etl.snapshot import Snapshot

log = structlog.get_logger()


@click.command("snapshot")
@click.argument("dataset_name", type=str, metavar="DATASET_PATH")
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", type=str, help="Path to local data file (for manual upload scenarios)")
def snapshot_cli(dataset_name: str, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Create snapshot from a snapshot script or .dvc file.

    DATASET_PATH can be provided in several formats:
    - Full path: namespace/version/short_name (e.g., tourism/2024-08-17/unwto_gdp)
    - Partial path: version/short_name (e.g., 2024-08-17/unwto_gdp)
    - Short name only: short_name (e.g., unwto_gdp)
    - Full file path: snapshots/namespace/version/short_name.py

    The command will automatically find the corresponding .py script or .dvc file
    in the snapshots directory. If multiple matches are found, you'll need to
    provide a more specific path.

    Run snapshot scripts in a standardized way. Supports three scenarios:
    1. Scripts with main() function - runs module directly
    2. Scripts with run() function - wraps in CLI with upload/path-to-file args
    3. Scripts with only .dvc file - runs snap.create_snapshot()

    Examples:

        etl snapshot tourism/2024-08-17/unwto_gdp
        etls tourism/2024-08-17/unwto_gdp
        etls 2024-08-17/unwto_gdp
        etls snapshots/tourism/2024-08-17/unwto_gdp.py

        etl snapshot abs/2024-08-06/employee_earnings_and_hours_australia_2008 --path-to-file data.csv
        etls abs/2024-08-06/employee_earnings_and_hours_australia_2008 --path-to-file data.csv
        etls 2024-08-06/employee_earnings_and_hours_australia_2008 --path-to-file data.csv

        etl snapshot dataset_name --skip-upload
        etls dataset_name --skip-upload
    """
    # Find the snapshot script
    script_path = find_snapshot_script(dataset_name)

    if script_path and script_path.exists():
        # Run the Python script
        run_snapshot_script(script_path, upload=upload, path_to_file=path_to_file)
    else:
        # Only .dvc file exists, run snap.create_snapshot directly
        run_snapshot_dvc_only(dataset_name, upload=upload, path_to_file=path_to_file)


def find_snapshot_script(dataset_name: str) -> Optional[Path]:
    """Find the snapshot script for the given dataset name.

    Args:
        dataset_name: Dataset path in one of several formats:
                     - Full file path: "snapshots/tourism/2024-08-17/unwto_gdp.py"
                     - Full path: "tourism/2024-08-17/unwto_gdp" (namespace/version/short_name)
                     - Partial path: "2024-08-17/unwto_gdp" (version/short_name)
                     - Short name only: "unwto_gdp" (short_name)

    Returns:
        Path to the .py script file, or None if not found
    """
    # Handle full file path with snapshots/ prefix and .py extension
    if dataset_name.startswith("snapshots/") and dataset_name.endswith(".py"):
        script_path = Path(dataset_name)
        if script_path.exists():
            return script_path
        else:
            return None

    # Remove any file extension if present (e.g., .py, .csv, .xlsx)
    if "." in dataset_name and not dataset_name.startswith("snapshots/"):
        # Only strip extension if it's not a full path starting with snapshots/
        # Find the last dot and check if it's likely a file extension
        dot_index = dataset_name.rfind(".")
        if dot_index > dataset_name.rfind("/"):  # Extension is after the last slash
            dataset_name = dataset_name[:dot_index]

    # Count the number of path separators to determine the type of path
    path_parts = dataset_name.split("/")

    if len(path_parts) == 3:
        # Full path: namespace/version/short_name
        script_path = paths.SNAPSHOTS_DIR / f"{dataset_name}.py"
        return script_path if script_path.exists() else None

    elif len(path_parts) == 2:
        # Partial path: version/short_name - search for matching namespace
        version, filename = path_parts
        pattern = f"*/{version}/{filename}.py"
        script_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

    elif len(path_parts) == 1:
        # Short name only - search in all directories
        filename = path_parts[0]
        pattern = f"**/{filename}.py"
        script_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

    else:
        # Invalid path format
        raise click.ClickException(f"Invalid dataset path format: {dataset_name}")

    # Handle multiple or no matches for partial paths
    if len(script_files) == 0:
        return None
    elif len(script_files) == 1:
        return script_files[0]
    else:
        # Multiple matches found
        log.info(f"Multiple scripts found matching '{dataset_name}':")
        for path in sorted(script_files):
            relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
            log.info(f"  {relative_path.with_suffix('')}")  # Remove .py extension for display
        raise click.ClickException("Please specify a more specific path to disambiguate.")


def run_snapshot_script(script_path: Path, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Run a snapshot script based on its structure."""
    # Load the module
    spec = importlib.util.spec_from_file_location("snapshot_module", script_path)
    if spec is None or spec.loader is None:
        raise click.ClickException(f"Could not load script: {script_path}")

    module = importlib.util.module_from_spec(spec)

    # Add the script's directory to sys.path so it can import relative modules
    script_dir = str(script_path.parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise click.ClickException(f"Error loading script {script_path}: {e}")
    finally:
        # Clean up sys.path
        if script_dir in sys.path:
            sys.path.remove(script_dir)

    # Check what functions are available
    func = None
    func_name = None
    
    if hasattr(module, "main"):
        func = getattr(module, "main")
        func_name = "main"
    elif hasattr(module, "run"):
        func = getattr(module, "run")
        func_name = "run"
    else:
        raise click.ClickException(f"Script {script_path} must have either a main() or run() function")

    # Call the function (either main or run)
    _call_snapshot_function(func, func_name, script_path, upload, path_to_file)


def _call_snapshot_function(func, func_name: str, script_path: Path, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Call a snapshot function, handling both click commands and regular functions."""
    # Check if it's a click command
    if isinstance(func, (click.Command, Command)):
        # It's a click command, call it with command line args
        args = []
        if not upload:
            args.append("--skip-upload")
        if path_to_file is not None:
            args.extend(["--path-to-file", path_to_file])

        try:
            # Call the click command with parsed arguments
            func(args, standalone_mode=False)
        except Exception as e:
            raise click.ClickException(f"Error calling {func_name}() click command: {e}")
    else:
        # Regular function - check its signature
        sig = inspect.signature(func)

        # Build arguments based on function signature
        kwargs = {}
        if "upload" in sig.parameters:
            kwargs["upload"] = upload
        if "path_to_file" in sig.parameters and path_to_file is not None:
            kwargs["path_to_file"] = path_to_file

        try:
            func(**kwargs)
        except TypeError as e:
            if path_to_file is not None and "path_to_file" not in sig.parameters:
                raise click.ClickException(
                    f"Script {script_path} {func_name}() function doesn't accept --path-to-file argument"
                )
            raise click.ClickException(f"Error calling {func_name}(): {e}")


def run_snapshot_dvc_only(dataset_name: str, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Run snapshot creation when only .dvc file exists."""
    # Handle full file path with snapshots/ prefix
    if dataset_name.startswith("snapshots/"):
        # Remove snapshots/ prefix
        dataset_name = dataset_name[10:]  # len("snapshots/") = 10

    # Remove any file extension if present (from failed script search)
    if "." in dataset_name:
        # Find the last dot and check if it's likely a file extension
        dot_index = dataset_name.rfind(".")
        if dot_index > dataset_name.rfind("/"):  # Extension is after the last slash
            dataset_name = dataset_name[:dot_index]

    # Convert partial path to full URI by finding the .dvc file
    path_parts = dataset_name.split("/")

    if len(path_parts) == 3:
        # Full path: namespace/version/filename - but need to find the actual .dvc file
        namespace, version, filename = path_parts
        pattern = f"{namespace}/{version}/{filename}.*.dvc"
        dvc_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

        if len(dvc_files) == 0:
            raise click.ClickException(f"No .dvc file found matching pattern '{filename}.*' in {namespace}/{version}")
        elif len(dvc_files) > 1:
            log.info(f"Multiple .dvc files found matching '{dataset_name}':")
            for path in sorted(dvc_files):
                relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
                log.info(f"  {relative_path.with_suffix('')}")  # Remove .dvc extension
            raise click.ClickException("Please specify the full filename with extension to disambiguate.")

        # Convert to snapshot URI
        dvc_file = dvc_files[0]
        relative_path = dvc_file.relative_to(paths.SNAPSHOTS_DIR)
        snapshot_uri = str(relative_path.with_suffix(""))  # Remove .dvc extension
    elif len(path_parts) == 2:
        # Partial path: version/filename - search for matching .dvc file
        version, filename = path_parts
        pattern = f"*/{version}/{filename}.*.dvc"
        dvc_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

        if len(dvc_files) == 0:
            raise click.ClickException(f"No .dvc file found matching pattern '{filename}.*' in version '{version}'")
        elif len(dvc_files) > 1:
            log.info(f"Multiple .dvc files found matching '{dataset_name}':")
            for path in sorted(dvc_files):
                relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
                log.info(f"  {relative_path.with_suffix('')}")  # Remove .dvc extension
            raise click.ClickException("Please specify a more specific path to disambiguate.")

        # Convert to snapshot URI
        dvc_file = dvc_files[0]
        relative_path = dvc_file.relative_to(paths.SNAPSHOTS_DIR)
        snapshot_uri = str(relative_path.with_suffix(""))  # Remove .dvc extension

    elif len(path_parts) == 1:
        # Just filename - search in all directories
        filename = path_parts[0]
        pattern = f"**/{filename}.*.dvc"
        dvc_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

        if len(dvc_files) == 0:
            raise click.ClickException(f"No .dvc file found matching pattern '{filename}.*'")
        elif len(dvc_files) > 1:
            log.info(f"Multiple .dvc files found matching '{dataset_name}':")
            for path in sorted(dvc_files):
                relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
                log.info(f"  {relative_path.with_suffix('')}")  # Remove .dvc extension
            raise click.ClickException("Please specify a more specific path to disambiguate.")

        # Convert to snapshot URI
        dvc_file = dvc_files[0]
        relative_path = dvc_file.relative_to(paths.SNAPSHOTS_DIR)
        snapshot_uri = str(relative_path.with_suffix(""))  # Remove .dvc extension
    else:
        raise click.ClickException(f"Invalid dataset path format: {dataset_name}")

    # Create snapshot object - this will find the .dvc file
    try:
        snap = Snapshot(snapshot_uri)
    except FileNotFoundError:
        raise click.ClickException(f"No .dvc file found for dataset: {snapshot_uri}")

    log.info("Creating snapshot from .dvc file", metadata_path=snap.metadata_path.relative_to(paths.SNAPSHOTS_DIR))

    # Call create_snapshot with appropriate arguments
    try:
        snap.create_snapshot(filename=path_to_file, upload=upload)
        log.info("Snapshot created successfully", uri=snap.uri)
    except Exception as e:
        raise click.ClickException(f"Error creating snapshot: {e}")


if __name__ == "__main__":
    snapshot_cli()
