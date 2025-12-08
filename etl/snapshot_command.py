"""Snapshot command for ETL CLI."""

import importlib.util
import inspect
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import rich_click as click
import structlog
from click.core import Command

from etl import paths
from etl.snapshot import Snapshot

log = structlog.get_logger()


def _normalize_dataset_name(dataset_name: str) -> str:
    """Normalize dataset name by removing prefixes and extensions.

    Args:
        dataset_name: Dataset path in various formats

    Returns:
        Normalized dataset name without prefixes or extensions
    """
    # Remove snapshot:// prefix if supplied by user
    dataset_name = dataset_name.removeprefix("snapshot://")

    # Handle full file path with snapshots/ prefix
    if dataset_name.startswith("snapshots/"):
        dataset_name = dataset_name.removeprefix("snapshots/")

    # Remove any file extension if present (e.g., .py, .csv, .xlsx)
    if "." in dataset_name:
        # Find the last dot and check if it's likely a file extension
        dot_index = dataset_name.rfind(".")
        if dot_index > dataset_name.rfind("/"):  # Extension is after the last slash
            dataset_name = dataset_name[:dot_index]

    return dataset_name


def check_for_version_ambiguity(dataset_name: str) -> None:
    """Check if there are multiple versions of the snapshot and fail if so.

    This prevents accidentally overwriting an existing snapshot when multiple
    versions exist for the same short_name.

    Args:
        dataset_name: Dataset path in one of several formats

    Raises:
        click.ClickException: If multiple versions are found
    """
    dataset_name = _normalize_dataset_name(dataset_name)

    # Count the number of path separators to determine the type of path
    path_parts = dataset_name.split("/")

    # Only check for ambiguity when the path is partial (not a full path)
    # Full paths (3 parts: namespace/version/short_name) are unambiguous
    if len(path_parts) == 3:
        return

    # For partial paths, search for all matching .dvc files across different versions
    if len(path_parts) == 2:
        # Partial path: version/short_name
        # Check if there are multiple namespaces with this version/short_name
        version, filename = path_parts
        pattern = f"*/{version}/{filename}.*.dvc"
        dvc_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

    elif len(path_parts) == 1:
        # Just short_name - search for all versions
        filename = path_parts[0]
        pattern = f"**/{filename}.*.dvc"
        dvc_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

    else:
        # Invalid or unknown format, let other functions handle it
        return

    # Group DVC files by (namespace, short_name) to find different versions
    # We want to detect if the same snapshot exists in multiple versions

    # Extract version from each path
    versions_by_snapshot: defaultdict[str, list[str]] = defaultdict(list)
    for dvc_file in dvc_files:
        relative_path = dvc_file.relative_to(paths.SNAPSHOTS_DIR)
        parts = relative_path.parts
        if len(parts) >= 3:
            namespace = parts[0]
            version = parts[1]
            # Get the filename without .dvc extension
            filename_with_ext = relative_path.stem  # e.g., "battery_cell_prices.xlsx"
            snapshot_key = f"{namespace}/{filename_with_ext}"
            versions_by_snapshot[snapshot_key].append(version)

    # Check if any snapshot has multiple versions
    for snapshot_key, versions in versions_by_snapshot.items():
        if len(versions) > 1:
            # Multiple versions found - this is ambiguous
            log.error(f"Multiple versions found for '{snapshot_key}':")
            for version in sorted(versions):
                namespace = snapshot_key.split("/")[0]
                filename_part = "/".join(snapshot_key.split("/")[1:])
                log.error(f"  {namespace}/{version}/{filename_part}")

            raise click.ClickException(
                f"Multiple snapshot versions found for '{dataset_name}'. "
                "Please specify the full path including the version (e.g., namespace/version/short_name) "
                "to disambiguate which version you want to use."
            )


@click.command("snapshot")
@click.argument("dataset_name", type=str, metavar="DATASET_PATH")
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", type=str, help="Path to local data file (for manual upload scenarios)")
@click.option("--dry-run", is_flag=True, help="Preview what would happen without creating/uploading the snapshot")
def snapshot_cli(dataset_name: str, upload: bool, path_to_file: Optional[str], dry_run: bool) -> None:
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

        etl snapshot dataset_name --dry-run
        etls dataset_name --dry-run
    """
    # Check for version ambiguity before proceeding
    check_for_version_ambiguity(dataset_name)

    # Find the snapshot script
    script_path = find_snapshot_script(dataset_name)

    if dry_run:
        # Dry run mode - just show what would happen
        if script_path and script_path.exists():
            log.info("DRY RUN: Would execute snapshot script", script_path=script_path.relative_to(paths.SNAPSHOTS_DIR))
        else:
            # Find the .dvc file to show what would be created
            dataset_name_normalized = _normalize_dataset_name(dataset_name)
            dvc_files = _find_files_by_pattern(dataset_name_normalized, ".*.dvc")

            if len(dvc_files) == 0:
                raise click.ClickException(f"No .dvc file found for '{dataset_name}'")
            elif len(dvc_files) > 1:
                log.info(f"DRY RUN: Multiple .dvc files found matching '{dataset_name}':")
                for path in sorted(dvc_files):
                    relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
                    log.info(f"  {relative_path.with_suffix('')}")
                raise click.ClickException("Please specify a more specific path to disambiguate.")

            dvc_file = dvc_files[0]
            log.info("DRY RUN: Would create snapshot from .dvc file", dvc_file=dvc_file.relative_to(paths.SNAPSHOTS_DIR))

        log.info("DRY RUN: Upload enabled" if upload else "DRY RUN: Upload disabled (--skip-upload)")
        if path_to_file:
            log.info("DRY RUN: Would use local file", path=path_to_file)
        return

    if script_path and script_path.exists():
        # Run the Python script
        run_snapshot_script(script_path, upload=upload, path_to_file=path_to_file)
    else:
        # Only .dvc file exists, run snap.create_snapshot directly
        run_snapshot_dvc_only(dataset_name, upload=upload, path_to_file=path_to_file)


def _find_files_by_pattern(dataset_name: str, extension: str) -> list[Path]:
    """Find files matching a dataset name pattern with given extension.

    Args:
        dataset_name: Normalized dataset path (no prefixes/extensions)
        extension: File extension to search for (e.g., '.py', '.*.dvc')

    Returns:
        List of matching file paths
    """
    path_parts = dataset_name.split("/")

    if len(path_parts) == 3:
        # Full path: namespace/version/short_name
        file_path = paths.SNAPSHOTS_DIR / f"{dataset_name}{extension}"
        return [file_path] if file_path.exists() else []

    elif len(path_parts) == 2:
        # Partial path: version/short_name
        version, filename = path_parts
        pattern = f"*/{version}/{filename}{extension}"
        return list(paths.SNAPSHOTS_DIR.glob(pattern))

    elif len(path_parts) == 1:
        # Short name only
        filename = path_parts[0]
        pattern = f"**/{filename}{extension}"
        return list(paths.SNAPSHOTS_DIR.glob(pattern))

    else:
        raise click.ClickException(f"Invalid dataset path format: {dataset_name}")


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
        return script_path if script_path.exists() else None

    dataset_name = _normalize_dataset_name(dataset_name)
    script_files = _find_files_by_pattern(dataset_name, ".py")

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
    except Exception:
        log.error(f"Error loading script {script_path}")
        raise
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


def _call_snapshot_function(
    func, func_name: str, script_path: Path, upload: bool, path_to_file: Optional[str] = None
) -> None:
    """Call a snapshot function, handling both click commands and regular functions."""
    # Check if it's a click command
    if isinstance(func, (click.Command, Command)):
        # It's a click command, call it with command line args
        args = []
        if not upload:
            args.append("--skip-upload")
        if path_to_file is not None:
            args.extend(["--path-to-file", path_to_file])

        # Call the click command with parsed arguments
        func(args, standalone_mode=False)
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
        except TypeError:
            if path_to_file is not None and "path_to_file" not in sig.parameters:
                raise click.ClickException(
                    f"Script {script_path} {func_name}() function doesn't accept --path-to-file argument"
                )
            log.error(f"Error calling {func_name}()")
            raise


def run_snapshot_dvc_only(dataset_name: str, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Run snapshot creation when only .dvc file exists."""
    dataset_name = _normalize_dataset_name(dataset_name)
    dvc_files = _find_files_by_pattern(dataset_name, ".*.dvc")

    if len(dvc_files) == 0:
        raise click.ClickException(f"No .dvc file found for '{dataset_name}'")
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

    # Create snapshot object
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
