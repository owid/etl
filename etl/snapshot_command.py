"""Snapshot command for ETL CLI."""

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Optional

import rich_click as click

from etl.snapshot import Snapshot


@click.command("snapshot")
@click.argument("dataset_name", type=str)
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", type=str, help="Path to local data file (for manual upload scenarios)")
def snapshot_cli(dataset_name: str, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Create snapshot from a snapshot script or .dvc file.

    Run snapshot scripts in a standardized way. Supports three scenarios:
    1. Scripts with main() function - runs module directly
    2. Scripts with run() function - wraps in CLI with upload/path-to-file args
    3. Scripts with only .dvc file - runs snap.create_snapshot()

    Examples:

        etl snapshot tourism/2024-08-17/unwto_gdp
        etls tourism/2024-08-17/unwto_gdp
        etls 2024-08-17/unwto_gdp

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
        dataset_name: Can be:
                     - Full path: "tourism/2024-08-17/unwto_gdp"
                     - Partial path: "2024-08-17/unwto_gdp" 
                     - Just filename: "unwto_gdp"

    Returns:
        Path to the .py script file, or None if not found
    """
    from etl import paths

    # Count the number of path separators to determine the type of path
    path_parts = dataset_name.split("/")

    if len(path_parts) == 3:
        # Full path: namespace/version/filename
        script_path = paths.SNAPSHOTS_DIR / f"{dataset_name}.py"
        return script_path if script_path.exists() else None

    elif len(path_parts) == 2:
        # Partial path: version/filename - search for matching namespace
        version, filename = path_parts
        pattern = f"*/{version}/{filename}.py"
        script_files = list(paths.SNAPSHOTS_DIR.glob(pattern))

    elif len(path_parts) == 1:
        # Just filename - search in all directories
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
        click.echo(f"Multiple scripts found matching '{dataset_name}':")
        for path in sorted(script_files):
            relative_path = path.relative_to(paths.SNAPSHOTS_DIR)
            click.echo(f"  {relative_path.with_suffix('')}")  # Remove .py extension for display
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
    if hasattr(module, "main"):
        # Script has main() function
        main_func = getattr(module, "main")
        
        # Check if it's a click command
        if isinstance(main_func, click.Command):
            # It's a click command, call it with command line args
            click.echo(f"Running {script_path} main() click command...")
            args = []
            if not upload:
                args.append("--skip-upload")
            if path_to_file is not None:
                args.extend(["--path-to-file", path_to_file])
            
            try:
                # Call the click command with parsed arguments
                main_func(args, standalone_mode=False)
            except Exception as e:
                raise click.ClickException(f"Error calling main() click command: {e}")
        else:
            # Regular function - check its signature
            sig = inspect.signature(main_func)

            # Build arguments based on function signature
            kwargs = {}
            if "upload" in sig.parameters:
                kwargs["upload"] = upload
            if "path_to_file" in sig.parameters and path_to_file is not None:
                kwargs["path_to_file"] = path_to_file

            # Call main with appropriate arguments
            click.echo(f"Running {script_path} main() function...")
            try:
                main_func(**kwargs)
            except TypeError as e:
                if path_to_file is not None and "path_to_file" not in sig.parameters:
                    raise click.ClickException(
                        f"Script {script_path} main() function doesn't accept --path-to-file argument"
                    )
                raise click.ClickException(f"Error calling main(): {e}")

    elif hasattr(module, "run"):
        # Script has run() function
        run_func = getattr(module, "run")
        sig = inspect.signature(run_func)

        # Build arguments based on function signature
        kwargs = {}
        if "upload" in sig.parameters:
            kwargs["upload"] = upload
        if "path_to_file" in sig.parameters and path_to_file is not None:
            kwargs["path_to_file"] = path_to_file

        click.echo(f"Running {script_path} run() function...")
        try:
            run_func(**kwargs)
        except TypeError as e:
            if path_to_file is not None and "path_to_file" not in sig.parameters:
                raise click.ClickException(
                    f"Script {script_path} run() function doesn't accept --path-to-file argument"
                )
            raise click.ClickException(f"Error calling run(): {e}")
    else:
        raise click.ClickException(f"Script {script_path} must have either a main() or run() function")


def run_snapshot_dvc_only(dataset_name: str, upload: bool, path_to_file: Optional[str] = None) -> None:
    """Run snapshot creation when only .dvc file exists."""
    # Create snapshot object - this will find the .dvc file
    try:
        snap = Snapshot(dataset_name)
    except FileNotFoundError:
        raise click.ClickException(f"No .dvc file found for dataset: {dataset_name}")

    click.echo(f"Creating snapshot from .dvc file: {snap.metadata_path}")

    # Call create_snapshot with appropriate arguments
    kwargs = {"upload": upload}
    if path_to_file is not None:
        kwargs["filename"] = path_to_file

    try:
        snap.create_snapshot(**kwargs)
        click.echo(f"✅ Snapshot created successfully: {snap.uri}")
    except Exception as e:
        raise click.ClickException(f"Error creating snapshot: {e}")


if __name__ == "__main__":
    snapshot_cli()
