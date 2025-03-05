#
#  etl d run-python-step
#

import sys
from importlib import import_module
from typing import Optional

import rich_click as click
from ipdb import launch_ipdb_on_exception

from etl.paths import BASE_PACKAGE, STEP_DIR


@click.command(name="run-python-step")
@click.argument("uri")
@click.argument("dest_dir")
@click.option("--ipdb", is_flag=True)
def main(uri: str, dest_dir: str, ipdb: Optional[bool]) -> None:
    """Import and run a specific step of the ETL.

    Meant to be ran as a subprocess by the main `etl` command. There's a quite big overhead (~3s) from importing all packages again in the new subprocess.
    """
    step_type, path = uri.split("://", 1)

    allowed_step_types = ["data", "data-private", "export"]
    if step_type not in allowed_step_types:
        raise ValueError(f"Step type must be one of {allowed_step_types}, not {step_type}")

    step_type = step_type.replace("-private", "")

    if ipdb:
        with launch_ipdb_on_exception():
            _import_and_run(step_type, path, dest_dir)
    else:
        _import_and_run(step_type, path, dest_dir)


def _import_and_run(step_type: str, path: str, dest_dir: str) -> None:
    # ensure that the module search path includes the script
    step_path = STEP_DIR / step_type / path
    # path can be either in a module with __init__.py or a single .py file
    module_dir = step_path if step_path.is_dir() else step_path.parent
    sys.path.append(module_dir.as_posix())

    # import the module
    module_path = path.replace("/", ".")
    import_path = f"{BASE_PACKAGE}.steps.{step_type}.{module_path}"
    module = import_module(import_path)

    # check it matches the expected interface
    if not hasattr(module, "run"):
        raise Exception(f'no run() method defined for module "{module}"')

    # run the step itself
    try:
        # This should work when using the new run functions that don't require dest_dir as an argument.
        module.run()
    except TypeError as e:
        # For backwards compatibility, execute the run function assuming it has dest_dir as an argument.
        if "missing 1 required positional argument: 'dest_dir'" in str(e):
            module.run(dest_dir)
        else:
            raise


if __name__ == "__main__":
    main()
