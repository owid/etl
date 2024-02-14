#
#  run_python_step
#

import sys
from importlib import import_module
from typing import Optional

import rich_click as click
from ipdb import launch_ipdb_on_exception

from etl.paths import BASE_PACKAGE, STEP_DIR


@click.command()
@click.argument("uri")
@click.argument("dest_dir")
@click.option("--ipdb", is_flag=True)
def main(uri: str, dest_dir: str, ipdb: Optional[bool]) -> None:
    """
    Import and run a specific step of the ETL. Meant to be ran as
    a subprocess by the main `etl` command. There's a quite big
    overhead (~3s) from importing all packages again in the new subprocess.
    """
    if not uri.startswith("data://") and not uri.startswith("data-private://"):
        raise ValueError("Only data:// or data-private:// URIs are supported")

    path = uri.split("//", 1)[1]

    if ipdb:
        with launch_ipdb_on_exception():
            _import_and_run(path, dest_dir)
    else:
        _import_and_run(path, dest_dir)


def _import_and_run(path: str, dest_dir: str) -> None:
    # ensure that the module search path includes the script
    step_path = STEP_DIR / "data" / path
    # path can be either in a module with __init__.py or a single .py file
    module_dir = step_path if step_path.is_dir() else step_path.parent
    sys.path.append(module_dir.as_posix())

    # import the module
    module_path = path.replace("/", ".")
    import_path = f"{BASE_PACKAGE}.steps.data.{module_path}"
    module = import_module(import_path)

    # check it matches the expected interface
    if not hasattr(module, "run"):
        raise Exception(f'no run() method defined for module "{module}"')

    # run the step itself
    module.run(dest_dir)


if __name__ == "__main__":
    main()
