#
#  run_python_step
#

import sys
from importlib import import_module
from typing import Optional

import click
from ipdb import launch_ipdb_on_exception

from etl import config
from etl.paths import BASE_PACKAGE, STEP_DIR


@click.command()
@click.argument("uri")
@click.argument("dest_dir")
@click.option("--ipdb", is_flag=True)
def main(uri: str, dest_dir: str, ipdb: Optional[bool]) -> None:
    """
    Import and run a specific step of the ETL. Meant to be ran as
    a subprocess by the main `etl` command.
    """
    if not uri.startswith("data://") and not uri.startswith("data-private://"):
        raise ValueError("Only data:// or data-private:// URIs are supported")

    path = uri.split("//", 1)[1]

    if ipdb:
        with launch_ipdb_on_exception():
            config.IPDB_ENABLED = True
            _import_and_run(path, dest_dir)
    else:
        _import_and_run(path, dest_dir)


def _import_and_run(path: str, dest_dir: str) -> None:
    # ensure that the module search path includes the script
    module_dir = (STEP_DIR / "data" / path).parent
    sys.path.append(module_dir.as_posix())

    # import the module
    module_path = path.replace("/", ".")
    import_path = f"{BASE_PACKAGE}.steps.data.{module_path}"
    module = import_module(import_path)

    # check it matches the expected interface
    if not hasattr(module, "run"):
        raise Exception(f'no run() method defined for module "{module}"')

    # run the step itself
    module.run(dest_dir)  # type: ignore


if __name__ == "__main__":
    main()
