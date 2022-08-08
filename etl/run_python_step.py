#
#  run_python_step
#

from importlib import import_module
import click
import sys

from etl.paths import STEP_DIR, BASE_PACKAGE


@click.command()
@click.argument("uri")
@click.argument("dest_dir")
def main(uri: str, dest_dir: str) -> None:
    """
    Import and run a specific step of the ETL. Meant to be ran as
    a subprocess by the main `etl` command.
    """
    if not uri.startswith("data://") and not uri.startswith("data-private://"):
        raise ValueError("Only data:// or data-private:// URIs are supported")

    path = uri.split("//", 1)[1]

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
