import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from cookiecutter.main import cookiecutter

from etl.dag_helpers import write_to_dag_file
from etl.files import apply_ruff_formatter_to_files, ruamel_dump
from etl.paths import DAG_DIR, STEP_DIR
from etl.steps import DAG

# TODO: Move this to apps/wizard
DAG_WIZARD_PATH = DAG_DIR / "wizard.yml"


def add_to_dag(dag: DAG, dag_path: Path = DAG_WIZARD_PATH) -> str:
    """Add steps to ``dag_path``, returning the added subdag as a YAML string.

    Delegates to :func:`etl.dag_helpers.write_to_dag_file` so comment
    preservation and formatting are consistent across every DAG-writing code
    path in the repo.
    """
    write_to_dag_file(dag_path, dag)
    return ruamel_dump({"steps": dag})


def generate_step(cookiecutter_path: Path, data: dict[str, Any], target_dir: Path) -> None:
    # data["test"] = ["this", "is", "a", "test"]
    print("--- Data Dictionary ---")
    print(data)
    with tempfile.TemporaryDirectory() as temp_dir:
        # create config file with data for cookiecutter
        config_path = cookiecutter_path / "cookiecutter.json"
        with open(config_path, "w") as f:
            json.dump(data, f, default=str)

        # Verify the contents of the JSON file
        with open(config_path) as f:
            config_data = json.load(f)
            print("--- JSON Config Data ---")
            print(config_data)

        try:
            cookiecutter(
                cookiecutter_path.as_posix(),
                no_input=True,
                output_dir=temp_dir,
                overwrite_if_exists=True,
                extra_context={"test": ["this", "is", "a", "test"]},
            )
        finally:
            config_path.unlink()

        # Apply black formatter to generated files.
        apply_ruff_formatter_to_files(file_paths=list(Path(temp_dir).glob("**/*.py")))

        shutil.copytree(
            Path(temp_dir),
            target_dir,
            dirs_exist_ok=True,
        )


def generate_step_to_channel(cookiecutter_path: Path, data: dict[str, Any]) -> Path:
    assert {"channel", "namespace", "version"} <= data.keys()

    target_dir = STEP_DIR / "data" / data["channel"]
    generate_step(cookiecutter_path, data, target_dir)
    return target_dir / data["namespace"] / data["version"]
