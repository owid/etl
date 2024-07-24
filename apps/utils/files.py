import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

from cookiecutter.main import cookiecutter

from etl.files import apply_ruff_formatter_to_files, ruamel_dump, ruamel_load
from etl.paths import DAG_DIR, STEP_DIR
from etl.steps import DAG

# TODO: Move this to apps/wizard
DAG_WIZARD_PATH = DAG_DIR / "wizard.yml"


def add_to_dag(dag: DAG, dag_path: Path = DAG_WIZARD_PATH) -> str:
    """Add dag to dag_path file."""
    with open(dag_path, "r") as f:
        doc = ruamel_load(f)

    doc["steps"].update(dag)

    with open(dag_path, "w") as f:
        f.write(ruamel_dump(doc))

    # Get subdag as string
    return ruamel_dump({"steps": dag})


def remove_from_dag(step: str, dag_path: Path = DAG_WIZARD_PATH) -> None:
    with open(dag_path, "r") as f:
        doc = ruamel_load(f)

    doc["steps"].pop(step, None)

    with open(dag_path, "w") as f:
        # Add new step to DAG
        f.write(ruamel_dump(doc))


def generate_step(cookiecutter_path: Path, data: Dict[str, Any], target_dir: Path) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # create config file with data for cookiecutter
        config_path = cookiecutter_path / "cookiecutter.json"
        with open(config_path, "w") as f:
            json.dump(data, f, default=str)

        try:
            cookiecutter(
                cookiecutter_path.as_posix(),
                no_input=True,
                output_dir=temp_dir,
                overwrite_if_exists=True,
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


def generate_step_to_channel(cookiecutter_path: Path, data: Dict[str, Any]) -> Path:
    assert {"channel", "namespace", "version"} <= data.keys()

    target_dir = STEP_DIR / "data" / data["channel"]
    generate_step(cookiecutter_path, data, target_dir)
    return target_dir / data["namespace"] / data["version"]
