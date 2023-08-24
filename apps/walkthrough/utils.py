import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import ruamel.yaml
import yaml
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog.utils import validate_underscore
from pywebio import output as po

from etl import config
from etl.files import apply_black_formatter_to_files
from etl.paths import (
    DAG_DIR,
    LATEST_POPULATION_VERSION,
    LATEST_REGIONS_VERSION,
    STEP_DIR,
)
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = DAG_DIR / "walkthrough.yml"
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"

# Load latest dataset versions
DATASET_POPULATION_URI = f"data://garden/demography/{LATEST_POPULATION_VERSION}/population"
DATASET_REGIONS_URI = f"data://garden/regions/{LATEST_REGIONS_VERSION}/regions"

# use origins in walkthrough
# WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "1") == "1"
WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "0") == "1"

# DAG dropdown options
dag_files = sorted(os.listdir(DAG_DIR))
dag_not_add_option = "(do not add to DAG)"
ADD_DAG_OPTIONS = [dag_not_add_option] + dag_files

if WALKTHROUGH_ORIGINS:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "dataset_title_owid": "Dummy OWID dataset title",
        "dataset_description_owid": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "date_published": "2020-01-01",
        "producer": "Dummy producer",
        "citation_producer": "Dummy producer citation",
        "dataset_url_download": "https://raw.githubusercontent.com/owid/etl/master/walkthrough/dummy_data.csv",
        "dataset_url_main": "https://www.url-dummy.com/",
        "license_name": "MIT dummy license",
    }
else:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "name": "Dummy dataset",
        "description": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "source_data_url": "https://raw.githubusercontent.com/owid/etl/master/walkthrough/dummy_data.csv",
        "publication_date": "2020-01-01",
        "source_name": "Dummy short source citation",
        "source_published_by": "Dummy full source citation",
        "url": "https://www.url-dummy.com/",
    }

# state shared between steps
APP_STATE = {}


def validate_short_name(short_name: str) -> Optional[str]:
    try:
        validate_underscore(short_name, "Short name")
        return None
    except Exception as e:
        return str(e)


WIDGET_TEMPLATE = """
<details {{#open}}open{{/open}}>
    <summary>
    {{#title}}
        {{& pywebio_output_parse}}
    {{/title}}
    </summary>
    {{#contents}}
        {{& pywebio_output_parse}}
    {{/contents}}
</details>
"""


def put_widget(title: Any, contents: List[Any]) -> None:
    """Widget that allows markdown in title."""
    po.put_widget(
        WIDGET_TEMPLATE,
        {
            "open": False,
            "title": title,
            "contents": contents,
        },
    )


def preview_file(path: Path, language: str) -> None:
    with open(path) as f:
        t = f.read()

    put_widget(
        title=po.put_success(po.put_markdown(f"File `{path}` was successfully generated")),
        contents=[po.put_markdown(f"```{language}\n{t}```")],
    )


def preview_dag(dag_content: str, dag_name: Union[str, Path] = DAG_WALKTHROUGH_PATH) -> None:
    put_widget(
        title=po.put_success(po.put_markdown(f"Steps in {dag_name} were successfully generated")),
        contents=[po.put_markdown(f"```yml\n{dag_content}\n```")],
    )


def add_to_dag(dag: DAG, dag_path: Path = DAG_WALKTHROUGH_PATH) -> str:
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].update(dag)

    with open(dag_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)

    return yaml.dump({"steps": dag})


def remove_from_dag(step: str, dag_path: Path = DAG_WALKTHROUGH_PATH) -> None:
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].pop(step, None)

    with open(dag_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)


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
        apply_black_formatter_to_files(file_paths=list(Path(temp_dir).glob("**/*.py")))

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


def _check_env() -> bool:
    """Check if environment variables are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            po.put_warning(
                po.put_markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?")
            )

    if ok:
        po.put_success(po.put_markdown("`.env` configured correctly"))
    return ok


def _show_environment() -> None:
    """Show environment variables."""
    po.put_info(
        po.put_markdown(
            f"""
    * **GRAPHER_USER_ID**: `{config.GRAPHER_USER_ID}`
    * **DB_USER**: `{config.DB_USER}`
    * **DB_NAME**: `{config.DB_NAME}`
    * **DB_HOST**: `{config.DB_HOST}`
    """
        )
    )


OWIDEnvType = Literal["live", "staging", "local", "unknown"]


class OWIDEnv:
    env_type_id: OWIDEnvType

    def __init__(
        self,
        env_type_id: Optional[OWIDEnvType] = None,
    ):
        if env_type_id is None:
            self.env_type_id = self.detect_env_type()
        else:
            self.env_type_id = env_type_id

    def detect_env_type(self) -> OWIDEnvType:
        # live
        if config.DB_NAME == "live_grapher":
            return "live"
        # staging
        elif config.DB_NAME == "staging_grapher" and config.DB_USER == "staging_grapher":
            return "staging"
        # local
        elif config.DB_NAME == "grapher" and config.DB_USER == "grapher":
            return "local"
        return "unknown"

    @property
    def admin_url(self):
        if self.env_type_id == "live":
            return "https://owid.cloud/admin"
        elif self.env_type_id == "staging":
            return "https://staging.owid.cloud/admin"
        elif self.env_type_id == "local":
            return "http://localhost:3030/admin"
        return None

    @property
    def chart_approval_tool_url(self):
        return f"{self.admin_url}/suggested-chart-revisions/review"

    def dataset_admin_url(self, dataset_id: Union[str, int]):
        return f"{self.admin_url}/datasets/{dataset_id}/"

    def variable_admin_url(self, variable_id: Union[str, int]):
        return f"{self.admin_url}/variables/{variable_id}/"

    def chart_admin_url(self, chart_id: Union[str, int]):
        return f"{self.admin_url}/charts/{chart_id}/edit"
