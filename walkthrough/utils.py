import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import ruamel.yaml
import yaml
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog.utils import validate_underscore
from pywebio import output as po

from etl.paths import BASE_DIR, SNAPSHOTS_DIR, STEP_DIR
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = BASE_DIR / "dag_files/dag_walkthrough.yml"
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"

DUMMY_DATA = {
    "namespace": "dummy",
    "short_name": "dummy",
    "version": "2020-01-01",
    "walden_version": "2020-01-01",
    "snapshot_version": "2020-01-01",
    "name": "Dummy dataset",
    "description": "This\nis\na\ndummy\ndataset",
    "file_extension": "xlsx",
    "source_data_url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2020.xlsx",
    "publication_date": "2020-01-01",
    "source_name": "dummy source",
    "url": "https://www.dummy.com/",
}


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


def preview_dag(dag_content: str, dag_name: str = "dag.yml") -> None:
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


def generate_step(cookiecutter_path: Path, data: Dict[str, Any]) -> Path:
    assert {"channel", "namespace", "version"} <= data.keys()

    with tempfile.TemporaryDirectory() as temp_dir:
        OUTPUT_DIR = temp_dir

        # generate ingest scripts
        cookiecutter(
            cookiecutter_path.as_posix(),
            no_input=True,
            output_dir=temp_dir,
            overwrite_if_exists=True,
            extra_context=dict(directory_name=data["channel"], **data),
        )

        if data["channel"] == "walden":
            DATASET_DIR = WALDEN_INGEST_DIR / data["namespace"] / data["version"]
        elif data["channel"] == "snapshots":
            DATASET_DIR = SNAPSHOTS_DIR / data["namespace"] / data["version"]
        else:
            DATASET_DIR = STEP_DIR / "data" / data["channel"] / data["namespace"] / data["version"]

        shutil.copytree(
            Path(OUTPUT_DIR) / data["channel"],
            DATASET_DIR,
            dirs_exist_ok=True,
        )

    return DATASET_DIR
