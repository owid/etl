import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog.utils import validate_underscore
from pywebio import output as po

from etl.paths import BASE_DIR, STEP_DIR
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = BASE_DIR / "dag_files/dag_walkthrough.yml"
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"

DUMMY_DATA = {
    "namespace": "dummy",
    "short_name": "dummy",
    "version": "2020-01-01",
    "walden_version": "2020-01-01",
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


def preview_file(path: Path, language: str) -> None:
    with open(path) as f:
        t = f.read()

    po.put_widget(
        WIDGET_TEMPLATE,
        {
            "open": False,
            "title": po.put_success(po.put_markdown(f"File `{path}` was successfully generated")),
            "contents": [po.put_markdown(f"```{language}\n{t}```")],
        },
    )


def preview_dag(dag_content: str, dag_name: str = "dag.yml") -> None:
    po.put_widget(
        WIDGET_TEMPLATE,
        {
            "open": False,
            "title": po.put_success(po.put_markdown(f"Steps in {dag_name} were successfully generated")),
            "contents": [po.put_markdown(f"```yml\n  {dag_content}\n```")],
        },
    )


def add_to_dag(dag: DAG, dag_path: Path = DAG_WALKTHROUGH_PATH) -> str:
    # read dag as string and as dictionary
    with open(dag_path, "r") as f:
        dag_str = f.read()
        # replace empty dag
        dag_str = dag_str.replace("{}", "")
        f.seek(0)
        dag_dict = yaml.safe_load(f)

    # exclude steps which are already there
    dag = {k: v for k, v in dag.items() if k not in dag_dict["steps"]}

    # step is already there don't add anything
    if not dag:
        return dag_str

    steps = yaml.dump({"steps": dag}).split("\n", 1)[1]

    dag_str += steps

    with open(dag_path, "w") as f:
        f.write(dag_str)

    return steps


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
        else:
            DATASET_DIR = STEP_DIR / "data" / data["channel"] / data["namespace"] / data["version"]

        shutil.copytree(
            Path(OUTPUT_DIR) / data["channel"],
            DATASET_DIR,
            dirs_exist_ok=True,
        )

    return DATASET_DIR
