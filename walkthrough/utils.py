from pathlib import Path
from typing import Optional

import yaml
from owid.catalog.utils import validate_underscore
from pywebio import output as po

from etl.paths import BASE_DIR
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = BASE_DIR / "dag_files/dag_walkthrough.yml"

DUMMY_DATA = {
    "short_name": "dummy",
    "namespace": "dummy",
    "version": "2020",
    "walden_version": "2020",
    "name": "Dummy dataset",
    "description": "This\nis\na\ndummy\ndataset",
    "file_extension": "xlsx",
    "source_data_url": "https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2020.xlsx",
    "publication_year": 2020,
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
            "title": po.put_success(
                po.put_markdown(f"File `{path}` was successfully generated")
            ),
            "contents": [po.put_markdown(f"```{language}\n{t}```")],
        },
    )


def preview_dag(dag_content: str) -> None:
    po.put_widget(
        WIDGET_TEMPLATE,
        {
            "open": False,
            "title": po.put_success(
                po.put_markdown("Steps in dag.yml were successfully generated")
            ),
            "contents": [po.put_markdown(f"```yml\n  {dag_content}\n```")],
        },
    )


def add_to_dag(dag: DAG) -> str:
    with open(DAG_WALKTHROUGH_PATH, "r") as f:
        dag_str = f.read()

    steps = yaml.dump({"steps": dag}).split("\n", 1)[1]

    dag_str += steps

    with open(DAG_WALKTHROUGH_PATH, "w") as f:
        f.write(dag_str)

    return dag_str
