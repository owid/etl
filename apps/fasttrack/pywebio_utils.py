from pathlib import Path
from typing import Any, List, Union

from pywebio import output as po

from apps.wizard.utils import DAG_WIZARD_PATH

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


def preview_dag(dag_content: str, dag_name: Union[str, Path] = DAG_WIZARD_PATH) -> None:
    put_widget(
        title=po.put_success(po.put_markdown(f"Steps in {dag_name} were successfully generated")),
        contents=[po.put_markdown(f"```yml\n{dag_content}\n```")],
    )
