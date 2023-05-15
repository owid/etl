import datetime as dt
import os
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po
from pywebio.session import go_app

import etl

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent
DEFAULT_EXTENSION = "csv"


class Options(Enum):
    ADD_TO_DAG = "Add steps into dag/walkthrough.yaml file"
    INCLUDE_METADATA_YAML = "Include *.meta.yaml file with metadata"
    GENERATE_NOTEBOOK = "Generate playground notebook"
    IS_PRIVATE = "Make dataset private"


class MeadowForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    snapshot_version: str
    file_extension: str
    add_to_dag: bool
    generate_notebook: bool
    include_metadata_yaml: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        if data["file_extension"] == "":
            data["file_extension"] = DEFAULT_EXTENSION
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["include_metadata_yaml"] = Options.INCLUDE_METADATA_YAML.value in options
        data["generate_notebook"] = Options.GENERATE_NOTEBOOK.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


def app(run_checks: bool) -> None:
    state = utils.APP_STATE

    with open(CURRENT_DIR / "meadow.md", "r") as f:
        po.put_markdown(f.read())

    data = pi.input_group(
        "Options",
        [
            pi.input(
                "Namespace",
                name="namespace",
                placeholder="institution",
                required=True,
                value=state.get("namespace"),
                help_text="Institution name. Example: emdat",
            ),
            pi.input(
                "Meadow dataset version",
                name="version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Meadow dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=state.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.input(
                "Snapshot version",
                name="snapshot_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Snapshot version (usually the same as the meadow version).",
            ),
            pi.input(
                "Snapshot file extension",
                name="file_extension",
                placeholder=DEFAULT_EXTENSION,
                value=state.get("file_extension"),
                help_text="File extension (without the '.') of the snapshot data file. Example: csv",
            ),
            pi.checkbox(
                "Additional Options",
                options=[
                    Options.ADD_TO_DAG.value,
                    Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                    Options.IS_PRIVATE.value,
                ],
                name="options",
                value=[
                    Options.ADD_TO_DAG.value,
                    # Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                ],
            ),
        ],
    )
    form = MeadowForm(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    private_suffix = "-private" if form.is_private else ""

    if form.add_to_dag:
        dag_content = utils.add_to_dag(
            {
                f"data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name}": [
                    f"snapshot{private_suffix}://{form.namespace}/{form.snapshot_version}/{form.short_name}.{form.file_extension}",
                ]
            }
        )
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step(CURRENT_DIR / "meadow_cookiecutter/", dict(**form.dict(), channel="meadow"))

    step_path = DATASET_DIR / (form.short_name + ".py")
    notebook_path = DATASET_DIR / "playground.ipynb"
    metadata_path = DATASET_DIR / (form.short_name + ".meta.yml")

    if not form.generate_notebook:
        os.remove(notebook_path)

    if not form.include_metadata_yaml:
        os.remove(metadata_path)

    po.put_markdown(
        f"""
## Next steps

1. Run `etl` to generate the dataset

    ```
    poetry run etl data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

2. (Optional) Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to examine the dataset output interactively.

3. (Optional) Generate metadata file `{form.short_name}.meta.yml` from your dataset with

    ```
    poetry run etl-metadata-export data/meadow/{form.namespace}/{form.version}/{form.short_name} -o etl/steps/data/meadow/{form.namespace}/{form.version}/{form.short_name}.meta.yml
    ```

    then manual edit it and rerun the step again with

    ```
    poetry run etl data{private_suffix}://meadow/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

    Note that metadata is inherited from previous step (snapshot) and you don't have to repeat it.

4. Continue to the garden step
"""
    )
    po.put_buttons(["Go to garden"], [lambda: go_app("garden", new_window=False)])
    po.put_markdown(
        """

## Generated files
"""
    )

    if form.include_metadata_yaml:
        utils.preview_file(metadata_path, "yaml")
    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content)
