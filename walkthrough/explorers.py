import datetime as dt
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):
    ADD_TO_DAG = "Add steps into dag.yml file"
    IS_PRIVATE = "Make dataset private"


class ExplorersForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    add_to_dag: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


def app(run_checks: bool) -> None:
    state = utils.APP_STATE

    with open(CURRENT_DIR / "explorers.md", "r") as f:
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
                "Explorers dataset version",
                name="version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the explorers dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Explorers dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=state.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.checkbox(
                "Additional Options",
                options=[
                    Options.ADD_TO_DAG.value,
                    Options.IS_PRIVATE.value,
                ],
                name="options",
                value=[
                    Options.ADD_TO_DAG.value,
                ],
            ),
        ],
    )
    form = ExplorersForm(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    private_suffix = "-private" if form.is_private else ""

    if form.add_to_dag:
        dag_content = utils.add_to_dag(
            {
                f"data{private_suffix}://explorers/{form.namespace}/{form.version}/{form.short_name}": [
                    f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}"
                ]
            }
        )
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step(CURRENT_DIR / "explorers_cookiecutter/", dict(**form.dict(), channel="explorers"))

    step_path = DATASET_DIR / (form.short_name + ".py")

    po.put_markdown(
        f"""
## Next steps

1. Run `etl` to generate the dataset locally

    ```
    poetry run etl data{private_suffix}://explorers/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

2. After merging to master, explorers dataset will be automatically generated and saved to [DigitalOcean Spaces](https://cloud.digitalocean.com/spaces/owid-catalog?i=8b5115&path=explorers%2F)
as `s3://owid-catalog/explorers/{form.namespace}/{form.version}/{form.short_name}`.

3. Check out docs about creating [Data explorers](https://www.notion.so/owid/Creating-Data-Explorers-cf47a5ef90f14c1fba8fc243aba79be7).


## Generated files
"""
    )

    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content)
