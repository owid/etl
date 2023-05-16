import datetime as dt
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


class Options(Enum):
    ADD_TO_DAG = "Add steps into dag.yml file"
    IS_PRIVATE = "Make dataset private"


class GrapherForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    garden_version: str
    add_to_dag: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


def app(run_checks: bool) -> None:
    state = utils.APP_STATE

    with open(CURRENT_DIR / "grapher.md", "r") as f:
        po.put_markdown(f.read())

    if run_checks:
        po.put_markdown("""## Checking `.env` file...""")
        utils._check_env()

    po.put_markdown("## Environment")
    utils._show_environment()

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
                "Grapher dataset version",
                name="version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the grapher dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Grapher dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=state.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.input(
                "Garden dataset version",
                name="garden_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
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
    form = GrapherForm(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    private_suffix = "-private" if form.is_private else ""

    if form.add_to_dag:
        dag_content = utils.add_to_dag(
            {
                f"data{private_suffix}://grapher/{form.namespace}/{form.version}/{form.short_name}": [
                    f"data{private_suffix}://garden/{form.namespace}/{form.garden_version}/{form.short_name}"
                ]
            }
        )
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step(CURRENT_DIR / "grapher_cookiecutter/", dict(**form.dict(), channel="grapher"))

    step_path = DATASET_DIR / (form.short_name + ".py")

    po.put_markdown(
        f"""
## Next steps

1. Test your step against your local database. If you have your grapher DB configured locally, your `.env` file should look similar to this:

    ```bash
    GRAPHER_USER_ID=your_user_id
    DB_USER=root
    DB_NAME=owid
    DB_HOST=127.0.0.1
    DB_PORT=3306
    DB_PASS=
    ```

    Get `your_user_id` by running SQL query `select id from users where email = 'your_name@ourworldindata.org'`.

    Then run the grapher step:
    ```
    poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
    ```
    Your new dataset should then appear in your local set-up: http://localhost:3030/admin/datasets. Follow the instructions [here](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md) to create your local Grapher development set-up.

2. When you feel confident, use `.env.staging` for staging which looks something like this:

    ```
    GRAPHER_USER_ID=your_user_id
    DB_USER=staging_grapher
    DB_NAME=staging_grapher
    DB_PASS=***
    DB_PORT=3306
    DB_HOST=owid-staging
    ```

    Make sure you have [Tailscale](https://tailscale.com/download) installed and running.

    After you run

    ```
    ENV=.env.staging poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
    ```

    you should see it [in staging admin](https://staging.owid.cloud/admin/datasets).

3. To get your dataset into production DB, you can merge your PR into master and it'll be deployed automatically. Alternatively, you can push it to production manually by using `.env.prod` file

    ```
    GRAPHER_USER_ID=your_user_id
    DB_USER=live_grapher
    DB_NAME=etl_grapher
    DB_PASS=***
    DB_PORT=3306
    DB_HOST=owid-live-db
    ```

    and running

    ```
    ENV=.env.prod poetry run etl grapher/{form.namespace}/{form.version}/{form.short_name} --grapher {"--private" if form.is_private else ""}
    ```

4. Check your dataset in [admin](https://owid.cloud/admin/datasets).

5. If you are an internal OWID member and, because of this dataset update, you want to update charts in our Grapher DB, continue with charts
"""
    )
    po.put_buttons(
        ["Go to charts"],
        [lambda: go_app("charts", new_window=False)],
    )
    po.put_markdown(
        """
## Generated files
"""
    )

    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content)
