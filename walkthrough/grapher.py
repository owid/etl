import shutil
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any

from cookiecutter.main import cookiecutter
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl
from etl import config
from etl.paths import STEP_DIR

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):

    ADD_TO_DAG = "Add steps into dag.yml file"


class GrapherForm(BaseModel):

    short_name: str
    namespace: str
    version: str
    add_to_dag: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        super().__init__(**data)


def app(run_checks: bool, dummy_data: bool) -> None:
    dummies = utils.DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "grapher.md", "r") as f:
        po.put_markdown(f.read())

    if run_checks:
        _check_env()

    _show_environment()

    data = pi.input_group(
        "Options",
        [
            pi.input(
                "Short name",
                name="short_name",
                placeholder="ggdc_maddison",
                required=True,
                value=dummies.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored short name",
            ),
            pi.input(
                "Namespace",
                name="namespace",
                placeholder="ggdc",
                required=True,
                value=dummies.get("namespace"),
            ),
            pi.input(
                "Version",
                name="version",
                placeholder="2020",
                required=True,
                value=dummies.get("version"),
            ),
            pi.checkbox(
                "Additional Options",
                options=[
                    Options.ADD_TO_DAG.value,
                ],
                name="options",
                value=[
                    Options.ADD_TO_DAG.value,
                ],
            ),
        ],
    )
    form = GrapherForm(**data)

    if form.add_to_dag:
        dag_content = utils.add_to_dag(
            {
                f"grapher://{form.namespace}/{form.version}/{form.short_name}": [
                    f"data://garden/{form.namespace}/{form.version}/{form.short_name}"
                ]
            }
        )
    else:
        dag_content = ""

    # cookiecutter on python files
    with tempfile.TemporaryDirectory() as temp_dir:
        OUTPUT_DIR = temp_dir

        # generate ingest scripts
        cookiecutter(
            (CURRENT_DIR / "grapher_cookiecutter/").as_posix(),
            no_input=True,
            output_dir=temp_dir,
            overwrite_if_exists=True,
            extra_context=dict(directory_name="grapher", **form.dict()),
        )

        DATASET_DIR = STEP_DIR / "grapher" / form.namespace / form.version

        shutil.copytree(
            Path(OUTPUT_DIR) / "grapher",
            DATASET_DIR,
            dirs_exist_ok=True,
        )

        step_path = DATASET_DIR / (form.short_name + ".py")

        po.put_markdown(
            f"""
## Next steps

1. Test your step against your local database. If you have your grapher DB configured locally, your `.env` file should look similar to this:

    ```
    GRAPHER_USER_ID=59
    DB_USER=root
    DB_NAME=owid
    DB_HOST=127.0.0.1
    ```

    Then run the grapher step:

    ```
    etl grapher://{form.namespace}/{form.version}/{form.short_name} --grapher
    ```

2. When you feel confident, change your `.env` to staging which looks something like this:

    ```
    GRAPHER_USER_ID=59
    DB_USER=staging_grapher
    DB_NAME=staging_grapher
    DB_PASS=***
    DB_PORT=3307
    DB_HOST=127.0.0.1
    ```

    Config above assumes you have opened SSH tunnel to staging DB on port 3307. If you have your `~/.ssh/config` set up correctly, then you can open the tunnel with

    ```
    ssh -f owid-staging -L 3307:localhost:3306 -N
    ```

    After you run

    ```
    etl grapher://{form.namespace}/{form.version}/{form.short_name} --grapher
    ```

    you should see it [in staging admin](https://staging.owid.cloud/admin/datasets).

3. Pushing to production grapher is **not yet automated**. After you get it reviewed and approved, you should again change your `.env` to production (and open SSH tunnel on port 3308 if necessary) and then run the command again from your local.

4. Check your dataset in [admin](https://owid.cloud/admin/datasets).


## Generated files
"""
        )

        utils.preview_file(step_path, "python")

        if dag_content:
            utils.preview_dag(dag_content)


def _check_env() -> None:
    po.put_markdown("""## Checking `.env` file...""")

    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            po.put_warning(
                po.put_markdown(
                    f"Environment variable `{env_name}` not found, do you have it in your `.env` file?"
                )
            )

    if ok:
        po.put_success(po.put_markdown("`.env` configured correctly"))


def _show_environment() -> None:
    po.put_markdown("## Environment")
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
