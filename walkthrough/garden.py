import datetime as dt
import os
from enum import Enum
from pathlib import Path
from typing import Any

from owid.catalog import Dataset
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl
from etl.paths import DATA_DIR

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):

    ADD_TO_DAG = "Add steps into dag.yml file"
    INCLUDE_METADATA_YAML = "Include *.meta.yaml file with metadata"
    GENERATE_NOTEBOOK = "Generate playground notebook"
    IS_PRIVATE = "Make dataset private"


class GardenForm(BaseModel):

    short_name: str
    namespace: str
    version: str
    meadow_version: str
    add_to_dag: bool
    include_metadata_yaml: bool
    generate_notebook: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["include_metadata_yaml"] = Options.INCLUDE_METADATA_YAML.value in options
        data["generate_notebook"] = Options.GENERATE_NOTEBOOK.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


def app(run_checks: bool, dummy_data: bool) -> None:
    dummies = utils.DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "garden.md", "r") as f:
        po.put_markdown(f.read())

    data = pi.input_group(
        "Options",
        [
            pi.input(
                "Namespace",
                name="namespace",
                placeholder="institution",
                required=True,
                value=dummies.get("namespace"),
                help_text="Institution name. Example: emdat",
            ),
            pi.input(
                "Garden dataset version",
                name="version",
                placeholder=str(dt.date.today()),
                required=True,
                value=dummies.get("version", str(dt.date.today())),
                help_text="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Garden dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=dummies.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.input(
                "Meadow dataset version",
                name="meadow_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=dummies.get("version", str(dt.date.today())),
                help_text="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
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
                    Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                ],
            ),
        ],
    )
    form = GardenForm(**data)

    if run_checks:
        _check_dataset_in_meadow(form)

    private_suffix = "-private" if form.is_private else ""

    if form.add_to_dag:
        dag_content = utils.add_to_dag(
            {
                f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}": [
                    f"data{private_suffix}://meadow/{form.namespace}/{form.meadow_version}/{form.short_name}"
                ]
            }
        )
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step(CURRENT_DIR / "garden_cookiecutter/", dict(**form.dict(), channel="garden"))

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

1. Harmonize country names with the following command (assuming country field is called `country`). Check out a [short demo](https://drive.google.com/file/d/1tBFMkgOgy4MmB7E7NmfMlfa4noWaiG3t/view) of the tool

    ```
    poetry run harmonize data/meadow/{form.namespace}/{form.meadow_version}/{form.short_name}/{form.short_name}.feather country etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.countries.json
    ```

    you can also add more countries manually there or to `{form.short_name}.country_excluded.json` file.

2. Run `etl` to generate the dataset

    ```
    poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

2. (Optional) Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to examine the dataset output interactively.

3. (Optional) Loading the dataset is also possible with this snippet:

    ```python
    from owid.catalog import Dataset
    from etl.paths import DATA_DIR

    ds = Dataset(DATA_DIR / "garden" / "{form.namespace}" / "{form.version}" / "{form.short_name}")
    print(ds.table_names)

    df = ds["{form.short_name}"]
    ```

4. (Optional) Generate metadata file `{form.short_name}.meta.yml` from your dataset with

    ```
    poetry run etl-metadata-export data/garden/{form.namespace}/{form.version}/{form.short_name} -o etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.meta.yml
    ```

    then manual edit it and rerun the step again with

    ```
    poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

5. Create a branch in [ETL](https://github.com/owid/etl), get it reviewed and merged.

6. Once your changes are merged, your steps will be run automatically by our server and published to the OWID catalog. Once that is finished, it can be found by anyone using:

    ```python
    from owid.catalog import find_one
    tab = find_one(table="{form.short_name}", namespace="{form.namespace}", dataset="{form.short_name}")
    print(tab.metadata)
    print(tab.head())
    ```

7. If you are an internal OWID member and want to push data to our Grapher DB, continue with `poetry run walkthrough grapher`

## Generated files
"""
    )

    if form.include_metadata_yaml:
        utils.preview_file(metadata_path, "yaml")
    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content)


def _check_dataset_in_meadow(form: GardenForm) -> None:
    private_suffix = "-private" if form.is_private else ""

    po.put_markdown("""## Checking Meadow dataset...""")
    cmd = f"etl data{private_suffix}://meadow/{form.namespace}/{form.meadow_version}/{form.short_name}"

    try:
        ds = Dataset(DATA_DIR / "meadow" / form.namespace / form.meadow_version / form.short_name)
        if form.short_name not in ds.table_names:
            po.put_warning(
                po.put_markdown(f"Table `{form.short_name}` not found in Meadow dataset, have you run ```\n{cmd}\n```?")
            )
        else:
            po.put_success("Dataset found in Meadow")
    except FileNotFoundError:
        # raise a warning, but continue
        po.put_warning(po.put_markdown(f"Dataset not found in Meadow, have you run ```\n{cmd}\n```?"))
