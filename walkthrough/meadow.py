import datetime as dt
import os
from enum import Enum
from pathlib import Path
from typing import Any

from owid.walden import Catalog as WaldenCatalog
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):

    ADD_TO_DAG = "Add steps into dag.yml file"
    INCLUDE_METADATA_YAML = "Include *.meta.yaml file with metadata"
    GENERATE_NOTEBOOK = "Generate playground notebook"
    LOAD_COUNTRIES_REGIONS = "Load countries regions in the script"
    LOAD_POPULATION = "Load population in the script"


class MeadowForm(BaseModel):

    short_name: str
    namespace: str
    version: str
    walden_version: str
    add_to_dag: bool
    load_countries_regions: bool
    load_population: bool
    generate_notebook: bool
    include_metadata_yaml: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = Options.ADD_TO_DAG.value in options
        data["include_metadata_yaml"] = Options.INCLUDE_METADATA_YAML.value in options
        data["load_countries_regions"] = Options.LOAD_COUNTRIES_REGIONS.value in options
        data["load_population"] = Options.LOAD_POPULATION.value in options
        data["generate_notebook"] = Options.GENERATE_NOTEBOOK.value in options
        super().__init__(**data)


def app(run_checks: bool, dummy_data: bool) -> None:
    dummies = utils.DUMMY_DATA if dummy_data else {}

    with open(CURRENT_DIR / "meadow.md", "r") as f:
        po.put_markdown(f.read())

    data = pi.input_group(
        "Options",
        [
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
                placeholder=str(dt.date.today()),
                required=True,
                value=dummies.get("version", str(dt.date.today())),
            ),
            pi.input(
                "Walden version",
                name="walden_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=dummies.get("version", str(dt.date.today())),
                help_text="Usually same as Version",
            ),
            pi.input(
                "Short name",
                name="short_name",
                placeholder="ggdc_maddison",
                required=True,
                value=dummies.get("short_name"),
                validate=utils.validate_short_name,
                help_text="Underscored short name",
            ),
            pi.checkbox(
                "Additional Options",
                options=[
                    Options.ADD_TO_DAG.value,
                    Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                    Options.LOAD_COUNTRIES_REGIONS.value,
                    Options.LOAD_POPULATION.value,
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
    form = MeadowForm(**data)

    if run_checks:
        _check_dataset_in_walden(form)

    if form.add_to_dag:
        deps = [f"walden://{form.namespace}/{form.walden_version}/{form.short_name}"]
        if form.load_population:
            deps.append("data://garden/owid/latest/key_indicators")
        if form.load_countries_regions:
            deps.append("data://garden/reference")
        dag_content = utils.add_to_dag({f"data://meadow/{form.namespace}/{form.version}/{form.short_name}": deps})
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step(CURRENT_DIR / "meadow_cookiecutter/", form.dict())

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
    poetry run etl data://meadow/{form.namespace}/{form.version}/{form.short_name}
    ```

2. (Optional) Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to examine the dataset output interactively.

3. (Optional) Loading the dataset is also possible with this snippet:

    ```python
    from owid.catalog import Dataset
    from etl.paths import DATA_DIR

    ds = Dataset(DATA_DIR / "meadow" / "{form.namespace}" / "{form.version}" / "{form.short_name}")
    print(ds.table_names)

    df = ds["{form.short_name}"]
    ```

4. (Optional) Generate metadata file `{form.short_name}.meta.yml` from your dataset with

    ```
    poetry run etl-metadata-export data/meadow/{form.namespace}/{form.version}/{form.short_name} -o etl/steps/data/meadow/{form.namespace}/{form.version}/{form.short_name}.meta.yml
    ```

    then manual edit it and rerun the step again with

    ```
    poetry run etl data://meadow/{form.namespace}/{form.version}/{form.short_name}
    ```

5. Exit the process and run next step with `poetry run walkthrough garden`

## Generated files
"""
    )

    if form.include_metadata_yaml:
        utils.preview_file(metadata_path, "yaml")
    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content)


def _check_dataset_in_walden(form: MeadowForm) -> None:
    po.put_markdown("""## Checking Walden dataset...""")
    try:
        WaldenCatalog().find_one(
            namespace=form.namespace,
            short_name=form.short_name,
            version=form.walden_version,
        )
        po.put_success("Dataset found in Walden")
    except KeyError as e:
        # raise a warning, but continue
        if "no match for dataset" in e.args[0]:
            po.put_warning("Dataset not found in Walden, did you upload it?")
