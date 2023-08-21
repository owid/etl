import datetime as dt
import os
from enum import Enum
from pathlib import Path
from typing import Any

import ruamel.yaml
from owid.catalog import Dataset
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po
from pywebio.session import go_app

import etl
from etl.paths import DATA_DIR

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):
    INCLUDE_METADATA_YAML = "Include *.meta.yaml file with metadata"
    GENERATE_NOTEBOOK = "Generate playground notebook"
    IS_PRIVATE = "Make dataset private"


class GardenForm(BaseModel):
    short_name: str
    namespace: str
    version: str
    meadow_version: str
    add_to_dag: bool
    dag_file: str
    include_metadata_yaml: bool
    generate_notebook: bool
    is_private: bool

    def __init__(self, **data: Any) -> None:
        options = data.pop("options")
        data["add_to_dag"] = data["dag_file"] != utils.ADD_DAG_OPTIONS[0]
        data["dag_file"] = data["dag_file"]
        data["include_metadata_yaml"] = Options.INCLUDE_METADATA_YAML.value in options
        data["generate_notebook"] = Options.GENERATE_NOTEBOOK.value in options
        data["is_private"] = Options.IS_PRIVATE.value in options
        super().__init__(**data)


def app(run_checks: bool) -> None:
    state = utils.APP_STATE

    po.put_markdown("# Walkthrough - Garden")
    with open(CURRENT_DIR / "garden.md", "r") as f:
        po.put_collapse("Instructions", [po.put_markdown(f.read())])

    data = pi.input_group(
        "Options",
        [
            pi.input(
                "Namespace",
                name="namespace",
                placeholder="institution",
                required=True,
                value=state.get("namespace") or "",
                help_text="Institution name. Example: emdat",
            ),
            pi.input(
                "Garden dataset version",
                name="version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the garden dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.input(
                "Garden dataset short name",
                name="short_name",
                placeholder="testing_dataset_name",
                required=True,
                value=state.get("short_name") or "",
                validate=utils.validate_short_name,
                help_text="Underscored dataset short name. Example: natural_disasters",
            ),
            pi.input(
                "Meadow dataset version",
                name="meadow_version",
                placeholder=str(dt.date.today()),
                required=True,
                value=state.get("version", str(dt.date.today())),
                help_text="Version of the meadow dataset (by default, the current date, or exceptionally the publication date).",
            ),
            pi.select("Add to DAG", utils.ADD_DAG_OPTIONS, name="dag_file", value=state.get("dag_file")),
            pi.checkbox(
                "Additional Options",
                options=[
                    Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                    Options.IS_PRIVATE.value,
                ],
                name="options",
                value=[
                    Options.INCLUDE_METADATA_YAML.value,
                    Options.GENERATE_NOTEBOOK.value,
                ],
            ),
        ],
    )
    form = GardenForm(**data)

    # save form data to global state for next steps
    state.update(form.dict())

    if run_checks:
        _check_dataset_in_meadow(form)

    private_suffix = "-private" if form.is_private else ""

    if form.add_to_dag:
        deps = [f"data{private_suffix}://meadow/{form.namespace}/{form.meadow_version}/{form.short_name}"]
        dag_content = utils.add_to_dag(
            dag={f"data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name}": deps},
            dag_path=CURRENT_DIR / ".." / "dag" / form.dag_file,
        )
    else:
        dag_content = ""

    DATASET_DIR = utils.generate_step_to_channel(
        CURRENT_DIR / "garden_cookiecutter/", dict(**form.dict(), channel="garden")
    )

    step_path = DATASET_DIR / (form.short_name + ".py")
    notebook_path = DATASET_DIR / "playground.ipynb"
    metadata_path = DATASET_DIR / (form.short_name + ".meta.yml")

    if not form.generate_notebook:
        os.remove(notebook_path)

    if not form.include_metadata_yaml:
        os.remove(metadata_path)

    if form.namespace == "dummy":
        _fill_dummy_metadata_yaml(metadata_path)

    po.put_markdown(
        f"""
## Next steps

1. Harmonize country names with the following command (assuming country field is called `country`). Check out a [short demo](https://drive.google.com/file/d/1tBFMkgOgy4MmB7E7NmfMlfa4noWaiG3t/view) of the tool

    ```
    poetry run etl-harmonize data/meadow/{form.namespace}/{form.meadow_version}/{form.short_name}/{form.short_name}.feather country etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.countries.json
    ```

    you can also add more countries manually there or to `{form.short_name}.country_excluded.json` file.

2. Run `etl` to generate the dataset

    ```
    poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

2. (Optional) Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to examine the dataset output interactively.

3. (Optional) Generate metadata file `{form.short_name}.meta.yml` from your dataset with

    ```
    poetry run etl-metadata-export data/garden/{form.namespace}/{form.version}/{form.short_name} -o etl/steps/data/garden/{form.namespace}/{form.version}/{form.short_name}.meta.yml
    ```

    then manual edit it and rerun the step again with

    ```
    poetry run etl data{private_suffix}://garden/{form.namespace}/{form.version}/{form.short_name} {"--private" if form.is_private else ""}
    ```

    Note that metadata is inherited from previous step (snapshot) and you don't have to repeat it.

4. (Optional) You can manually move steps from `dag/walkthrough.yml` to some other `dag/*.yml` if you feel like it belongs there. After you are happy with your code, run `make test` to find any issues.

5. Create a pull request in [ETL](https://github.com/owid/etl), get it reviewed and merged.

6. (Optional) Once your changes are merged, your steps will be run automatically by our server and published to the OWID catalog. Then it can be loaded by anyone using:

    ```python
    from owid.catalog import find_one
    tab = find_one(table="{form.short_name}", namespace="{form.namespace}", dataset="{form.short_name}")
    print(tab.metadata)
    print(tab.head())
    ```

7. If you are an internal OWID member and want to push data to our Grapher DB, continue to the grapher step or to explorers step.
"""
    )
    po.put_buttons(
        ["Go to grapher", "Go to explorers"],
        [lambda: go_app("grapher", new_window=False), lambda: go_app("explorers", new_window=False)],
    )
    po.put_markdown(
        """

## Generated files
"""
    )

    if form.include_metadata_yaml:
        utils.preview_file(metadata_path, "yaml")
    utils.preview_file(step_path, "python")

    if dag_content:
        utils.preview_dag(dag_content=dag_content, dag_name=f"`dag/{form.dag_file}`")


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


def _fill_dummy_metadata_yaml(metadata_path: Path) -> None:
    """Fill dummy metadata yaml file with some dummy values. Only useful when
    --dummy-data is used. We need this to avoid errors in `walkthrough grapher --dummy-data`."""
    with open(metadata_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    if utils.WALKTHROUGH_ORIGINS:
        # add all available metadata fields to dummy variable
        variable_meta = {
            "title": "Dummy",
            "description": "This is a dummy indicator with full metadata.",
            "licenses": [],
            "unit": "Dummy unit",
            "short_unit": "Du",
            "display": {
                "isProjection": True,
                "conversionFactor": 1000,
                "numDecimalPlaces": 1,
                "tolerance": 5,
                "yearIsDay": False,
                "zeroDay": "1900-01-01",
                "entityAnnotationsMap": "Germany: dummy annotation",
                "includeInTable": True,
            },
            "description_short": "Short description of the dummy indicator.",
            "description_from_producer": "The description of the dummy indicator by the producer, shown separately on a data page.",
            "processing_level": "major",
            "license": {"name": "CC-BY 4.0", "url": ""},
            "presentation": {
                "grapher_config": {
                    "title": "The dummy indicator - chart title",
                    "subtitle": "You'll never guess where the line will go",
                    "hasMapTab": True,
                    "selectedEntityNames": ["Germany", "Italy", "France"],
                },
                "title_public": "The dummy indicator - data page title",
                "title_variant": "historical data",
                "producer_short": "ACME",
                "attribution": "ACME project",
                "topic_tags_links": ["Internet"],
                "key_info_text": [
                    "First bullet point info about the data. [Detail on demand link](#dod:primaryenergy)",
                    "Second bullet point with **bold** text and a [normal link](https://ourworldindata.org)",
                ],
                "faqs": [{"fragment_id": "cherries", "gdoc_id": "16uGVylqtS-Ipc3OCxqapJ3BEVGjWf648wvZpzio1QFE"}],
            },
        }

        doc["tables"]["dummy"]["variables"] = {"dummy_variable": variable_meta}
    else:
        doc["tables"]["dummy"]["variables"] = {"dummy_variable": {"unit": "dummy unit", "title": "Dummy"}}

    with open(metadata_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)
