import os
import shutil
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any

from cookiecutter.main import cookiecutter
from owid.walden import Catalog as WaldenCatalog
from pydantic import BaseModel
from pywebio import input as pi
from pywebio import output as po

import etl
from etl.paths import DAG_FILE, STEP_DIR

from . import utils

CURRENT_DIR = Path(__file__).parent

ETL_DIR = Path(etl.__file__).parent.parent


class Options(Enum):

    ADD_TO_DAG = "Add steps into dag.yml file"
    INCLUDE_METADATA_YAML = "Include *.meta.yaml file with metadata"
    GENERATE_NOTEBOOK = "Generate validation notebook"
    LOAD_COUNTRIES_REGIONS = "Load countries regions in the script"
    LOAD_POPULATION = "Load population in the script"


class MeadowForm(BaseModel):

    short_name: str
    namespace: str
    version: str
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
        dag_content = _add_to_dag(form)
    else:
        dag_content = ""

    # cookiecutter on python files
    with tempfile.TemporaryDirectory() as temp_dir:
        OUTPUT_DIR = temp_dir

        # generate ingest scripts
        cookiecutter(
            (CURRENT_DIR / "meadow_cookiecutter/").as_posix(),
            no_input=True,
            output_dir=temp_dir,
            overwrite_if_exists=True,
            extra_context=dict(directory_name="meadow", **form.dict()),
        )

        DATASET_DIR = STEP_DIR / "data" / "meadow" / form.namespace / form.version

        shutil.copytree(
            Path(OUTPUT_DIR) / "meadow",
            DATASET_DIR,
            dirs_exist_ok=True,
        )

        step_path = DATASET_DIR / (form.short_name + ".py")
        notebook_path = DATASET_DIR / "validate.ipynb"
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
    etl data://meadow/{form.namespace}/{form.version}/{form.short_name}
    ```

2. Generated notebook `{notebook_path.relative_to(ETL_DIR)}` can be used to validate the dataset output

3. Loading the dataset is also possible with this snippet:

    ```python
    from owid.catalog import Dataset
    from etl.paths import DATA_DIR

    ds = Dataset(DATA_DIR / "meadow" / "{form.namespace}" / "{form.version}" / "{form.short_name}")
    print(ds.table_names)

    df = ds["{form.short_name}"]
    ```

4. Exit the process and run next step with `poetry run walkthrough garden`

## Generated files
"""
        )

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
            version=form.version,
        )
        po.put_success("Dataset found in Walden")
    except KeyError as e:
        # raise a warning, but continue
        if e.args[0] == "no match for dataset":
            po.put_warning("Dataset not found in Walden, did you upload it?")


def _add_to_dag(form: MeadowForm) -> str:
    with open(DAG_FILE, "r") as f:
        dag = f.read()

    s = """# Autogenerated steps by walkthrough
  data://meadow/dummy/2020/dummy:
    - walden://dummy/2020/dummy"""

    if form.load_population:
        s += "\n    - data://garden/owid/latest/key_indicators"

    if form.load_countries_regions:
        s += "\n    - data://garden/reference"

    # poor man update of dag file, see if methods from `create_new_steps.py` can help out here
    # TODO: add population and reference dataset if needed
    dag = dag.replace(
        "# Autogenerated steps by walkthrough",
        s,
    )

    with open(DAG_FILE, "w") as f:
        f.write(dag)

    return s
