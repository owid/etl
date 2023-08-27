import datetime as dt
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import ruamel.yaml
import streamlit as st
import yaml
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog.utils import validate_underscore

from etl.files import apply_black_formatter_to_files
from etl.paths import (
    DAG_DIR,
    LATEST_POPULATION_VERSION,
    LATEST_REGIONS_VERSION,
    STEP_DIR,
)
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = DAG_DIR / "walkthrough.yml"
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"

# Load latest dataset versions
DATASET_POPULATION_URI = f"data://garden/demography/{LATEST_POPULATION_VERSION}/population"
DATASET_REGIONS_URI = f"data://garden/regions/{LATEST_REGIONS_VERSION}/regions"

# use origins in walkthrough
# WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "1") == "1"
WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "0") == "1"

# DAG dropdown options
dag_files = sorted(os.listdir(DAG_DIR))
dag_not_add_option = "(do not add to DAG)"
ADD_DAG_OPTIONS = [dag_not_add_option] + dag_files

# Date today
DATE_TODAY = dt.date.today().strftime("%Y-%m-%d")


if WALKTHROUGH_ORIGINS:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "dataset_title_owid": "Dummy OWID dataset title",
        "dataset_description_owid": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "date_published": "2020-01-01",
        "producer": "Dummy producer",
        "citation_producer": "Dummy producer citation",
        "dataset_url_download": "https://raw.githubusercontent.com/owid/etl/master/walkthrough/dummy_data.csv",
        "dataset_url_main": "https://www.url-dummy.com/",
        "license_name": "MIT dummy license",
    }
else:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "name": "Dummy dataset",
        "description": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "source_data_url": "https://raw.githubusercontent.com/owid/etl/master/walkthrough/dummy_data.csv",
        "publication_date": "2020-01-01",
        "source_name": "Dummy short source citation",
        "source_published_by": "Dummy full source citation",
        "url": "https://www.url-dummy.com/",
    }

# state shared between steps
APP_STATE = {}


def validate_short_name(short_name: str) -> Optional[str]:
    try:
        validate_underscore(short_name, "Short name")
        return None
    except Exception as e:
        return str(e)


def add_to_dag(dag: DAG, dag_path: Path = DAG_WALKTHROUGH_PATH) -> str:
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].update(dag)

    with open(dag_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)

    return yaml.dump({"steps": dag})


def remove_from_dag(step: str, dag_path: Path = DAG_WALKTHROUGH_PATH) -> None:
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].pop(step, None)

    with open(dag_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)


def generate_step(cookiecutter_path: Path, data: Dict[str, Any], target_dir: Path) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # create config file with data for cookiecutter
        config_path = cookiecutter_path / "cookiecutter.json"
        with open(config_path, "w") as f:
            json.dump(data, f, default=str)

        try:
            cookiecutter(
                cookiecutter_path.as_posix(),
                no_input=True,
                output_dir=temp_dir,
                overwrite_if_exists=True,
            )
        finally:
            config_path.unlink()

        # Apply black formatter to generated files.
        apply_black_formatter_to_files(file_paths=list(Path(temp_dir).glob("**/*.py")))

        shutil.copytree(
            Path(temp_dir),
            target_dir,
            dirs_exist_ok=True,
        )


def generate_step_to_channel(cookiecutter_path: Path, data: Dict[str, Any]) -> Path:
    assert {"channel", "namespace", "version"} <= data.keys()

    target_dir = STEP_DIR / "data" / data["channel"]
    generate_step(cookiecutter_path, data, target_dir)
    return target_dir / data["namespace"] / data["version"]


class SessionState:
    """Management of state variables shared across different apps."""

    steps: List[str] = ["snapshot", "meadow", "garden", "grapher", "explorers"]

    def __init__(self: "SessionState", step: str) -> "SessionState":
        """Construct variable."""
        self.step = step
        self._init_steps()

    def _init_steps(self: "SessionState") -> None:
        if "steps" not in st.session_state:
            st.session_state["steps"] = {}
        for step in self.steps:
            if step not in st.session_state["steps"]:
                st.session_state["steps"][step] = {}
                # Defaults for Snapshot
                if step == "snapshot":
                    st.session_state["steps"][step] = {
                        **st.session_state["steps"][step],
                        **{
                            f"{step}.snapshot_version": DATE_TODAY,
                            f"{step}.origin.date_accessed": DATE_TODAY,
                        },
                    }

    def _check_step(self: "SessionState") -> None:
        """Check that the value for step is valid."""
        if self.step is None or self.step not in self.steps:
            raise ValueError(f"Step {self.step} not in {self.steps}.")

    def get_variables_of_step(self: "SessionState") -> Dict[str, Any]:
        """Get variables of a specific step.

        Variables are assumed to have keys `step.NAME`, based on the keys given in the widgets within a form.
        """
        return {k: v for k, v in st.session_state.items() if k.startswith(f"{self.step}.")}

    def update(self: "SessionState") -> None:
        """Update global variables of step.

        This is expected to be called when submitting the step's form.
        """
        self._check_step()
        print(f"Updating {self.step}...")
        st.session_state["steps"][self.step] = self.get_variables_of_step()

    def default_value(
        self: "SessionState", key: str, previous_step: Optional[str] = None, default_last: Optional[Any] = ""
    ) -> str:
        """Get the default value of a variable.

        This is useful when setting good defaults in widgets (e.g. text_input).

        Priority of default value is:
            - Check if there is a value stored for this field in the current step.
            - If not, check if there is a value stored for this field in the previous step.
            - If not, use value given by `default_last`.
        """
        self._check_step()
        # Get name of previous step
        if previous_step is None:
            previous_step = self.previous_step
        # (1) Get value stored for this field (in current step)
        value_step = st.session_state["steps"][self.step].get(key)
        if value_step:
            return value_step
        # (2) If none, check if previous step has a value and use that one, otherwise (3) use empty string.
        key = key.replace(f"{self.step}.", f"{self.previous_step}.")
        return st.session_state["steps"][self.previous_step].get(key, default_last)

    @property
    def previous_step(self: "SessionState") -> str:
        """Get the name of the previous step.

        E.g. 'snapshot' is the step prior to 'meadow', etc.
        """
        self._check_step()
        idx = max(self.steps.index(self.step) - 1, 0)
        return self.steps[idx]
