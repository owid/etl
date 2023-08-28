import argparse
import datetime as dt
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

import jsonschema
import ruamel.yaml
import streamlit as st
import yaml
from cookiecutter.main import cookiecutter
from owid import walden
from owid.catalog.utils import validate_underscore
from pydantic import BaseModel

from etl import config
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

# Get current directory
CURRENT_DIR = Path(__file__).parent

# Phases accepted
PHASES = Literal["all", "snapshot", "meadow", "garden", "grapher"]


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


class classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


class AppState:
    """Management of state variables shared across different apps."""

    steps: List[str] = ["snapshot", "meadow", "garden", "grapher", "explorers"]

    def __init__(self: "AppState") -> "AppState":
        """Construct variable."""
        self.step = st.session_state["step_name"]
        self._init_steps()

    def _init_steps(self: "AppState") -> None:
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
                            "snapshot_version": DATE_TODAY,
                            "origin.date_accessed": DATE_TODAY,
                        },
                    }

    def _check_step(self: "AppState") -> None:
        """Check that the value for step is valid."""
        if self.step is None or self.step not in self.steps:
            raise ValueError(f"Step {self.step} not in {self.steps}.")

    def get_variables_of_step(self: "AppState") -> Dict[str, Any]:
        """Get variables of a specific step.

        Variables are assumed to have keys `step.NAME`, based on the keys given in the widgets within a form.
        """
        return {k: v for k, v in st.session_state.items() if k.startswith(f"{self.step}.")}

    def update(self: "AppState") -> None:
        """Update global variables of step.

        This is expected to be called when submitting the step's form.
        """
        self._check_step()
        print(f"Updating {self.step}...")
        st.session_state["steps"][self.step] = self.get_variables_of_step()

    def update_from_form(self, form: Dict[str, Any]) -> None:
        self._check_step()
        st.session_state["steps"][self.step] = form.dict()

    @property
    def state_step(self: "AppState") -> Dict[str, Any]:
        """Get state variables of step."""
        self._check_step()
        return st.session_state["steps"][self.step]

    def default_value(
        self: "AppState", key: str, previous_step: Optional[str] = None, default_last: Optional[Any] = ""
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
        value_step = self.state_step.get(key)
        if value_step:
            return value_step
        # (2) If none, check if previous step has a value and use that one, otherwise (3) use empty string.
        key = key.replace(f"{self.step}.", f"{self.previous_step}.")
        return st.session_state["steps"][self.previous_step].get(key, default_last)

    def display_error(self: "AppState", key: str) -> None:
        """Get error message for a given key."""
        if "errors" in self.state_step:
            print(key)
            if msg := self.state_step.get("errors", {}).get(key, ""):
                st.error(msg)

    @property
    def previous_step(self: "AppState") -> str:
        """Get the name of the previous step.

        E.g. 'snapshot' is the step prior to 'meadow', etc.
        """
        self._check_step()
        idx = max(self.steps.index(self.step) - 1, 0)
        return self.steps[idx]

    def st_widget(
        self: "AppState", st_widget: Callable, default_last: Optional[str] = "", **kwargs: Dict[str, Any]
    ) -> None:
        """Wrap a streamlit widget with a default value."""
        key = kwargs["key"]
        # Get default value (either from previous edits, or from previous steps)
        default_value = self.default_value(key, default_last=default_last)
        # Change key name, to be stored it in general st.session_state
        kwargs["key"] = f"{self.step}.{key}"
        # Default value for selectbox (and other widgets with selectbox-like behavior)
        if "options" in kwargs:
            index = kwargs["options"].index(default_value) if default_value in kwargs["options"] else 0
            kwargs["index"] = index
        # Default value for other widgets (if none is given)
        elif "value" not in kwargs:
            kwargs["value"] = default_value

        # Create widget
        widget = st_widget(**kwargs)
        # Show error message
        self.display_error(key)
        return widget

    @classproperty
    def args(cls: "AppState") -> argparse.Namespace:
        """Get arguments passed from command line."""
        if "args" in st.session_state:
            return st.session_state["args"]
        else:
            parser = argparse.ArgumentParser(description='This app lists animals')
            parser.add_argument('--phase')
            parser.add_argument('--run-checks', action='store_true')
            parser.add_argument('--dummy-data', action='store_true')
            args = parser.parse_args()
            st.session_state["args"] = args
        return st.session_state["args"]


class StepForm(BaseModel):
    """Form abstract class."""

    errors: Dict[str, Any] = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.validate()

    @classmethod
    def filter_relevant_fields(cls: "StepForm", step_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter relevant fields from form."""
        return {k.replace(f"{step_name}.", ""): v for k, v in data.items() if k.startswith(f"{step_name}.")}

    @classmethod
    def from_state(cls: "StepForm", validate: bool = True) -> "StepForm":
        """Build object from session_state variables."""
        data = cls.filter_relevant_fields(step_name=st.session_state["step_name"], data=st.session_state)
        return cls(**data)

    def validate(self):
        raise NotImplementedError("Needs to be implemented in the child class!")

    @property
    def metadata(self):
        raise NotADirectoryError("Needs to be implemented in the child class!")

    def to_yaml(self, path: Path) -> None:
        with open(path, "w") as f:
            ruamel.yaml.dump(self.metadata, f, Dumper=ruamel.yaml.RoundTripDumper)

    def validate_schema(self, schema_path: str, ignore_keywords: List[str] = []):
        validator = jsonschema.Draft7Validator(schema_path)
        errors = sorted(validator.iter_errors(self.metadata), key=str)  # get all validation errors
        for error in errors:
            error_type = error.schema_path[-1]
            uri = [ll for ll in error.schema_path if ll not in ["properties"] + ignore_keywords]
            uri = uri[:-1]
            # required but missing fields
            if error_type == "required":
                rex = r"'(.*)' is a required property"
                uri += [re.findall(rex, error.message)[0]]
                uri = ".".join(uri)
                self.errors[uri] = f"`{uri}` field is required!"
            # wrong types
            elif error_type == "type":
                uri = ".".join(uri)
                self.errors[uri] = f"Invalid type for field `{uri}`!"
            elif error_type == "pattern":
                uri = ".".join(uri)
                self.errors[uri] = f"Invalid format of field `{uri}`!"
            # unknown validation error
            else:
                raise Exception(f"Unknown error type {error_type} with message {error.message}")

    def check_required(self, fields_names: List[str]):
        """Check that all fields in `fields_names` are not empty."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            if attr == "":
                self.errors[field_name] = f"{field_name} cannot be empty"

    def check_snake(self, fields_names: List[str]):
        """Check that all fields in `fields_names` are in snake case."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            snake = is_snake(attr)
            if snake:
                self.errors[field_name] = f"{field_name} must be in snake case"


def is_snake(s: str) -> bool:
    """Check that `s` is in snake case.

    First character is not allowed to be a number!
    """
    rex = r"[a-z][a-z0-9]+(?:_[a-z0-9]+)*"
    return bool(re.fullmatch(rex, s))


def extract(error_message: str):
    """Get field name that caused the error."""
    rex = r"'(.*)' is a required property"
    return re.findall(rex, error_message)[0]


def config_style_html():
    st.markdown(
        """
    <style>
    .streamlit-expanderHeader {
        font-size: x-large;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def preview_file(file_path: str, language: str = "python") -> None:
    """Preview file in streamlit."""
    with open(file_path, "r") as f:
        code = f.read()
    with st.expander(f"File: `{file_path}`", expanded=False):
        st.code(code, language=language)


def preview_dag_additions(dag_content, dag_path):
    if dag_content:
        with st.expander(f"File: `{dag_path}`", expanded=False):
            st.code(dag_content, "yaml")


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / f"{st.session_state['step_name']}.md", "r") as f:
        return f.read()


def _check_env() -> bool:
    """Check if environment variables are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?")

    if ok:
        st.success(("`.env` configured correctly"))
    return ok


def _show_environment() -> None:
    """Show environment variables."""
    st.info(
        f"""
    **Environment variables**:

    ```
    GRAPHER_USER_ID: {config.GRAPHER_USER_ID}
    DB_USER: {config.DB_USER}
    DB_NAME: {config.DB_NAME}
    DB_HOST: {config.DB_HOST}
    ```
    """
    )


def clean_empty_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove empty values from dict.

    REference: https://stackoverflow.com/a/27974027/5056599
    """
    if isinstance(d, dict):
        return {k: v for k, v in ((k, clean_empty_dict(v)) for k, v in d.items()) if v}
    if isinstance(d, list):
        return [v for v in map(clean_empty_dict, d) if v]
    return d
