import re
from copy import deepcopy
from datetime import date
from datetime import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

import jsonref
import jsonschema
import streamlit as st
from pydantic import BaseModel
from typing_extensions import Self

from apps.utils.files import generate_step_to_channel
from apps.wizard.etl_steps.utils import ADD_DAG_OPTIONS, COOKIE_STEPS, SNAPSHOT_SCHEMA, remove_playground_notebook
from apps.wizard.utils import clean_empty_dict
from etl.files import ruamel_dump
from etl.helpers import write_to_dag_file
from etl.paths import DAG_DIR


def is_snake(s: str) -> bool:
    """Check that `s` is in snake case.

    First character is not allowed to be a number!
    """
    rex = r"[a-z][a-z0-9]+(?:_[a-z0-9]+)*"
    return bool(re.fullmatch(rex, s))


class StepForm(BaseModel):
    """Form abstract class."""

    errors: Dict[str, Any] = {}
    step_name: str

    def __init__(self: Self, **kwargs: str | int) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Construct parent class."""
        super().__init__(**kwargs)
        self.validate()

    @classmethod
    def filter_relevant_fields(cls: Type[Self], step_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter relevant fields from form."""
        return {k.replace(f"{step_name}.", ""): v for k, v in data.items() if k.startswith(f"{step_name}.")}

    @classmethod
    def from_state(cls: Type[Self]) -> Self:
        """Build object from session_state variables."""
        session_state = cast(Dict[str, Any], dict(st.session_state))
        data = cls.filter_relevant_fields(step_name=st.session_state["step_name"], data=session_state)
        return cls(**data)

    def validate(self: Self) -> None:  # type: ignore[reportIncompatibleMethodOverride]
        """Validate form fields."""
        raise NotImplementedError("Needs to be implemented in the child class!")

    @property
    def metadata(self: Self) -> None:
        """Get metadata as dictionary."""
        raise NotADirectoryError("Needs to be implemented in the child class!")

    def to_yaml(self: Self, path: Path) -> None:
        """Export form (metadata) to yaml file."""
        with open(path, "w") as f:
            assert self.metadata
            f.write(ruamel_dump(self.metadata))

    def validate_schema(self: Self, schema: Dict[str, Any], ignore_keywords: Optional[List[str]] = None) -> None:
        """Validate form fields against schema.

        Note that not all form fields are present in the schema (some do not belong to metadata, but are needed to generate the e.g. dataset URI)
        """
        if ignore_keywords == []:
            ignore_keywords = []
        # Validator
        validator = jsonschema.Draft7Validator(schema)
        # Plain JSON
        schema_full = jsonref.replace_refs(schema)
        # Process each error
        errors = sorted(validator.iter_errors(self.metadata), key=str)  # get all validation errors
        for error in errors:
            # Get error type
            error_type = error.validator
            if error_type not in {"required", "type", "pattern"}:
                raise Exception(f"Unknown error type {error_type} with message '{error.message}'")
            # Get field values
            values = self.get_invalid_field(error, schema_full)
            # Get uri of problematic field
            uri = error.json_path.replace("$.meta.", "")
            # Some fixes when error type is "required"
            if error_type == "required":
                # Get uri and values for the actual field!
                # Note that for errors that are of type 'required', this might contain the top level field. E.g., suppose 'origin.title' is required;
                # this requirement is defined at 'origin' level, hence error.schema_path will point to 'origin' and not 'origin.title'.
                field_name = self._get_required_field_name(error)
                uri = f"{uri}.{field_name}"
                values = values["properties"][field_name]
                if "errorMessage" not in values:
                    # print("DEBUG, replaced errormsg")
                    values["errorMessage"] = error.message.replace("'", "`")
            # Save error message
            if "errorMessage" in values:
                self.errors[uri] = values["errorMessage"]
            else:
                self.errors[uri] = error.message
            # print("DEBUG", uri, error.message, values["errorMessage"])

    def get_invalid_field(self: Self, error, schema_full) -> Any:
        """Get all key-values for the field that did not validate.

        Note that for errors that are of type 'required', this might contain the top level field. E.g., suppose 'origin.title' is required;
        this requirement is defined at 'origin' level, hence error.schema_path will point to 'origin' and not 'origin.title'.
        """
        # print("schema_path:", error.schema_path)
        # print("validator_value:", error.validator_value)
        # print("absolute_schema_path:", error.absolute_schema_path)
        # print("relative_path:", error.relative_path)
        # print("absolute_path:", error.absolute_path)
        # print("json_path:", error.json_path)
        queue = list(error.schema_path)[:-1]
        # print(queue)
        values = deepcopy(schema_full)
        for key in queue:
            values = values[key]
        return values

    def _get_required_field_name(self: Self, error):
        """Get required field name

        Required field names are defined by the field containing these. Hence, when there is an error, the path points to the top level field,
        not the actual one that is required and is missing.
        """
        rex = r"'(.*)' is a required property"
        field_name = re.findall(rex, error.message)[0]
        return field_name

    def check_required(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are not empty."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            # print(field_name, attr)
            if attr in ["", []]:
                self.errors[field_name] = f"`{field_name}` is a required property"

    def check_snake(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are in snake case."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            if not is_snake(attr):
                self.errors[field_name] = f"`{field_name}` must be in snake case"

    def check_is_version(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are in snake case."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            rex = r"^\d{4}-\d{2}-\d{2}$|^\d{4}$|^latest$"
            if not re.fullmatch(rex, attr):
                self.errors[field_name] = f"`{field_name}` must have format YYYY-MM-DD, YYYY or 'latest'"


class DataForm(StepForm):
    """express step form."""

    step_name: str = "data"

    # List of steps
    steps_to_create: List[str]
    # Common
    namespace: str
    namespace_custom: Optional[str] = None  # Custom
    short_name: str
    version: str
    dag_file: str
    is_private: bool
    add_to_dag: bool
    # Only in Meadow
    snapshot_dependencies: Optional[List[str]] = None
    # Only in Garden
    update_period_days: Optional[int] = None
    topic_tags: Optional[List[str]] = None
    update_period_date: Optional[date] = None  # Custom
    notebook: Optional[bool] = None
    # Extra steps
    dependencies_extra: Dict[str, Any]

    def __init__(self: Self, **data: Any) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Construct class."""
        data["add_to_dag"] = data["dag_file"] != ADD_DAG_OPTIONS[0]

        # Handle custom namespace
        if ("namespace_custom" in data) and data["namespace_custom"] is not None:
            data["namespace"] = str(data["namespace_custom"])

        # Handle update_period_days. Obtain from date.
        if "update_period_date" in data:
            assert isinstance(data["update_period_date"], date)
            update_period_days = (data["update_period_date"] - date.today()).days

            data["update_period_days"] = update_period_days

        # Extra options
        data["is_private"] = True if "private" in data["extra_options"] else False
        data["notebook"] = True if "notebook" in data["extra_options"] else False

        data["dependencies_extra"] = {
            "garden": data.get("dependencies_extra_garden"),
            "grapher": data.get("dependencies_extra_grapher"),
        }
        # st.write(data)
        super().__init__(**data)  # type: ignore

    def validate(self: Self) -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # Default common checks
        fields_required = ["namespace", "short_name", "version"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["version"]

        # Extra checks for particular steps
        if "meadow" in self.steps_to_create:
            fields_required += ["snapshot_dependencies"]
        if "garden" in self.steps_to_create:
            fields_required += ["topic_tags"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # Check tags
        if "garden" in self.steps_to_create:
            assert isinstance(self.topic_tags, list), "topic_tags must be a list! Should have been ensured actually!"
            if (len(self.topic_tags) > 1) and ("Uncategorized" in self.topic_tags):
                self.errors["topic_tags"] = "If you choose multiple tags, you cannot choose `Uncategorized`."

    @property
    def base_step_name(self) -> str:
        """namespace/version/short_name"""
        return f"{self.namespace}/{self.version}/{self.short_name}"

    @property
    def snapshot_names_with_extension(self) -> List[str]:
        """Get snapshot names with extension."""
        assert "meadow" in self.steps_to_create, "Snapshot names are only needed for meadow steps!"
        assert self.snapshot_dependencies is not None, "Snapshot dependencies must be present!"
        return [s.split("/")[-1] for s in self.snapshot_dependencies]

    def step_uri(self, channel: str) -> str:
        """Get step URI."""
        match channel:
            case "meadow":
                return f"data{self.private_suffix}://meadow/{self.base_step_name}"
            case "garden":
                return f"data{self.private_suffix}://garden/{self.base_step_name}"
            case "grapher":
                return f"data{self.private_suffix}://grapher/{self.base_step_name}"
            case _:
                raise ValueError(f"Channel `{channel}` not recognized.")

    @property
    def meadow_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://meadow/{self.base_step_name}"

    @property
    def garden_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://garden/{self.base_step_name}"

    @property
    def grapher_step_uri(self) -> str:
        """Get garden step name."""
        return f"data{self.private_suffix}://grapher/{self.base_step_name}"

    @property
    def dag_path(self) -> Path:
        """Get DAG path."""
        return DAG_DIR / self.dag_file

    @property
    def private_suffix(self) -> str:
        return "-private" if self.is_private else ""

    @property
    def topic_tags_export(self):
        ## HOTFIX 1: filter topic_tags if empty
        if self.topic_tags is None or self.topic_tags == []:
            topic_tags = ""
        ## HOTFIX 2: For some reason, when using cookiecutter only the first element in the list is taken?
        ## Hence we need to convert the list to an actual string
        else:
            topic_tags = "- " + "\n- ".join(self.topic_tags)
        return topic_tags

    def to_dict(self, channel: str):
        common = {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "version": self.version,
            "add_to_dag": self.add_to_dag,
            "dag_file": self.dag_file,
            "is_private": self.is_private,
            "channel": "meadow",
        }
        match channel:
            case "meadow":
                common.update(
                    {
                        "snapshot_names_with_extension": self.snapshot_names_with_extension,
                    }
                )
            case "garden":
                common.update(
                    {
                        "meadow_version": self.version,
                        "update_period_days": self.update_period_days,
                        "topic_tags": self.topic_tags_export,
                    }
                )
            case "grapher":
                common.update(
                    {
                        "garden_version": self.version,
                    }
                )
            case _:
                raise ValueError(f"Channel `{channel}` not recognized.")
        return common

    def create_files(self, channel: str) -> List[Dict[str, Any]]:
        # print(self.snapshot_names_with_extension)
        # Generate files
        DATASET_DIR = generate_step_to_channel(cookiecutter_path=COOKIE_STEPS[channel], data=self.to_dict(channel))
        # Remove playground notebook if not needed
        if channel != "garden" or not self.notebook:
            remove_playground_notebook(DATASET_DIR)

        # Add to generated files
        generated_files = [
            {
                "path": DATASET_DIR / (self.short_name + ".py"),
                "language": "python",
                "channel": channel,
            }
        ]
        if channel == "garden":
            generated_files.append(
                {
                    "path": DATASET_DIR / (self.short_name + ".meta.yml"),
                    "language": "yaml",
                    "channel": "garden",
                }
            )
        return generated_files

    def add_steps_to_dag(self) -> str:
        if self.add_to_dag:
            # Get dag
            dag = self.dag
            # Get comment
            default_comment = "\n#\n# TODO: add step name (just something recognizable)\n#"
            if "meadow" in self.steps_to_create:
                # Load metadata from Snapshot
                # snap = Snapshot(self.base_snapshot_name)
                # assert snap.metadata.origin is not None, "Origin metadata must be present!"
                # comment = f"#\n#{snap.metadata.origin.title} - {snap.metadata.origin.producer}\n#\n#"
                comments = {
                    self.step_uri("meadow"): default_comment,
                }
            elif "garden" in self.steps_to_create:
                comments = {
                    self.step_uri("garden"): default_comment,
                }
            elif "grapher" in self.steps_to_create:
                comments = {
                    self.step_uri("grapher"): default_comment,
                }
            else:
                comments = None
            # Add to DAG
            write_to_dag_file(dag_file=self.dag_path, dag_part=dag, comments=comments)
            return ruamel_dump({"steps": dag})
        else:
            return ""

    @property
    def dag(self) -> Dict[str, Any]:
        dag = {}

        # Meadow dependencies (snapshots)
        if "meadow" in self.steps_to_create:
            dag[self.step_uri("meadow")] = self.snapshot_dependencies

        # Garden, Grapher dependencies. Default + Extra.
        channels_all = ["meadow", "garden", "grapher"]
        for i, channel in enumerate(channels_all[1:]):
            if channel in self.steps_to_create:
                dag[self.step_uri(channel)] = [self.step_uri(channels_all[i])]
                if self.dependencies_extra[channel] is not None:
                    dag[self.step_uri(channel)] += self.dependencies_extra[channel]
        return dag


class SnapshotForm(StepForm):
    """Interface for snapshot form."""

    step_name: str = "snapshot"

    # config
    namespace: str
    snapshot_version: str
    short_name: str
    file_extension: str
    is_private: bool
    dataset_manual_import: bool

    # origin
    title: str
    description: str
    title_snapshot: str
    description_snapshot: str
    origin_version: str
    date_published: str
    producer: str
    citation_full: str
    attribution: Optional[str]
    attribution_short: str
    url_main: str
    url_download: str
    date_accessed: str

    # license
    license_url: str
    license_name: str

    def __init__(self: Self, **data: str | int) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Construct form."""
        # Change name for certain fields (and remove old ones)
        data["license_url"] = data["origin.license.url"]
        data["origin_version"] = data["origin.version_producer"]
        data["dataset_manual_import"] = data["local_import"]

        # Handle custom namespace
        if "namespace_custom" in data:
            data["namespace"] = str(data["namespace_custom"])

        # Handle custom license
        if "origin.license.name_custom" in data:
            data["license_name"] = data["origin.license.name_custom"]
        else:
            data["license_name"] = data["origin.license.name"]

        # Remove unused fields
        data = {k: v for k, v in data.items() if k not in ["origin.license.url", "origin.license.name"]}
        # Remove 'origin.' prefix from keys
        data = {k.replace("origin.", ""): v for k, v in data.items()}

        # Init object (includes schema validation)
        super().__init__(**data)

        # Handle custom attribution
        if not self.errors:
            if "attribution_custom" in data:
                self.attribution = str(data["attribution_custom"])
            else:
                self.attribution = self.parse_attribution(data)

    def parse_attribution(self: Self, data: Dict[str, str | int]) -> str | None:
        """Parse the field attribution.

        By default, the field attribution contains the format of the attribution, not the actual attribution. This function
        renders the actual attribution.
        """
        attribution_template = cast(str, data["attribution"])
        if attribution_template == "{producer} ({year})":
            return None
        data_extra = {
            "year": dt.strptime(str(data["date_published"]), "%Y-%m-%d").year,
            # "version_producer": data["origin_version"],
        }
        attribution = attribution_template.format(**data, **data_extra).replace("  ", " ")
        return attribution

    def validate(self: "SnapshotForm") -> None:
        """Check that fields in form are valid.

        - Add error message for each field (to be displayed in the form).
        - Return True if all fields are valid, False otherwise.
        """
        # 1) Validate using schema
        # This only applies to SnapshotMeta fields
        self.validate_schema(SNAPSHOT_SCHEMA, ["meta"])

        # 2) Check other fields (non meta)
        fields_required = ["namespace", "snapshot_version", "short_name", "file_extension"]
        fields_snake = ["namespace", "short_name"]
        fields_version = ["snapshot_version"]

        self.check_required(fields_required)
        self.check_snake(fields_snake)
        self.check_is_version(fields_version)

        # License
        if self.license_name == "":
            self.errors["origin.license.name_custom"] = "Please introduce the name of the custom license!"

        # Attribution
        if self.attribution == "":
            self.errors["origin.attribution_custom"] = "Please introduce the name of the custom attribute!"

    @property
    def metadata(self: Self) -> Dict[str, Any]:  # type: ignore[reportIncompatibleMethodOverride]
        """Define metadata for easy YAML-export."""
        license_field = {
            "name": self.license_name,
            "url": self.license_url,
        }
        meta = {
            "meta": {
                "origin": {
                    "title": self.title,
                    "description": self.description.replace("\n", "\n      "),
                    "title_snapshot": self.title_snapshot,
                    "description_snapshot": self.description_snapshot.replace("\n", "\n      "),
                    "producer": self.producer,
                    "citation_full": self.citation_full,
                    "attribution": self.attribution,
                    "attribution_short": self.attribution_short,
                    "version_producer": self.origin_version,
                    "url_main": self.url_main,
                    "url_download": self.url_download,
                    "date_published": self.date_published,
                    "date_accessed": self.date_accessed,
                    "license": license_field,
                },
                "is_public": not self.is_private,
            }
        }
        meta = cast(Dict[str, Any], clean_empty_dict(meta))
        return meta
