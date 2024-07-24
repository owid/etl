import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

import jsonref
import jsonschema
import streamlit as st
from pydantic import BaseModel
from typing_extensions import Self

from etl.files import ruamel_dump


class StepForm(BaseModel):
    """Form abstract class."""

    errors: Dict[str, Any] = {}
    step_name: str

    def __init__(self: Self, **kwargs: str | int) -> None:
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
        # st.write(data)
        return cls(**data)

    def validate(self: Self) -> None:
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
            print("DEBUG", uri, error.message, values["errorMessage"])

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
            print(field_name, attr)
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


def is_snake(s: str) -> bool:
    """Check that `s` is in snake case.

    First character is not allowed to be a number!
    """
    rex = r"[a-z][a-z0-9]+(?:_[a-z0-9]+)*"
    return bool(re.fullmatch(rex, s))
