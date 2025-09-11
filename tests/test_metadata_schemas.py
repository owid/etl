from pathlib import Path

import structlog
import yaml
from jsonschema import (
    Draft7Validator,
)
from jsonschema.exceptions import ValidationError
from yaml.loader import SafeLoader

from etl.files import read_json_schema
from etl.paths import SCHEMAS_DIR, SNAPSHOTS_DIR, STEPS_DATA_DIR

log = structlog.get_logger()

DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")
SNAPSHOT_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "snapshot-schema.json")


# only validate versions after this date
# bump this if we significantly change the schema
VALIDATE_AFTER = "2024-03-01"

# Excluded invalid metadata files, should be fixed if possible
EXCLUDE = [
    "garden/excess_mortality/latest/excess_mortality/excess_mortality.meta.yml",
    "meadow/who/latest/fluid.meta.yml",
]


# Override the default YAML loader to treat dates as strings
def construct_yaml_str(self, node):
    return self.construct_scalar(node)


def load_yaml_as_string(path):
    SafeLoader.add_constructor("tag:yaml.org,2002:timestamp", construct_yaml_str)
    with open(path, "r") as file:
        return yaml.load(file, Loader=SafeLoader)


def test_dataset_schemas():
    validator = Draft7Validator(DATASET_SCHEMA)
    validation_errors = []

    # Walk over all files in STEPS_DATA_DIR with *.meta.yml extension
    for meta_file_path in Path(STEPS_DATA_DIR).glob("**/*.meta.yml"):
        # extract version from path
        version = meta_file_path.relative_to(STEPS_DATA_DIR).parts[2]

        # Only validate versions after VALIDATE_AFTER
        if version != "latest" and version < VALIDATE_AFTER:
            continue

        # Exclude known invalid metadata files
        if any(ex in str(meta_file_path) for ex in EXCLUDE):
            continue

        # Ignore fasttrack and backport metadata
        if "fasttrack/" in str(meta_file_path) or "backport/" in str(meta_file_path):
            continue

        data = load_yaml_as_string(meta_file_path)

        # Ignore invalid `description` field, it's in too many latest datasets
        for tab in data.get("tables", {}).values():
            for ind in tab.get("variables", {}).values():
                if "description" in ind:
                    del ind["description"]

                # Ignore pinned schemas in presentation.grapher_config
                if "$schema" in ind.get("presentation", {}).get("grapher_config", {}):
                    del ind["presentation"]["grapher_config"]

        # Validate the loaded data against the schema
        try:
            validator.validate(data)
        except ValidationError as e:
            validation_errors.append((meta_file_path, e))

    # If there are validation errors, log summary and raise the first one
    if validation_errors:
        log.error("VALIDATION SUMMARY", error_count=len(validation_errors))
        for i, (file_path, error) in enumerate(validation_errors, 1):
            log.error("Validation error", index=i, file=str(file_path), message=error.message)

        # Raise the first error
        first_file, first_error = validation_errors[0]
        raise ValidationError(f"Validation error in file: {first_file}") from first_error


def test_snapshot_schemas():
    validator = Draft7Validator(SNAPSHOT_SCHEMA)

    for meta_file_path in Path(SNAPSHOTS_DIR).glob("**/*.dvc"):
        # extract version from etl/snapshots/namespace/version/snapshot_name.ext.dvc
        version = meta_file_path.parent.name

        # Only validate versions after VALIDATE_AFTER
        if version != "latest" and version < VALIDATE_AFTER:
            continue

        # Ignore fasttrack and backport metadata
        if "fasttrack/" in str(meta_file_path) or "backport/" in str(meta_file_path):
            continue

        data = load_yaml_as_string(meta_file_path)

        # Validate the loaded data against the schema
        try:
            validator.validate(data)
        except ValidationError as e:
            raise ValidationError(f"Validation error in file: {meta_file_path}") from e
