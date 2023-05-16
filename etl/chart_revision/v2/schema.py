import copy
from typing import Any, Dict, List

import requests
from jsonschema import Draft202012Validator, validate, validators
from sqlmodel import Session, select

import etl.grapher_model as gm
from etl.db import get_engine

# Version of the schema
SCHEMA_VERSION = "003"


def get_schema_chart_config() -> Dict[str, Any]:
    """Get the schema of a chart configuration.

    Version of the schema used is defined by variable `SCHEMA_VERSION`. More details on available versions can be found
    at https://github.com/owid/owid-grapher/tree/master/packages/%40ourworldindata/grapher/src/schema.

    Returns
    -------
    Dict[str, Any]
        Schema of a chart configuration.
    """
    return requests.get(
        f"https://files.ourworldindata.org/schemas/grapher-schema.{SCHEMA_VERSION}.json",
        timeout=10,
    ).json()


def validate_chart_config(config: Dict[str, Any]) -> None:
    """Validate the schema of a chart configuration."""
    schema = get_schema_chart_config()
    validate(config, schema)


def validate_chart_config_and_set_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    """Add properties with default values to a config file, if they are not present.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """

    def _extend_with_set_default(validator_class):  # type: ignore
        validate_properties = validator_class.VALIDATORS["properties"]

        def _set_defaults(validator, properties, instance, schema):  # type: ignore
            for property, subschema in properties.items():
                if "default" in subschema:
                    instance.setdefault(property, subschema["default"])

            for error in validate_properties(
                validator,
                properties,
                instance,
                schema,
            ):
                yield error

        return validators.extend(
            validator_class,
            {"properties": _set_defaults},
        )

    # Create custom validation object
    DefaultSetterValidatingValidator = _extend_with_set_default(Draft202012Validator)
    # Get schema
    schema = get_schema_chart_config()
    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    DefaultSetterValidatingValidator(schema).validate(config_new)
    return config_new


def validate_chart_config_and_remove_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    """Remove properties with values equal to their defaults from schema.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """

    def _extend_with_remove_default(validator_class):  # type: ignore
        validate_properties = validator_class.VALIDATORS["properties"]

        def _set_defaults(validator, properties, instance, schema):  # type: ignore
            for property, subschema in properties.items():
                if "default" in subschema:
                    if subschema["default"] == instance[property]:
                        del instance[property]

            for error in validate_properties(
                validator,
                properties,
                instance,
                schema,
            ):
                yield error

        return validators.extend(
            validator_class,
            {"properties": _set_defaults},
        )

    # Create custom validation object
    DefaultDeleteValidatingValidator = _extend_with_remove_default(Draft202012Validator)
    # Get schema
    schema = get_schema_chart_config()
    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    DefaultDeleteValidatingValidator(schema).validate(config_new)
    return config_new
