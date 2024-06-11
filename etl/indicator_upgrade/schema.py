import copy
from typing import Any, Dict, Optional

import requests
from jsonschema import Draft202012Validator, validate, validators
from structlog import get_logger

# Version of the schema
SCHEMA_VERSION = "003"
# Logger
log = get_logger()


def get_schema_chart_config() -> Dict[str, Any]:
    """Get the schema of a chart configuration.

    Version of the schema used is defined by variable `SCHEMA_VERSION`. More details on available versions can be found
    at https://github.com/owid/owid-grapher/tree/master/packages/%40ourworldindata/grapher/src/schema.

    Returns
    -------
    Dict[str, Any]
        Schema of a chart configuration.
    """
    # import json
    # path = "~/repos/owid-grapher/packages/@ourworldindata/grapher/src/schema"
    # path = f"{path}/grapher-schema.{SCHEMA_VERSION}.json"
    # print(path)
    # with open(path, "r") as f:
    #     return json.load(f)
    return requests.get(
        f"https://files.ourworldindata.org/schemas/grapher-schema.{SCHEMA_VERSION}.json",
        timeout=20,
    ).json()


def validate_chart_config(config: Dict[str, Any]) -> None:
    """Validate the schema of a chart configuration."""
    schema = get_schema_chart_config()
    validate(config, schema)


def validate_chart_config_and_set_defaults(
    config: Dict[str, Any], schema: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Add properties with default values to a config file, if they are not present.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.
    schema: Dict[str, Any]
        JSON-like object. Schema of the chart configuration. If none is provided, the latest schema is downloaded.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """
    log.info("schema: validating schema and adding defaults")

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
    if schema is None:
        schema = get_schema_chart_config()
    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    try:
        DefaultSetterValidatingValidator(schema).validate(config_new)
    except Exception as e:
        raise Exception(f"Could not validate schema for chart {config['id']}: {e}")
    # Add minTime if not set (no default provided in schema)
    # Kinda hacky
    if config_new["type"] not in {"StackedDiscreteBar", "Marimekko", "DiscreteBar"}:
        if "minTime" not in config_new:
            config_new["minTime"] = "earliest"
    return config_new


def fix_errors_in_schema(config: Dict[str, Any]) -> Dict[str, Any]:
    """Fix common errors in schema and tries to catch up with latest schema version."""
    config_new = copy.deepcopy(config)
    if "map" in config_new:
        assert "variableId" not in config_new["map"], "map.variableId has been deprecated by map.columnSlug"
    if ("timelineMaxTime" in config_new) and (config_new["timelineMaxTime"] is None):
        del config_new["timelineMaxTime"]
    if ("timelineMinTime" in config_new) and (config_new["timelineMinTime"] is None):
        del config_new["timelineMinTime"]
    return config_new


def validate_chart_config_and_remove_defaults(
    config: Dict[str, Any], schema: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Remove properties with values equal to their defaults from schema.

    Parameters
    ----------
    config : Dict[str, Any]
        JSON-like object. Typically the chart configuration field.
    schema: Dict[str, Any]
        JSON-like object. Schema of the chart configuration. If none is provided, the latest schema is downloaded.

    Returns
    -------
    Dict[str, Any]
        Updated object, with defaults set.
    """
    log.info("schema: validating schema and removing defaults")

    def _extend_with_remove_default(validator_class):  # type: ignore
        validate_properties = validator_class.VALIDATORS["properties"]

        def _set_defaults(validator, properties, instance, schema):  # type: ignore
            for property, subschema in properties.items():
                is_required = property in (schema or {}).get("required", [])
                if "default" in subschema:
                    if not is_required and subschema["default"] == instance[property]:
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
    if schema is None:
        schema = get_schema_chart_config()
    # Validate and update config with defaults
    config_new = copy.deepcopy(config)
    DefaultDeleteValidatingValidator(schema).validate(config_new)
    return config_new
