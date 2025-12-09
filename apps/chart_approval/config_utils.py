import json
from typing import Any, Dict

import requests
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from etl.config import OWIDEnv
from etl.grapher.model import Chart


def get_chart_config_with_hashes(chart_id: int, env: OWIDEnv) -> Dict[str, Any]:
    """
    Get chart config by ID and replace variableId values with hashes of their API data.

    Args:
        chart_id: Chart ID to fetch
        env: OWID environment configuration

    Returns:
        Chart config dictionary with variableId values replaced by hashes
    """
    # Get config from database using ORM
    with Session(env.engine) as session:
        chart = session.get(Chart, chart_id)
        if not chart:
            raise ValueError(f"Chart with ID {chart_id} not found")

        config = chart.config

    # Remove version field
    if "version" in config:
        del config["version"]

    if "colorScale" in config and config["colorScale"] == {}:
        del config["colorScale"]

    if "map" in config and config["map"] == {"colorScale": {}}:
        del config["map"]

    # Remove map.columnSlug field if it exists
    if "map" in config and isinstance(config["map"], dict) and "columnSlug" in config["map"]:
        del config["map"]["columnSlug"]

    # Remove includeInTable field from all dimensions' display
    if "dimensions" in config and isinstance(config["dimensions"], list):
        for dimension in config["dimensions"]:
            if isinstance(dimension, dict) and "display" in dimension:
                if isinstance(dimension["display"], dict) and "includeInTable" in dimension["display"]:
                    dimension["display"].pop("includeInTable")

    # Find and replace all variableId values
    def replace_variable_ids(obj: Any) -> Any:
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                if key == "variableId" and isinstance(value, int):
                    # Get hash for this variable ID
                    new_obj[key] = get_variable_data_hash(value, env)
                else:
                    new_obj[key] = replace_variable_ids(value)
            return new_obj
        elif isinstance(obj, list):
            return [replace_variable_ids(item) for item in obj]
        else:
            return obj

    result = replace_variable_ids(config)
    # Ensure we return a Dict[str, Any] as expected
    if isinstance(result, dict):
        return result
    else:
        raise TypeError(f"Expected dict result, got {type(result)}")


def get_variable_data_hash(variable_id: int, env: OWIDEnv) -> str:
    """
    Get hash of variable data from OWID API.

    Args:
        variable_id: Variable ID to fetch data for
        env: OWID environment configuration

    Returns:
        Hash of the variable data
    """
    url = env.indicator_data_url(variable_id)

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    return str(hash(json.dumps(data)))


def get_chart_config(chart_id: int, engine: Engine) -> Dict[str, Any]:
    """
    Get chart config by ID without any modifications using ORM.

    Args:
        chart_id: Chart ID to fetch
        engine: SQLAlchemy engine for database connection

    Returns:
        Original chart config dictionary
    """
    with Session(engine) as session:
        chart = session.get(Chart, chart_id)
        if not chart:
            raise ValueError(f"Chart with ID {chart_id} not found")

        return chart.config
