import json
import math
from typing import Any, Dict

import numpy as np
import requests
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from etl.config import OWIDEnv
from etl.grapher.model import Chart


def round_to_n_sig_figs(x: float, n: int) -> float:
    """Round a number to n significant figures.

    Args:
        x: Number to round
        n: Number of significant figures

    Returns:
        Rounded number
    """
    if x == 0:
        return 0
    return round(x, -int(math.floor(math.log10(abs(x)))) + (n - 1))


def _round_values_intelligently(values: list[float]) -> list[float]:
    """Round values to meaningful precision based on data characteristics.

    Strategy:
    1. Detect if values look like percentages (0-100 range)
    2. Detect scale (order of magnitude)
    3. Round to appropriate decimal places or significant figures

    Args:
        values: List of numeric values (may contain None)

    Returns:
        List of rounded values
    """
    # Filter out None/null values and non-numeric values for analysis
    numeric_values = [v for v in values if v is not None and isinstance(v, (int, float))]
    if not numeric_values:
        return values

    arr = np.array(numeric_values)
    value_min = np.min(arr)
    value_max = np.max(arr)
    value_range = value_max - value_min

    # Heuristic 1: Percentages (0-100 range)
    if value_min >= 0 and value_max <= 100 and value_range > 1:
        # Round to 2 decimal places (e.g., 97.88%)
        decimals = 2

    # Heuristic 2: Very small values (scientific notation territory)
    elif value_max < 0.01:
        # Use 6 significant figures
        return [round_to_n_sig_figs(v, 6) if v is not None else v for v in values]

    # Heuristic 3: Large values (thousands, millions)
    elif value_max > 1000:
        # Use 5 significant figures (preserves 10,000 → 10,000 but rounds 10,000.123 → 10,000)
        return [round_to_n_sig_figs(v, 5) if v is not None else v for v in values]

    # Heuristic 4: Values between 0.01 and 1000
    else:
        # Round to 4 decimal places (good for rates, ratios)
        decimals = 4

    # Apply decimal rounding (only to numeric values)
    return [round(v, decimals) if v is not None and isinstance(v, (int, float)) else v for v in values]


def get_chart_config_with_hashes(chart_id: int, env: OWIDEnv, round_values: bool = True) -> Dict[str, Any]:
    """
    Get chart config by ID and replace variableId values with hashes of their API data.

    Args:
        chart_id: Chart ID to fetch
        env: OWID environment configuration
        round_values: If True, round numeric values to meaningful precision before hashing

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
                    new_obj[key] = get_variable_data_hash(value, env, round_values=round_values)
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


def get_variable_data_hash(variable_id: int, env: OWIDEnv, round_values: bool = True) -> str:
    """
    Get hash of variable data from OWID API.

    Args:
        variable_id: Variable ID to fetch data for
        env: OWID environment configuration
        round_values: If True, round numeric values to meaningful precision before hashing

    Returns:
        Hash of the variable data (potentially rounded)
    """
    url = env.indicator_data_url(variable_id)

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    if round_values and "values" in data:
        data = data.copy()  # Don't modify original
        data["values"] = _round_values_intelligently(data["values"])

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
