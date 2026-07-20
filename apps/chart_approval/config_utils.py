import json
import math
from typing import Any

import numpy as np
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from etl.config import OWIDEnv
from etl.grapher.model import Chart
from etl.http import session as http_session


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


def get_variable_max_year(variable_id: int, env: OWIDEnv) -> int | None:
    """Get the maximum year from a variable's data.

    Args:
        variable_id: Variable ID to fetch data for
        env: OWID environment configuration

    Returns:
        Maximum year in the data, or None if no data
    """
    url = env.indicator_data_url(variable_id)
    response = http_session.get(url)
    response.raise_for_status()
    data = response.json()

    if "years" in data and data["years"]:
        return max(data["years"])
    return None


def get_chart_config_with_hashes(
    chart_id: int, env: OWIDEnv, round_values: bool = True, use_max_year_hash: bool = False
) -> dict[str, Any]:
    """
    Get chart config by ID and replace variableId values with hashes of their API data.

    Args:
        chart_id: Chart ID to fetch
        env: OWID environment configuration
        round_values: If True, round numeric values to meaningful precision before hashing
        use_max_year_hash: If True, use only max year for hashing instead of full data

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
                    if use_max_year_hash:
                        max_year = get_variable_max_year(value, env)
                        new_obj[key] = str(max_year) if max_year is not None else "None"
                    else:
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

    response = http_session.get(url)
    response.raise_for_status()

    data = response.json()

    if round_values and "values" in data:
        data = data.copy()  # Don't modify original
        data["values"] = _round_values_intelligently(data["values"])

    return str(hash(json.dumps(data)))


def get_variable_data_with_entities(variable_id: int, env: OWIDEnv) -> tuple[dict[str, Any], dict[int, str]]:
    """Fetch a variable's data points and its entity id -> name mapping.

    Args:
        variable_id: Variable ID to fetch
        env: OWID environment configuration

    Returns:
        Tuple of (data dict with years/entities/values, entity id -> name mapping)
    """
    data_response = http_session.get(env.indicator_data_url(variable_id))
    data_response.raise_for_status()
    data = data_response.json()

    metadata_response = http_session.get(env.indicator_metadata_url(variable_id))
    metadata_response.raise_for_status()
    metadata = metadata_response.json()

    entity_names = {e["id"]: e["name"] for e in metadata.get("dimensions", {}).get("entities", {}).get("values", [])}

    return data, entity_names


def summarize_variable_data_diff(
    variable_id_a: int,
    env_a: OWIDEnv,
    variable_id_b: int,
    env_b: OWIDEnv,
    round_values: bool = True,
    max_examples: int = 10,
) -> dict[str, Any]:
    """Compare the actual data points of two variables (potentially different IDs/environments).

    Args:
        variable_id_a: First variable ID (e.g. staging)
        env_a: Environment to fetch variable_id_a from
        variable_id_b: Second variable ID (e.g. production)
        env_b: Environment to fetch variable_id_b from
        round_values: If True, round numeric values before comparing (ignore float noise)
        max_examples: Max number of example changes/additions/removals to include

    Returns:
        Dict summarizing the diff: counts of changed/added/removed points and examples of each.
    """
    data_a, entities_a = get_variable_data_with_entities(variable_id_a, env_a)
    data_b, entities_b = get_variable_data_with_entities(variable_id_b, env_b)

    values_a = _round_values_intelligently(data_a["values"]) if round_values else data_a["values"]
    values_b = _round_values_intelligently(data_b["values"]) if round_values else data_b["values"]

    map_a = {(year, entity): value for year, entity, value in zip(data_a["years"], data_a["entities"], values_a)}
    map_b = {(year, entity): value for year, entity, value in zip(data_b["years"], data_b["entities"], values_b)}

    keys_a = set(map_a)
    keys_b = set(map_b)
    common_keys = keys_a & keys_b

    def entity_name(entity_id: int) -> str:
        return entities_a.get(entity_id) or entities_b.get(entity_id) or str(entity_id)

    changed = []
    for year, entity_id in common_keys:
        value_a = map_a[(year, entity_id)]
        value_b = map_b[(year, entity_id)]
        if value_a != value_b:
            changed.append((entity_name(entity_id), year, value_a, value_b))

    def magnitude(item: tuple[str, int, Any, Any]) -> float:
        _, _, value_a, value_b = item
        if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
            return abs(value_b - value_a)
        return 0.0

    changed.sort(key=magnitude, reverse=True)

    only_in_a = sorted(entity_name(entity_id) + f" ({year})" for year, entity_id in (keys_a - keys_b))
    only_in_b = sorted(entity_name(entity_id) + f" ({year})" for year, entity_id in (keys_b - keys_a))

    return {
        "variable_id_a": variable_id_a,
        "variable_id_b": variable_id_b,
        "n_points_a": len(keys_a),
        "n_points_b": len(keys_b),
        "n_common": len(common_keys),
        "n_changed": len(changed),
        "n_only_a": len(only_in_a),
        "n_only_b": len(only_in_b),
        "all_changed": changed,
        "examples_changed": changed[:max_examples],
        "examples_only_a": only_in_a[:max_examples],
        "examples_only_b": only_in_b[:max_examples],
    }


def blank_variable_ids(obj: Any) -> Any:
    """Recursively replace every 'variableId' value with None.

    Lets us check whether two configs are identical except for which variable IDs they reference
    (e.g. after a version bump that minted new IDs, or a WDI cycle that added no new indicators).
    """
    if isinstance(obj, dict):
        return {key: (None if key == "variableId" else blank_variable_ids(value)) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [blank_variable_ids(item) for item in obj]
    else:
        return obj


def dimension_diff_within_tolerance(
    dim_diff: dict[str, Any],
    tolerance_pct: float,
    tolerance_abs_floor: float,
    max_changed_points: int,
    max_new_points: int = 1000,
) -> bool:
    """Check whether a dimension's data diff (from diff_chart_dimension_data) is small enough to auto-approve.

    A dimension passes if:
    - no points were removed relative to production (n_only_b == 0) — points missing from staging
      that existed in production are a coverage regression and always require manual review, however
      small max_changed_points/tolerance_pct are set
    - at most max_new_points points were added in staging (n_only_a, e.g. a new year of coverage) —
      expected from a routine update and given a generous allowance, but not unbounded: an
      unexpectedly large jump in coverage (e.g. an accidental variable swap that happens to look like
      "all new points" against the old one) can visibly change the chart and still deserves a human
      look, however small max_changed_points/tolerance_pct are set
    - at most max_changed_points points changed
    - every changed point's relative difference is within tolerance_pct, using tolerance_abs_floor as a
      floor on the denominator (and to short-circuit differences smaller than the floor) so tiny values
      near zero don't produce a misleadingly huge relative change

    Args:
        dim_diff: One entry from diff_chart_dimension_data's result
        tolerance_pct: Max relative change (%) allowed per changed point
        tolerance_abs_floor: Absolute floor used both to short-circuit negligible diffs and as the minimum
            denominator for the relative-change calculation
        max_changed_points: Max number of changed points allowed before requiring manual review
        max_new_points: Max number of newly-added points (present in staging only) allowed before
            requiring manual review

    Returns:
        True if the dimension's diff is within tolerance, False otherwise
    """
    if dim_diff["n_only_b"]:
        return False
    if dim_diff["n_only_a"] > max_new_points:
        return False
    if dim_diff["n_changed"] > max_changed_points:
        return False
    for _entity, _year, value_a, value_b in dim_diff["all_changed"]:
        if not isinstance(value_a, (int, float)) or not isinstance(value_b, (int, float)):
            return False
        diff = abs(value_b - value_a)
        if diff <= tolerance_abs_floor:
            continue
        denominator = max(abs(value_a), tolerance_abs_floor)
        if (diff / denominator) * 100 > tolerance_pct:
            return False
    return True


def diff_chart_dimension_data(
    chart_id: int,
    engine_a: Engine,
    env_a: OWIDEnv,
    engine_b: Engine,
    env_b: OWIDEnv,
    round_values: bool = True,
    max_examples: int = 10,
) -> list[dict[str, Any]]:
    """For a chart, compare the actual data behind each dimension (y/x/size/color) between two environments.

    Only dimensions whose underlying data actually differs are included in the result.

    Args:
        chart_id: Chart ID to inspect
        engine_a: DB engine for the first environment (e.g. staging)
        env_a: OWID environment configuration matching engine_a
        engine_b: DB engine for the second environment (e.g. production)
        env_b: OWID environment configuration matching engine_b
        round_values: If True, round numeric values before comparing
        max_examples: Max number of example changes to include per dimension

    Returns:
        List of per-dimension diff summaries (see summarize_variable_data_diff), each tagged with
        the dimension's "property" (y, x, size, color, ...).
    """
    config_a = get_chart_config(chart_id, engine_a)
    config_b = get_chart_config(chart_id, engine_b)

    # A property can appear more than once (e.g. a multi-series chart with several
    # `property: "y"` dimensions) -- keep every variable ID per property, not just the last
    # one a dict comprehension would happen to keep, or an earlier series' change could be
    # silently dropped from the comparison entirely.
    dimensions_a: dict[str, list[int]] = {}
    for d in config_a.get("dimensions", []):
        if "variableId" in d:
            dimensions_a.setdefault(d["property"], []).append(d["variableId"])
    dimensions_b: dict[str, list[int]] = {}
    for d in config_b.get("dimensions", []):
        if "variableId" in d:
            dimensions_b.setdefault(d["property"], []).append(d["variableId"])

    results = []
    for prop in dimensions_a.keys() & dimensions_b.keys():
        ids_a, ids_b = dimensions_a[prop], dimensions_b[prop]
        # A changed series count for this property is itself a structural change worth
        # manual review, not something to silently truncate away via zip().
        if len(ids_a) != len(ids_b):
            results.append(
                {
                    "property": prop,
                    "n_changed": 0,
                    "n_common": 0,
                    "n_only_a": len(ids_a),
                    "n_only_b": len(ids_b),
                    "examples_changed": [],
                    "variable_id_a": ids_a,
                    "variable_id_b": ids_b,
                }
            )
            continue

        for variable_id_a, variable_id_b in zip(ids_a, ids_b):
            diff = summarize_variable_data_diff(
                variable_id_a, env_a, variable_id_b, env_b, round_values=round_values, max_examples=max_examples
            )
            if diff["n_changed"] or diff["n_only_a"] or diff["n_only_b"]:
                diff["property"] = prop
                results.append(diff)

    return results


def get_chart_config(chart_id: int, engine: Engine) -> dict[str, Any]:
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
