"""Utils

NOTE: Should not import from any other submodule in etl.collection.
"""

import re
from collections import defaultdict
from copy import deepcopy
from itertools import product
from string import Formatter
from typing import Any, Dict, List, Set, Tuple

from deprecated import deprecated
from owid.catalog import Dataset
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from etl.collection.exceptions import ParamKeyError
from etl.config import OWID_ENV, OWIDEnv
from etl.db import read_sql
from etl.files import yaml_dump
from etl.paths import DATA_DIR

CHART_DIMENSIONS = ["y", "x", "size", "color"]
INDICATORS_SLUG = "indicator"


# combine
def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


# common, model.core
def map_indicator_path_to_id(catalog_path: str, owid_env: OWIDEnv | None = None) -> str | int:
    # Check if given path is actually an ID
    if str(catalog_path).isdigit():
        return catalog_path

    # Get ID, assuming given path is a catalog path
    if owid_env is None:
        engine = OWID_ENV.engine
    else:
        engine = owid_env.engine
    with Session(engine) as session:
        db_indicator = gm.Variable.from_id_or_path(session, catalog_path)
        assert db_indicator.id is not None
        return db_indicator.id


# .load_table_names_from_dependencies
def load_dataset_from_step(step: str) -> Dataset:
    uri = re.sub(r"^(data|data-private)://", "", step)
    # TODO: read no metadata
    return Dataset(DATA_DIR / uri)


# .has_duplicate_table_names
def load_table_names_from_dependencies(dependencies: Set[str]) -> List[str]:
    table_names = []
    for uri in dependencies:
        ds = load_dataset_from_step(uri)
        for table_name in ds.table_names:
            table_names.append(table_name)
    return table_names


# core.create
def has_duplicate_table_names(dependencies: Set[str]) -> bool:
    table_names = load_table_names_from_dependencies(dependencies)
    return len(table_names) != len(set(table_names))


# core.create
def get_tables_by_name_mapping(dependencies: Set[str]) -> Dict[str, List[str]]:
    """Dictionary mapping table short name to table object.

    Note that the format is {"table_name": [table_uri], ...}. This is because there could be collisions where multiple table names are mapped to the same table (e.g. two datasets could have a table with the same name).
    """
    tb_name_to_tb = defaultdict(list)

    for dep in dependencies:
        ## Ignore non-grapher dependencies
        if not re.match(r"^(data|data-private)://grapher/", dep):
            continue

        ds = load_dataset_from_step(dep)
        for table_name in ds.table_names:
            tb = ds.read(table_name, load_data=False)
            assert tb.m.dataset is not None, f"Dataset not found for table {table_name}"
            table_uri = f"{tb.m.dataset.uri}/{table_name}"
            # Add table -> uri
            tb_name_to_tb[table_name].append(table_uri)
            # Add dataset -> uri
            tb_name_to_tb[f"{ds.m.short_name}/{table_name}"].append(table_uri)

    return tb_name_to_tb


# model.core
def validate_indicators_in_db(indicators, engine):
    """Check that indicators are in DB!"""
    q = """
    select
        id,
        catalogPath
    from variables
    where catalogPath in %(indicators)s
    """
    df = read_sql(q, engine, params={"indicators": tuple(indicators)})
    missing_indicators = set(indicators) - set(df["catalogPath"])
    if missing_indicators:
        raise ValueError(f"Missing indicators in DB: {missing_indicators}")


# .get_complete_dimensions_filter
def expand_combinations(dim_dict):
    # Normalize all values to lists (in case they are sets)
    keys = list(dim_dict)
    values = [list(dim_dict[k]) for k in keys]
    return [dict(zip(keys, combo)) for combo in product(*values)]


# model.core
def get_complete_dimensions_filter(
    dimensions_available: Dict[str, Set[str]], dimensions_filter: Dict[str, Any]
) -> List[Dict[str, str]]:
    """Given a dimension filter"""
    dimensions_filter_complete = {}

    for dim, choices in dimensions_available.items():
        if dim not in dimensions_filter:
            dimensions_filter_complete[dim] = choices
        else:
            if isinstance(dimensions_filter[dim], str):
                assert (
                    dimensions_filter[dim] in choices
                ), f"Choice {dimensions_filter[dim]} not found for dimension {dim}!"
                dimensions_filter_complete[dim] = [dimensions_filter[dim]]
            elif isinstance(dimensions_filter[dim], list):
                assert all(
                    choice in choices for choice in dimensions_filter[dim]
                ), f"Choices {dimensions_filter[dim]} not found for dimension {dim}!"
                dimensions_filter_complete[dim] = dimensions_filter[dim]

    return expand_combinations(dimensions_filter_complete)


# model.core
def unique_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a list of unique records based on a specific key."""
    # Deduplicate by converting to tuples (which are hashable)
    seen = set()
    unique_records = []
    for record in records:
        # Convert each dictionary to a frozenset of items (key-value pairs)
        items = frozenset(record.items())
        if items not in seen:
            seen.add(items)
            unique_records.append(record)
    return unique_records


###################################
# Potential functions to deprecate
###################################


# none
def dump_yaml_with_anchors(data):
    """
    Dump a dictionary to a YAML string, converting definition keys to anchors
    and replacing quoted alias strings with YAML aliases.

    Args:
        data (dict): The dictionary to dump.

    Returns:
        str: The YAML string with anchors and aliases.
    """
    # Dump the dict to a YAML string. Using default_flow_style=False to get block style.
    dumped = yaml_dump(data)

    # For any key in the definitions block starting with "def_",
    # insert an anchor. This regex finds lines with an indented key that starts with def_.
    dumped = re.sub(
        r"^(\s+)(def_[^:]+):(.*)$",
        lambda m: f"{m.group(1)}{m.group(2)}: &{m.group(2)}{m.group(3)}",
        dumped,
        flags=re.MULTILINE,
    )

    # Replace quoted alias strings like '*def_2329260084214905053'
    # with an unquoted alias *def_2329260084214905053.
    dumped = re.sub(r"""(['"])(\*def_[^'"]+)\1""", lambda m: m.group(2), dumped)

    return dumped


# .extract_definitions
def move_field_to_top(data, field):
    """
    Returns a new dictionary with the specified field moved to the top.
    If the field doesn't exist, returns the original dictionary.
    """
    if field not in data:
        return data

    # Create a new dictionary starting with the specified field
    new_data = {field: data[field]}

    # Add the remaining items in their original order
    for key, value in data.items():
        if key != field:
            new_data[key] = value

    return new_data


# none
def extract_definitions(config: dict) -> dict:
    config = deepcopy(config)

    definitions = defaultdict(dict)
    for view in config["views"]:
        # Create shared definitions
        for indicator in view["indicators"]["y"]:
            # Move some fields into definitions
            display = indicator["display"]
            for key in ("additionalInfo",):
                info = display[key]
                info = info.replace("\\n", "\n")

                h = "def_" + str(abs(hash(display[key])))

                definitions[key][h] = info
                display[key] = "*" + h

    if "definitions" not in config:
        config["definitions"] = {}
    config["definitions"].update(definitions)
    config = move_field_to_top(config, "definitions")

    return config


def _check_missing_fields(template: str, params: dict) -> None:
    """
    Scan a single format string and raise ParamKeyError
    if it references a key that is missing from params.
    """
    fmt = Formatter()
    missing = {
        field_name for literal_text, field_name, *_ in fmt.parse(template) if field_name and field_name not in params
    }
    if missing:
        raise ParamKeyError(f"Missing keys for placeholders {missing!r} in template: {template!r}")


def fill_placeholders(data, params) -> Dict[str, Any] | List[Any] | Set[Any] | Tuple[Any] | str:
    """
    Recursively walk *data* (dicts, lists, tuples, sets, primitives) and
    replace any `{placeholder}` found in strings using values from *params*.

    Raises
    ------
    ParamKeyError
        If a placeholder key is referenced that is absent from *params*.
    """
    if isinstance(data, dict):
        return {k: fill_placeholders(v, params) for k, v in data.items()}

    if isinstance(data, (list, tuple, set)):
        container_type = type(data)
        return container_type(fill_placeholders(item, params) for item in data)

    if isinstance(data, str):
        _check_missing_fields(data, params)
        # All placeholders are present – safe to format
        return data.format(**params)

    # Any other type (int, bool, None, etc.) is returned unchanged
    return data


@deprecated("Use class method Collection.group_views instead.")
def group_views_legacy(views: list[dict[str, Any]], by: list[str]) -> list[dict[str, Any]]:
    """
    Group views by the specified dimensions. Concatenate indicators for the same group.

    :param views: List of views dictionaries.
    :param by: List of dimensions to group by.
    """
    views = deepcopy(views)

    grouped_views = {}
    for view in views:
        # Group key
        key = tuple(view["dimensions"][dim] for dim in by)

        if key not in grouped_views:
            if set(view["indicators"].keys()) != {"y"}:
                raise NotImplementedError(
                    "Only 'y' indicator is supported in groupby. Adapt the code for other fields."
                )

            if isinstance(view["indicators"]["y"], list):
                if len(view["indicators"]["y"]) > 1:
                    raise NotImplementedError(
                        "Only single indicator is supported in groupby. Adapt the code for multiple indicators."
                    )
                view["indicators"]["y"] = view["indicators"]["y"]
            else:
                view["indicators"]["y"] = [view["indicators"]["y"]]

            # Add to dictionary
            grouped_views[key] = view
        else:
            if isinstance(view["indicators"]["y"], list):
                if len(view["indicators"]["y"]) > 1:
                    raise NotImplementedError(
                        "Only single indicator is supported in groupby. Adapt the code for multiple indicators."
                    )
                indicator = view["indicators"]["y"][0]
            else:
                indicator = view["indicators"]["y"]
            grouped_views[key]["indicators"]["y"].append(indicator)

    return list(grouped_views.values())
