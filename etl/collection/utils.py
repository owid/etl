import re
from collections import defaultdict
from copy import deepcopy
from itertools import product
from typing import Any, Dict, List, Optional, Set, TypeVar

from owid.catalog import Dataset

from etl.db import read_sql
from etl.files import yaml_dump
from etl.paths import DATA_DIR

CHART_DIMENSIONS = ["y", "x", "size", "color"]


def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


def load_dataset_from_step(step: str) -> Dataset:
    uri = re.sub(r"^(data|data-private)://", "", step)
    # TODO: read no metadata
    return Dataset(DATA_DIR / uri)


def load_table_names_from_dependencies(dependencies: Set[str]) -> List[str]:
    table_names = []
    for uri in dependencies:
        ds = load_dataset_from_step(uri)
        for table_name in ds.table_names:
            table_names.append(table_name)
    return table_names


def has_duplicate_table_names(dependencies: Set[str]) -> bool:
    table_names = load_table_names_from_dependencies(dependencies)
    return len(table_names) != len(set(table_names))


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


def merge_common_metadata_by_dimension(common_params, view_dimensions: Dict[str, Any], view_params, field_name: str):
    """
    Merge metadata entries with dimension-based inheritance and deep merging.
    Resolves conflicts by specificity and raises an error for any unresolved conflicts.
    """

    # Helper to check if an entry's dimensions match the active dimensions
    def entry_matches(entry_dims, active_dims):
        if not entry_dims:
            return True
        for k, v in entry_dims.items():
            if k not in active_dims or active_dims[k] != v:
                return False
        return True

    # Filter entries applicable to the current dimensions and sort by specificity (num of dimension conditions)
    applicable_entries = [entry for entry in common_params if entry_matches(entry.dimensions, view_dimensions)]
    applicable_entries = sorted(applicable_entries, key=lambda e: e.num_dimensions)

    # Placeholder for result
    final_result = {}

    # Track the priority (specificity level) and source of the last set value for each key
    key_priority = {}
    key_source = {}
    # Map of tuple key paths to their source (for nested keys conflict reporting)
    value_source_map = {}
    # Dictionary of conflicts: { key_path_tuple: [ {source: ..., value: ...}, ... ] }
    unresolved_conflicts = {}

    def deep_equal(val1, val2):
        """Check if val1 and val2 are equal, even if they are nested dictionaries or lists (or combination)."""
        if not isinstance(val1, type(val2)):
            return False
        if isinstance(val1, dict):
            if val1.keys() != val2.keys():
                return False
            return all(deep_equal(val1[k], val2[k]) for k in val1)
        if isinstance(val1, list):
            if len(val1) != len(val2):
                return False
            return all(deep_equal(x, y) for x, y in zip(val1, val2))
        return val1 == val2

    def deep_merge(base, override):
        """Deep merge a dictionary with another dictionary. Values from `override` take priority."""
        merged = deepcopy(base)
        for k, v in override.items():
            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                merged[k] = deep_merge(merged[k], v)
            else:
                merged[k] = deepcopy(v)
        return merged

    def source_desc(entry):
        """Get a string representation identifying a specific dimension.

        This string is used to identify the source of a metadata entry after several merges. It can be useful to track the origin of a conflict.

        When no dimension is specified, the string "default" is returned. This means that the entry applies to all dimensions.
        """
        dims = entry.get("dimensions")
        return str(dims) if dims and len(dims) > 0 else "default"

    # Record the source for all nested keys in a dict (for conflict reporting)
    def record_source_for_dict(value, path_prefix, source):
        if isinstance(value, dict):
            for sub_key, sub_val in value.items():
                new_path = path_prefix + (sub_key,)
                value_source_map[new_path] = source
                record_source_for_dict(sub_val, new_path, source)

    # Merge dictionaries for equal-priority entries, recording conflicts for differing subkeys
    def merge_same_level_dict(existing_dict, new_dict, parent_path, source_prev, source_new):
        for sub_key, new_val in new_dict.items():
            if sub_key in existing_dict:
                existing_val = existing_dict[sub_key]
                if isinstance(existing_val, dict) and isinstance(new_val, dict):
                    # Recurse into nested dict
                    merge_same_level_dict(existing_val, new_val, parent_path + (sub_key,), source_prev, source_new)
                else:
                    # Check for conflicts on this sub-key
                    if deep_equal(existing_val, new_val):
                        # Values are identical – no conflict (already in result, nothing to change)
                        continue
                    conflict_path = parent_path + (sub_key,)
                    # Determine the original source of the existing value (from value_source_map or default to source_prev)
                    existing_source = value_source_map.get(conflict_path, source_prev)
                    new_source = source_new
                    # Record this conflict (with sources and values)
                    if conflict_path not in unresolved_conflicts:
                        unresolved_conflicts[conflict_path] = [
                            {"source": existing_source, "value": deepcopy(existing_val)},
                            {"source": new_source, "value": deepcopy(new_val)},
                        ]
                    else:
                        # Append if this source/value pair not already recorded
                        if not any(
                            rec["source"] == new_source and deep_equal(rec["value"], new_val)
                            for rec in unresolved_conflicts[conflict_path]
                        ):
                            unresolved_conflicts[conflict_path].append(
                                {"source": new_source, "value": deepcopy(new_val)}
                            )
                    # Do not override the existing value; leave it as-is until resolved by higher priority
            else:
                # New sub-key (no conflict) – add to dictionary
                existing_dict[sub_key] = deepcopy(new_val)
                value_source_map[parent_path + (sub_key,)] = source_new
                record_source_for_dict(new_val, parent_path + (sub_key,), source_new)
        # (Sub-keys present only in existing_dict remain intact with their original source)

    # Process each entry in order
    for entry in applicable_entries:
        # TODO: temporary conversion
        entry = entry.to_dict()

        entry_source = source_desc(entry)
        current_priority = len(entry.get("dimensions", {}))
        # Combine config and metadata sections for convenience
        combined_fields = {}
        if field_name in entry:
            combined_fields = deepcopy(entry[field_name])

        # Merge this entry's fields into final_result
        for key, new_val in combined_fields.items():
            if key in final_result:
                prev_priority = key_priority[key]
                if current_priority == prev_priority:
                    # Same priority as an existing entry that set this key
                    existing_val = final_result[key]
                    prev_source = key_source[key]
                    if isinstance(existing_val, dict) and isinstance(new_val, dict):
                        # Deep merge dictionaries at the same level, handling sub-conflicts
                        merge_same_level_dict(existing_val, new_val, (key,), prev_source, entry_source)
                        # Update the final_result with merged dict (existing_val is mutated in-place)
                        final_result[key] = existing_val
                    else:
                        # Primitive or non-dict values conflict if not identical
                        if deep_equal(existing_val, new_val):
                            continue  # they are the same value, no conflict
                        conflict_path = (key,)
                        existing_source = value_source_map.get((key,), key_source.get(key, prev_source))
                        new_source = entry_source
                        if conflict_path not in unresolved_conflicts:
                            unresolved_conflicts[conflict_path] = [
                                {"source": existing_source, "value": deepcopy(existing_val)},
                                {"source": new_source, "value": deepcopy(new_val)},
                            ]
                        else:
                            if not any(
                                rec["source"] == new_source and deep_equal(rec["value"], new_val)
                                for rec in unresolved_conflicts[conflict_path]
                            ):
                                unresolved_conflicts[conflict_path].append(
                                    {"source": new_source, "value": deepcopy(new_val)}
                                )
                        # Keep the existing value for now (do not overwrite at equal priority)
                else:
                    # current entry is higher priority – override lower value
                    if isinstance(final_result[key], dict) and isinstance(new_val, dict):
                        # Remove any conflicts related to this key (it's being overridden by higher specificity)
                        for path in list(unresolved_conflicts.keys()):
                            if path[0] == key:
                                if len(path) == 1:
                                    unresolved_conflicts.pop(path, None)
                                else:
                                    # If conflict was on a nested key, only consider it resolved if this entry provides that subkey
                                    if path[1] in new_val:
                                        unresolved_conflicts.pop(path, None)
                        # Deep merge dictionaries: override overlapping subkeys, keep others
                        final_result[key] = deep_merge(final_result[key], new_val)
                    else:
                        # Override entire value
                        for path in list(unresolved_conflicts.keys()):
                            if path[0] == key:
                                unresolved_conflicts.pop(path, None)
                        final_result[key] = deepcopy(new_val)
                    # Update source and priority for this key
                    key_priority[key] = current_priority
                    key_source[key] = entry_source
                    value_source_map[(key,)] = entry_source
                    record_source_for_dict(new_val, (key,), entry_source)
            else:
                # New key (no existing value in result)
                final_result[key] = deepcopy(new_val)
                key_priority[key] = current_priority
                key_source[key] = entry_source
                value_source_map[(key,)] = entry_source
                record_source_for_dict(new_val, (key,), entry_source)

    # Apply view_params (highest priority overrides)
    if view_params:
        for key, val in view_params.items():
            # Remove any conflict associated with this key (custom override resolves it)
            for path in list(unresolved_conflicts.keys()):
                if path[0] == key:
                    unresolved_conflicts.pop(path, None)
            if key in final_result and isinstance(final_result[key], dict) and isinstance(val, dict):
                final_result[key] = deep_merge(final_result[key], val)
            else:
                final_result[key] = deepcopy(val)
            key_priority[key] = float("inf")
            key_source[key] = "custom"
            value_source_map[(key,)] = "custom"
            record_source_for_dict(val, (key,), "custom")

    # If any conflicts remain unresolved, raise an error with details
    if unresolved_conflicts:
        messages = []
        for path, info_list in unresolved_conflicts.items():
            key_path = ".".join(path)
            # List sources and values involved in this conflict
            sources = [f"{rec['source']} (value={rec['value']})" for rec in info_list]
            messages.append(f"'{key_path}' from " + " vs ".join(sources))
        raise ValueError("Unresolved conflicts for keys: " + "; ".join(messages))

    return final_result


# Pre-compile the regex pattern for performance.
_pattern = re.compile(r"_([a-z])")


def snake_to_camel(s: str) -> str:
    # Use the compiled pattern to substitute underscores with the uppercase letter.
    return _pattern.sub(lambda match: match.group(1).upper(), s)


def camelize(obj: Any, exclude_keys: Optional[Set[str]] = None) -> Any:
    """
    Recursively converts dictionary keys from snake_case to camelCase, unless the key is in exclude_keys.

    Parameters:
        obj: The object (dict, list, or other) to process.
        exclude_keys: An optional iterable of keys that should not be converted (including nested values).
    """
    exclude_keys = exclude_keys or set()

    if isinstance(obj, dict):
        new_obj: dict[Any, Any] = {}
        for key, value in obj.items():
            # Leave the key unchanged if it's in the exclusion list
            if key in exclude_keys:
                new_obj[key] = value
            else:
                new_obj[snake_to_camel(key)] = camelize(value, exclude_keys)
        return new_obj
    elif isinstance(obj, list):
        return [camelize(item, exclude_keys) for item in obj]
    else:
        return obj


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


def expand_combinations(dim_dict):
    # Normalize all values to lists (in case they are sets)
    keys = list(dim_dict)
    values = [list(dim_dict[k]) for k in keys]
    return [dict(zip(keys, combo)) for combo in product(*values)]


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


def prune_dict(d: dict) -> dict:
    """Remove all keys starting with underscore and all empty values from a dictionary.

    NOTE: This method was copied from owid.catalog.utils. It is slightly different in the sense that it does not remove fields with empty lists! This is because there are some fields which are mandatory and can be empty! (TODO: should probably fix the schema / engineering side)

    """
    out = {}
    for k, v in d.items():
        if not k.startswith("_") and v not in [None, {}]:
            if isinstance(v, dict):
                out[k] = prune_dict(v)
            elif isinstance(v, list):
                out[k] = [prune_dict(x) if isinstance(x, dict) else x for x in v if x not in [None, {}]]
            else:
                out[k] = v
    return out


T = TypeVar("T")


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

    return cls


def group_views(views: list[dict[str, Any]], by: list[str]) -> list[dict[str, Any]]:
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
