import re
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Set

from owid.catalog import Dataset, Table

from etl.db import read_sql
from etl.paths import DATA_DIR


def records_to_dictionary(records, key: str):
    """Transform: [{key: ..., a: ..., b: ...}, ...] -> {key: {a: ..., b: ...}, ...}."""

    dix = {}
    for record in records:
        assert key in record, f"`{key}` not found in record: {record}!"
        dix[record[key]] = {k: v for k, v in record.items() if k != key}

    return dix


def get_tables_by_name_mapping(dependencies: Set[str]) -> Dict[str, List[Table]]:
    """Dictionary mapping table short name to table object.

    Note that the format is {"table_name": [tb], ...}. This is because there could be collisions where multiple table names are mapped to the same table (e.g. two datasets could have a table with the same name).
    """
    tb_name_to_tb = defaultdict(list)

    for dep in dependencies:
        ## Ignore non-grapher dependencies
        if not re.match(r"^(data|data-private)://grapher/", dep):
            continue

        uri = re.sub(r"^(data|data-private)://", "", dep)
        ds = Dataset(DATA_DIR / uri)
        for table_name in ds.table_names:
            tb_name_to_tb[table_name].append(ds.read(table_name, load_data=False))

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


def merge_common_metadata_by_dimension(common_params, view_dimensions, view_params, field_name):
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
