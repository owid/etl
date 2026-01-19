# Stub file for backwards compatibility - re-exports from core/utils.py
# New code should import from owid.catalog.core.utils or owid.catalog directly
from owid.catalog.core.utils import (
    T,
    dataclass_from_dict,
    dynamic_yaml_load,
    dynamic_yaml_to_dict,
    hash_any,
    log,
    parse_numeric_list,
    prune_dict,
    pruned_json,
    remove_details_on_demand,
    underscore,
    underscore_table,
    validate_underscore,
)

__all__ = [
    "T",
    "log",
    "prune_dict",
    "pruned_json",
    "underscore",
    "underscore_table",
    "validate_underscore",
    "dynamic_yaml_load",
    "dynamic_yaml_to_dict",
    "hash_any",
    "dataclass_from_dict",
    "remove_details_on_demand",
    "parse_numeric_list",
]
