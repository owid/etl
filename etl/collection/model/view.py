import re
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, cast

from etl.collection.exceptions import CommonViewParamConflict, ExtraIndicatorsInUseError
from etl.collection.model.base import MDIMBase, pruned_json
from etl.collection.model.schema_types import ViewConfig, ViewMetadata
from etl.collection.utils import CHART_DIMENSIONS

REGEX_CATALOG_PATH = (
    r"^grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/[A-Za-z0-9_]+/[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)
REGEX_CATALOG_PATH_OPTIONS = (
    r"^(?:(?:grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/)?[A-Za-z0-9_]+/)?[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)


class ReadOnlyNamespace(SimpleNamespace):
    def __setattr__(self, name, value):
        if hasattr(self, name):
            raise AttributeError(f"Cannot modify attribute '{name}'")
        super().__setattr__(name, value)


@pruned_json
@dataclass
class CommonView(MDIMBase):
    dimensions: Dict[str, Any] | None = None
    config: ViewConfig | Dict[str, Any] | None = None
    metadata: ViewMetadata | Dict[str, Any] | None = None

    @property
    def num_dimensions(self) -> int:
        return len(self.dimensions) if self.dimensions is not None else 0


@pruned_json
@dataclass
class Indicator(MDIMBase):
    catalogPath: str
    display: Dict[str, Any] | None = None

    def __post_init__(self):
        # Validate that the catalog path is either (i) complete or (ii) in the format table#indicator.
        if not self.is_a_valid_path(self.catalogPath):
            raise ValueError(f"Invalid catalog path: {self.catalogPath}")

    def has_complete_path(self) -> bool:
        pattern = re.compile(REGEX_CATALOG_PATH)
        complete = bool(pattern.match(self.catalogPath))
        return complete

    def update_display(self, display: Dict[str, Any]):
        """Update the display dictionary of the indicator."""
        if self.display is None:
            self.display = {}
        self.display.update(display)

    @classmethod
    def is_a_valid_path(cls, path: str) -> bool:
        """Valid paths are:
        - grapher/namespace/version/dataset/table#indicator.
        - dataset/table#indicator
        - table#indicator
        """
        pattern = re.compile(REGEX_CATALOG_PATH_OPTIONS)
        valid = bool(pattern.match(path))
        return valid

    def __setattr__(self, name, value):
        """Validate that the catalog path is either (i) complete or (ii) in the format table#indicator."""
        if hasattr(self, name):
            if (name == "catalogPath") and (not self.is_a_valid_path(value)):
                raise ValueError(f"Invalid catalog path: {value}")
        return super().__setattr__(name, value)

    def expand_path(self, tables_by_name: Dict[str, List[str]]):
        # Do nothing if path is already complete
        if self.has_complete_path():
            return self

        # If path is not complete, we need to expand it!
        table_name, indicator_name = self.catalogPath.split("#")

        # Check table is in any of the datasets!
        assert (
            table_name in tables_by_name
        ), f"Table name `{table_name}` not found in dependency tables! Available tables are: {', '.join(tables_by_name.keys())}"

        # Check table name to table mapping is unique
        assert (
            len(tables_by_name[table_name]) == 1
        ), f"There are multiple dependencies (datasets) with a table named {table_name}. Please add dataset name (dataset_name/table_name#indicator_name) if you haven't already, or use the complete dataset URI in this case."

        # Check dataset in table metadata is not None
        tb_uri = tables_by_name[table_name][0]
        # assert tb.m.dataset is not None, f"Dataset not found for table {table_name}"

        # Build URI
        self.catalogPath = tb_uri + "#" + indicator_name

        return self


@pruned_json
@dataclass
class ViewIndicators(MDIMBase):
    """Indicators in a MDIM/Explorer view."""

    y: List[Indicator] | None = None
    x: Indicator | None = None
    size: Indicator | None = None
    color: Indicator | None = None

    @property
    def num_indicators(self) -> int:
        """Get the total number of indicators in the view."""
        return sum([1 for dim in CHART_DIMENSIONS if getattr(self, dim, None) is not None])

    def has_non_y_indicators(self) -> bool:
        """Check if the view has non-y indicators."""
        return any([getattr(self, dim, None) is not None for dim in CHART_DIMENSIONS[1:]])

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ViewIndicators":
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # Coerce each dimension field (y, x, size, color) from [str, ...] -> [{'path': str}, ...]
        for dim in CHART_DIMENSIONS:
            if dim in data:
                if isinstance(data[dim], list):
                    data[dim] = [{"catalogPath": item} if isinstance(item, str) else item for item in data[dim]]
                else:
                    if isinstance(data[dim], str):
                        data[dim] = [{"catalogPath": data[dim]}] if dim == "y" else {"catalogPath": data[dim]}
                    elif dim == "y":
                        data[dim] = [data[dim]]
        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)

    def to_records(self) -> List[Dict[str, str | Dict[str, Any]]]:
        indicators = []
        for dim in CHART_DIMENSIONS:
            dimension_val = getattr(self, dim, None)
            if dimension_val is None:
                continue
            if isinstance(dimension_val, list):
                for d in dimension_val:
                    display = d.display if d.display is not None else {}
                    indicator_ = {"path": d.catalogPath, "axis": dim, "display": display}
                    indicators.append(indicator_)
            else:
                display = dimension_val.display if dimension_val.display is not None else {}
                indicator_ = {
                    "path": dimension_val.catalogPath,
                    "axis": dim,
                    "display": display,
                }
                indicators.append(indicator_)
        return indicators

    def expand_paths(self, tables_by_name: Dict[str, List[str]]):
        """Expand the catalog paths of all indicators in the view."""
        for dim in CHART_DIMENSIONS:
            dimension_val = getattr(self, dim, None)
            if dimension_val is None:
                continue
            if isinstance(dimension_val, list):
                for indicator in dimension_val:
                    indicator.expand_path(tables_by_name)
            else:
                dimension_val.expand_path(tables_by_name)

        return self

    def set_indicator(
        self,
        y: List[str] | List[Dict[str, Any]] | str | Dict[str, Any] | None = None,
        x: str | None = None,
        color: str | None = None,
        size: str | None = None,
    ):
        def _load_indicator(indicator_raw, indicator_label):
            if isinstance(indicator_raw, str):
                indicator = Indicator(catalogPath=indicator_raw)
            elif isinstance(indicator_raw, dict):
                indicator = Indicator.from_dict(indicator_raw)
            else:
                raise ValueError(
                    f"Invalid type for indicator {indicator_label}: {type(indicator_raw)}. Expected str or dict."
                )
            return indicator

        if y is not None:
            if isinstance(y, list):
                self.y = [_load_indicator(yy, "y") for yy in y]
            else:
                self.y = [_load_indicator(y, "y")]
        if x is not None:
            self.x = _load_indicator(x, "x")
        if color is not None:
            self.color = _load_indicator(color, "color")
        if size is not None:
            self.size = _load_indicator(size, "size")


@pruned_json
@dataclass
class View(MDIMBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # config: Optional[Union[ViewConfig, Dict[str, Any]]] = None
    config: ViewConfig | Dict[str, Any] | None = None
    metadata: ViewMetadata | Dict[str, Any] | None = None
    _is_grouped: bool = False  # Private flag to mark views created by grouping

    @property
    def is_grouped(self) -> bool:
        """Check if this view was created by grouping other views."""
        return self._is_grouped

    def mark_as_grouped(self) -> None:
        """Mark this view as created by grouping other views."""
        object.__setattr__(self, "_is_grouped", True)

    @property
    def d(self) -> ReadOnlyNamespace:
        # Create a hash of current dimensions content for cache invalidation
        current_hash = hash(frozenset(self.dimensions.items()))

        # Check if we have a cached version and if dimensions haven't changed
        if hasattr(self, "_d_cache") and hasattr(self, "_d_hash") and self._d_hash == current_hash:
            return self._d_cache

        # Create new ReadOnlyNamespace and cache it with current hash
        self._d_cache = ReadOnlyNamespace(**self.dimensions)
        self._d_hash = current_hash
        return self._d_cache

    def __setattr__(self, name, value):
        if name == "d":
            raise AttributeError(f"Cannot set read-only property '{name}'")
        # Clear cache when dimensions object is replaced entirely
        if name == "dimensions":
            if hasattr(self, "_d_cache"):
                delattr(self, "_d_cache")
            if hasattr(self, "_d_hash"):
                delattr(self, "_d_hash")
        super().__setattr__(name, value)

    def has_non_y_indicators(self) -> bool:
        """Check if the view has non-y indicators."""
        return self.indicators.has_non_y_indicators()

    @property
    def has_multiple_indicators(self) -> bool:
        # Get list of indicators
        indicators = self.indicators.to_records()
        return len(indicators) > 1

    @property
    def num_indicators(self) -> int:
        """Get the total number of indicators in the view."""
        return self.indicators.num_indicators

    @property
    def metadata_is_needed(self) -> bool:
        return self.has_multiple_indicators and (self.metadata is None)

    def expand_paths(self, tables_by_name: Dict[str, List[str]]):
        """Expand all indicator paths in the view.

        Make sure that they are all complete paths. This includes indicators in view, but also those in config (if any).
        """
        # Expand paths in indicators
        self.indicators.expand_paths(tables_by_name)

        # Expand paths in config fields
        if self.config is not None:
            if "sortColumnSlug" in self.config:
                indicator = Indicator(self.config["sortColumnSlug"]).expand_path(tables_by_name)
                self.config["sortColumnSlug"] = indicator.catalogPath

            if "map" in self.config:
                if "columnSlug" in self.config["map"]:
                    indicator = Indicator(self.config["map"]["columnSlug"]).expand_path(tables_by_name)
                    self.config["map"]["columnSlug"] = indicator.catalogPath

        return self

    def combine_with_common(self, common_views: List[CommonView], common_has_priority: bool = False):
        """Combine config and metadata fields in view with those specified by definitions.common_views."""
        # Update config
        new_config = merge_common_metadata_by_dimension(
            common_views,
            self.dimensions,
            self.config,
            "config",
            common_has_priority=common_has_priority,
        )
        if new_config:
            self.config = cast(ViewConfig, new_config)
        # Update metadata
        new_metadata = merge_common_metadata_by_dimension(
            common_views,
            self.dimensions,
            self.metadata,
            "metadata",
            common_has_priority=common_has_priority,
        )
        if new_metadata:
            self.metadata = cast(ViewMetadata, new_metadata)

        return self

    @property
    def indicators_in_config(self):
        indicators = []
        if self.config is not None:
            # Get indicators from sortColumnSlug
            if "sortColumnSlug" in self.config:
                indicators.append(self.config["sortColumnSlug"])

            # Update indicators from map.columnSlug
            if ("map" in self.config) and "columnSlug" in self.config["map"]:
                indicators.append((self.config["map"]["columnSlug"]))

        return indicators

    def indicators_used(self, tolerate_extra_indicators: bool = False):
        """Get a flatten list of all indicators used in the view.

        In addition, it also validates that indicators used in config are also in the view.

        NOTE: Use this method after expanding paths! Otherwise, it will not work as expected. E.g. view.expand_paths(tables_by_name).indicators_used()
        """
        # Validate indicators in view
        indicators = self.indicators.to_records()
        indicators = [ind["path"] for ind in indicators]

        # All indicators in `indicators_extra` should be in `indicators`! E.g. you can't sort by an indicator that is not in the chart!
        ## E.g. the indicator used to sort, should be in use in the chart! Or, the indicator in the map tab should be in use in the chart!
        invalid_indicators = set(self.indicators_in_config).difference(set(indicators))
        if not tolerate_extra_indicators and invalid_indicators:
            raise ExtraIndicatorsInUseError(
                f"Extra indicators not allowed. This means that some indicators are referenced in the chart config (e.g. map.columnSlug or sortColumnSlug), but never used in the chart tab. Unexpected indicators: {invalid_indicators}. If this is expected, set `tolerate_extra_indicators=True`."
            )
        elif invalid_indicators:
            indicators = indicators + list(invalid_indicators)

        return indicators

    def matches(self, **kwargs):
        """Evaluate if a view matches a set of dimensions.

        kwargs:
            Key-value pairs representing the dimension names and values to match. Keys are function arguments, and values are the argument values, which can either be a single value or a list of values. If a list, it evaluates to True if any value is matched.

        ```python
        for v in c.views:
            if v.matches(age="all", sex="female"):
                pass
            elif v.matches(age=[0, 10]):
                pass
        ```
        """
        for dim_name, dim_value in kwargs.items():
            if dim_name not in self.dimensions:
                raise ValueError(f"Dimension '{dim_name}' not found in view dimensions: {self.dimensions.keys()}")
            if hasattr(dim_value, "__iter__") and not isinstance(dim_value, (str, bytes)):
                # If the dimension value is a list, check if it matches any of the values in the view dimensions
                if dim_value and self.dimensions[dim_name] not in dim_value:
                    return False
            elif isinstance(dim_value, (str, int, float)):
                if self.dimensions[dim_name] != dim_value:
                    return False
        return True


def merge_common_metadata_by_dimension(
    common_config,
    view_dimensions: Dict[str, Any],
    view_config,
    field_name: str,
    common_has_priority: bool = False,
):
    """
    Merge metadata entries with dimension-based inheritance and deep merging.
    Resolves conflicts by specificity and raises an error for any unresolved conflicts.

    common_has_priority: bool
        Set to True if the parameters the parameters from common_config should override the parameters from view_config.
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
    applicable_entries = [entry for entry in common_config if entry_matches(entry.dimensions, view_dimensions)]
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

    # Process each entry in order, and build the config/metadata PRIOR to merging it to view_config
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

    # Combine `final_result` and `view_config`
    # - final_result: At this point it contains the combined parameters (config or metadata) from `common_params`.
    # - view_config: It contains the parameters for a given specific view.
    #
    # By default (`common_has_priority=False``), content of `view_config` takes preference.
    if view_config:
        for key, val in view_config.items():
            # Default behavior: `view_config` takes priority over `common_params`
            if not common_has_priority:
                # Remove any conflict associated with this key (custom override resolves it)
                for path in list(unresolved_conflicts.keys()):
                    if path[0] == key:
                        unresolved_conflicts.pop(path, None)
                # If the key is already in final_result, merge or override as needed (with deep merge if required)
                if key in final_result and isinstance(final_result[key], dict) and isinstance(val, dict):
                    final_result[key] = deep_merge(final_result[key], val)
                else:
                    final_result[key] = deepcopy(val)
            # Common has priority
            else:
                if key in final_result and isinstance(final_result[key], dict) and isinstance(val, dict):
                    final_result[key] = deep_merge(val, final_result[key])
                elif key not in final_result:
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
        raise CommonViewParamConflict("Unresolved conflicts for keys: " + "; ".join(messages))

    return final_result
