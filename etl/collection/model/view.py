import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from owid.catalog.meta import GrapherConfig

from etl.collection.model.base import MDIMBase
from etl.collection.utils import CHART_DIMENSIONS, merge_common_metadata_by_dimension, pruned_json

REGEX_CATALOG_PATH = (
    r"^grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/[A-Za-z0-9_]+/[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)
REGEX_CATALOG_PATH_OPTIONS = (
    r"^(?:(?:grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/)?[A-Za-z0-9_]+/)?[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)


@pruned_json
@dataclass
class CommonView(MDIMBase):
    dimensions: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    @property
    def num_dimensions(self) -> int:
        return len(self.dimensions) if self.dimensions is not None else 0


@pruned_json
@dataclass
class Indicator(MDIMBase):
    catalogPath: str
    display: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Validate that the catalog path is either (i) complete or (ii) in the format table#indicator.
        if not self.is_a_valid_path(self.catalogPath):
            raise ValueError(f"Invalid catalog path: {self.catalogPath}")

    def has_complete_path(self) -> bool:
        pattern = re.compile(REGEX_CATALOG_PATH)
        complete = bool(pattern.match(self.catalogPath))
        return complete

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

    y: Optional[List[Indicator]] = None
    x: Optional[Indicator] = None
    size: Optional[Indicator] = None
    color: Optional[Indicator] = None

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

    def to_records(self) -> List[Dict[str, Union[str, Dict[str, Any]]]]:
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


@pruned_json
@dataclass
class View(MDIMBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # NOTE: Maybe worth putting as classes at some point?
    config: Optional[GrapherConfig] = None
    metadata: Optional[Any] = None

    @property
    def d(self):
        return self.dimensions

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

    def combine_with_common(self, common_views: List[CommonView]):
        """Combine config and metadata fields in view with those specified by definitions.common_views."""
        # Update config
        new_config = merge_common_metadata_by_dimension(common_views, self.dimensions, self.config, "config")
        if new_config:
            self.config = new_config
        # Update metadata
        new_metadata = merge_common_metadata_by_dimension(common_views, self.dimensions, self.metadata, "metadata")
        if new_metadata:
            self.metadata = new_metadata

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
            raise ValueError(
                f"Extra indicators not in use. This means that some indicators are referenced in the chart config (e.g. map.columnSlug or sortColumnSlug), but never used in the chart tab. Unexpected indicators: {invalid_indicators}. If this is expected, set `tolerate_extra_indicators=True`."
            )
        elif invalid_indicators:
            indicators = indicators + list(invalid_indicators)

        return indicators
