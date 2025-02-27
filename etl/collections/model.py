"""WIP: Drafting a model for dealing with MDIM/Explorer configuration.

This should be aligned with the MDIM schema.

THINGS TO SOLVE:

    - If an attribute is Optional, MetaBase.from_dict is not correctly loading it as the appropriate class when given.
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypeVar

from owid.catalog import Table
from owid.catalog.meta import MetaBase

DIMENSIONS = ["y", "x", "size", "color"]
T = TypeVar("T")
REGEX_CATALOG_PATH = (
    r"^(?:grapher/[A-Za-z0-9_]+/(?:\d{4}-\d{2}-\d{2}|\d{4}|latest)/[A-Za-z0-9_]+/)?[A-Za-z0-9_]+#[A-Za-z0-9_]+$"
)


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


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # calling original to_dict returns dictionaries, not objects
    cls.to_dict = lambda self, **kwargs: prune_dict(orig(self, **kwargs))  # type: ignore

    return cls


@pruned_json
@dataclass
class Indicator(MetaBase):
    catalogPath: str
    display: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        # Validate that the catalog path is either (i) complete or (ii) in the format table#indicator.
        if not self.is_a_valid_path(self.catalogPath):
            raise ValueError(f"Invalid catalog path: {self.catalogPath}")

    def has_complete_path(self) -> bool:
        return "/" in self.catalogPath

    @classmethod
    def is_a_valid_path(cls, path: str) -> bool:
        pattern = re.compile(REGEX_CATALOG_PATH)
        valid = bool(pattern.match(path))
        return valid

    def __setattr__(self, name, value):
        """Validate that the catalog path is either (i) complete or (ii) in the format table#indicator."""
        if hasattr(self, name):
            if (name == "catalogPath") and (not self.is_a_valid_path(value)):
                raise ValueError(f"Invalid catalog path: {value}")
        return super().__setattr__(name, value)

    def expand_path(self, tables_by_name: Dict[str, List[Table]]):
        # Do nothing if path is already complete
        if self.has_complete_path():
            return self

        # If path is not complete, we need to expand it!
        table_name = self.catalogPath.split("#")[0]

        # Check table is in any of the datasets!
        assert (
            table_name in tables_by_name
        ), f"Table name `{table_name}` not found in dependency tables! Available tables are: {', '.join(tables_by_name.keys())}"

        # Check table name to table mapping is unique
        assert (
            len(tables_by_name[table_name]) == 1
        ), f"There are multiple dependencies (datasets) with a table named {table_name}. Please use the complete dataset URI in this case."

        # Check dataset in table metadata is not None
        tb = tables_by_name[table_name][0]
        assert tb.m.dataset is not None, f"Dataset not found for table {table_name}"

        # Build URI
        self.catalogPath = tb.m.dataset.uri + "/" + self.catalogPath

        return self


@pruned_json
@dataclass
class ViewIndicators(MetaBase):
    """Indicators in a MDIM/Explorer view."""

    y: Optional[List[Indicator]] = None
    x: Optional[Indicator] = None
    size: Optional[Indicator] = None
    color: Optional[Indicator] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ViewIndicators":
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # Coerce each dimension field (y, x, size, color) from [str, ...] -> [{'path': str}, ...]
        for dim in DIMENSIONS:
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

    def to_records(self) -> List[Dict[str, str]]:
        indicators = []
        for dim in DIMENSIONS:
            dimension_val = getattr(self, dim, None)
            if dimension_val is None:
                continue
            if isinstance(dimension_val, list):
                for d in dimension_val:
                    indicators.append({"path": d.catalogPath, "dimension": dim})
            else:
                indicators.append({"path": dimension_val.catalogPath, "dimension": dim})
        return indicators

    def expand_paths(self, tables_by_name: Dict[str, List[Table]]):
        """Expand the catalog paths of all indicators in the view."""
        for dim in DIMENSIONS:
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
class View(MetaBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # NOTE: Maybe worth putting as classes at some point?
    config: Optional[Any] = None
    metadata: Optional[Any] = None

    @property
    def has_multiple_indicators(self) -> bool:
        # Get list of indicators
        indicators = self.indicators.to_records()
        return len(indicators) > 1

    @property
    def metadata_is_needed(self) -> bool:
        return self.has_multiple_indicators and (self.metadata is None)

    def expand_paths(self, tables_by_name: Dict[str, List[Table]]):
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

    # def indicators_in_view(self):
    #     """Get the list of indicators in use in a view.

    #     It returns the list as a list of records:

    #     [
    #         {
    #             "path": "data://path/to/dataset#indicator",
    #             "dimension": "y"
    #         },
    #         ...
    #     ]

    #     TODO: This is being called twice, maybe there is a way to just call it once. Maybe if it is an attribute of a class?
    #     """
    #     indicators_view = []
    #     # Get indicators from dimensions
    #     for dim in DIMENSIONS:
    #         if dim in self.indicators:
    #             indicator_raw = view["indicators"][dim]
    #             if isinstance(indicator_raw, list):
    #                 assert dim == "y", "Only `y` can come as a list"
    #                 indicators_view += [
    #                     {
    #                         "path": extract_catalog_path(ind),
    #                         "dimension": dim,
    #                     }
    #                     for ind in indicator_raw
    #                 ]
    #             else:
    #                 indicators_view.append(
    #                     {
    #                         "path": extract_catalog_path(indicator_raw),
    #                         "dimension": dim,
    #                     }
    #                 )
    #     return indicators_view


@pruned_json
@dataclass
class DimensionChoice(MetaBase):
    slug: str
    name: str
    description: Optional[str] = None


@pruned_json
@dataclass
class Dimension(MetaBase):
    """MDIM/Explorer dimension configuration."""

    slug: str
    name: str
    # NOTE: currently MetaBase.from_dict not loading Optional fields with appropriate class
    choices: Optional[List[DimensionChoice]] = None  # Only allowed to be None if checkbox
    presentation: Optional[Dict[str, Any]] = None


@pruned_json
@dataclass
class Collection(MetaBase):
    """Overall MDIM/Explorer config"""

    dimensions: List[Dimension]
    views: List[View]


@pruned_json
@dataclass
class Explorer(Collection):
    """Model for Explorer configuration."""

    config: Dict[str, str]


@pruned_json
@dataclass
class Multidim(Collection):
    """Model for MDIM configuration."""

    title: Dict[str, str]
    defaultSelection: List[str]
    topicTags: Optional[List[str]] = None
    definitions: Optional[Any] = None


# # def main():
# import yaml

# from etl.collections.utils import (
#     get_tables_by_name_mapping,
# )

# f_mdim = "/home/lucas/repos/etl/etl/steps/export/multidim/covid/latest/covid.cases_tests.yml"
# with open(f_mdim) as istream:
#     cfg_mdim = yaml.safe_load(istream)
# mdim = Multidim.from_dict(cfg_mdim)

# dependencies = {
#     "data://grapher/covid/latest/hospital",
#     "data://grapher/covid/latest/vaccinations_global",
#     "data://grapher/covid/latest/vaccinations_manufacturer",
#     "data://grapher/covid/latest/testing",
#     "data://grapher/excess_mortality/latest/excess_mortality",
#     "data-private://grapher/excess_mortality/latest/excess_mortality_economist",
#     "data://grapher/covid/latest/xm_who",
#     "data://grapher/covid/latest/cases_deaths",
#     "data://grapher/covid/latest/covax",
#     "data://grapher/covid/latest/infections_model",
#     "data://grapher/covid/latest/google_mobility",
#     "data://grapher/regions/2023-01-01/regions",
# }
# tables_by_name = get_tables_by_name_mapping(dependencies)

# mdim.views[0].indicators.expand_paths(tables_by_name)

# f_explorer = "/home/lucas/repos/etl/etl/steps/export/explorers/covid/latest/covid.config.yml"
# with open(f_explorer) as istream:
#     cfg_explorer = yaml.safe_load(istream)
# explorer = Explorer.from_dict(cfg_explorer)
# # cfg.views[0].indicators.y
