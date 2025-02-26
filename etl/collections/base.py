"""WIP: Drafting a model for dealing with MDIM/Explorer configuration.

This should be aligned with the MDIM schema.

THINGS TO SOLVE:

    - If an attribute is Optional, MetaBase.from_dict is not correctly loading it as the appropriate class when given.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml
from owid.catalog.meta import MetaBase
from owid.catalog.utils import pruned_json


@pruned_json
@dataclass
class Indicator(MetaBase):
    catalogPath: str
    display: Optional[Dict[str, Any]] = None


@pruned_json
@dataclass
class ViewIndicators(MetaBase):
    """Indicators in a MDIM/Explorer view."""

    # TODO: these attributes should ALL be Optional.
    # NOTE: currently MetaBase.from_dict not loading Optional fields with appropriate class
    y: List[Indicator]
    x: Optional[List[Indicator]] = None
    size: Optional[List[Indicator]] = None
    color: Optional[List[Indicator]] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ViewIndicators":
        """Coerce the dictionary into the expected shape before passing it to the parent class."""
        # Make a shallow copy so we don't mutate the user's dictionary in-place
        data = dict(d)

        # Coerce each dimension field (y, x, size, color) from [str, ...] -> [{'path': str}, ...]
        for dim in ("y", "x", "size", "color"):
            if dim in data:
                if isinstance(data[dim], list):
                    coerced_items = []
                    for item in data[dim]:
                        if isinstance(item, str):
                            coerced_items.append({"catalogPath": item})
                        else:
                            # If already a dict or something else, leave it as-is
                            coerced_items.append(item)
                    data[dim] = coerced_items
                else:
                    if isinstance(data[dim], str):
                        data[dim] = [{"catalogPath": data[dim]}]
                    else:
                        data[dim] = [data[dim]]

        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)


@pruned_json
@dataclass
class View(MetaBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # NOTE: currently MetaBase.from_dict not loading Optional fields with appropriate class
    config: Optional[Any] = None
    metadata: Optional[Any] = None


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
f_mdim = "/home/lucas/repos/etl/etl/steps/export/multidim/covid/latest/covid.cases_tests.yml"
with open(f_mdim) as istream:
    cfg_mdim = yaml.safe_load(istream)
mdim = Multidim.from_dict(cfg_mdim)

# f_explorer = "/home/lucas/repos/etl/etl/steps/export/explorers/covid/latest/covid.config.yml"
# with open(f_explorer) as istream:
#     cfg_explorer = yaml.safe_load(istream)
# explorer = Explorer.from_dict(cfg_explorer)
# # cfg.views[0].indicators.y
