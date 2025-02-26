"""WIP: Drafting a model for dealing with MDIM/Explorer configuration.

This should be aligned with the MDIM schema.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml
from owid.catalog.meta import MetaBase


@dataclass
class Indicator(MetaBase):
    path: str
    display: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        print("POST ViewIndicators!")


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
                if isinstance(data[dim], str):
                    data[dim] = [{"path": data[dim]}]
                if isinstance(data[dim], list):
                    coerced_items = []
                    for item in data[dim]:
                        if isinstance(item, str):
                            coerced_items.append({"path": item})
                        else:
                            # If already a dict or something else, leave it as-is
                            coerced_items.append(item)
                    data[dim] = coerced_items

        # Now that data is in the expected shape, let the parent class handle the rest
        return super().from_dict(data)


@dataclass
class View(MetaBase):
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: ViewIndicators
    # NOTE: currently MetaBase.from_dict not loading Optional fields with appropriate class
    config: Optional[Any] = None
    metadata: Optional[Any] = None


@dataclass
class DimensionChoice(MetaBase):
    slug: str
    name: str
    description: Optional[str] = None


@dataclass
class Dimension(MetaBase):
    """MDIM/Explorer dimension configuration."""

    slug: str
    name: str
    # NOTE: currently MetaBase.from_dict not loading Optional fields with appropriate class
    choices: Optional[List[DimensionChoice]] = None  # Only allowed to be None if checkbox
    presentation: Optional[Dict[str, Any]] = None


@dataclass
class Collection(MetaBase):
    """Overall MDIM/Explorer config"""

    config: Dict[str, str]
    dimensions: List[Dimension]
    views: List[View]


# def main():
#     filename = "/home/lucas/repos/etl/etl/steps/export/multidim/covid/latest/covid.cases.yml"
filename = "/home/lucas/repos/etl/etl/steps/export/explorers/covid/latest/covid.config.yml"
with open(filename) as istream:
    yml = yaml.safe_load(istream)
# cfg = Config.from_dict(yml)
# cfg.views[0].indicators.y
