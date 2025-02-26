"""WIP: Drafting a model for dealing with MDIM/Explorer configuration.

This should be aligned with the MDIM schema.
"""

from typing import Any, Dict, List, Optional


class Config:
    """Overall MDIM/Explorer config"""

    config: Dict[str, str]
    views: List["ViewConfig"]
    dimensions: List["DimensionConfig"]


class ViewConfig:
    """MDIM/Explorer view configuration."""

    dimensions: Dict[str, str]
    indicators: "ViewIndicators"
    config: Optional[Any]
    metadata: Optional[Any]


class ViewIndicators:
    """Indicators in a MDIM/Explorer view."""

    y: Optional[List["Indicator"]]
    x: Optional[List["Indicator"]]
    size: Optional[List["Indicator"]]
    color: Optional[List["Indicator"]]


class Indicator:
    path: str
    display: Dict[str, Any]


class DimensionConfig:
    """MDIM/Explorer dimension configuration."""

    choices: List["ChoiceConfig"]


class ChoiceConfig:
    slug: str
    name: str
    description: str
