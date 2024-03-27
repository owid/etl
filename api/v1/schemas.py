from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Extra

from .. import utils


class Presentation(BaseModel):
    titlePublic: Optional[str] = None
    titleVariant: Optional[str] = None
    attribution: Optional[str] = None
    attributionShort: Optional[str] = None

    class Config:
        extra = Extra.forbid

    def to_meta_dict(self) -> Dict[str, Any]:
        d = {
            "title_public": self.titlePublic,
            "title_variant": self.titleVariant,
            "attribution_short": self.attributionShort,
        }
        return utils.prune_none(d)


class Indicator(BaseModel):
    """JSON schema for indicator metadata from Data API (R2)."""

    name: Optional[str] = None
    unit: Optional[str] = None
    shortUnit: Optional[str] = None
    display: Optional[Dict[str, Any]] = None
    descriptionShort: Optional[str] = None
    descriptionProcessing: Optional[str] = None
    descriptionFromProducer: Optional[str] = None
    descriptionKey: Optional[List[str]] = None
    presentation: Optional[Presentation] = None
    processingLevel: Optional[Literal["minor", "major", ""]] = None
    updatePeriodDays: Union[int, Literal[""], None] = None

    class Config:
        extra = Extra.forbid

    def to_meta_dict(self) -> Dict[str, Any]:
        d = {
            "title": self.name,
            "unit": self.unit,
            "short_unit": self.shortUnit,
            "display": self.display,
            "description_short": self.descriptionShort,
            "description_processing": self.descriptionProcessing,
            "description_from_producer": self.descriptionFromProducer,
            "description_key": self.descriptionKey,
            "processing_level": self.processingLevel,
            "update_period_days": self.updatePeriodDays,
            "presentation": self.presentation.to_meta_dict() if self.presentation else None,
        }
        d = utils.prune_none(d)

        return d


class UpdateIndicatorRequest(BaseModel):
    """JSON schema for request to update indicator metadata."""

    catalogPath: str
    indicator: Indicator
    dataApiUrl: str
    dryRun: bool = False
    triggerETL: bool = False

    class Config:
        extra = Extra.forbid
