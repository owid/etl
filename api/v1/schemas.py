from typing import Any, Dict, List, Optional

from owid.catalog.meta import PROCESSING_LEVELS
from pydantic import BaseModel

from .. import utils


class Presentation(BaseModel):
    titlePublic: Optional[str] = None
    titleVariant: Optional[str] = None
    attributionShort: Optional[str] = None

    def to_meta_dict(self) -> Dict[str, Any]:
        d = {
            "title_public": self.titlePublic,
            "title_variant": self.titleVariant,
            "attribution_short": self.attributionShort,
        }
        return utils.prune_none(d)


class Indicator(BaseModel):
    name: Optional[str] = None
    unit: Optional[str] = None
    shortUnit: Optional[str] = None
    display: Optional[Dict[str, Any]] = None
    descriptionShort: Optional[str] = None
    descriptionProcessing: Optional[str] = None
    descriptionFromProducer: Optional[str] = None
    descriptionKey: Optional[List[str]] = None
    presentation: Optional[Presentation] = None
    processingLevel: Optional[PROCESSING_LEVELS] = None
    updatePeriodDays: Optional[int] = None

    def to_meta_dict(self) -> Dict[str, Any]:
        d = {
            "title": self.name,
            # TODO: what about deleting values?
            "unit": self.unit,
            "short_unit": self.shortUnit,
            "display": self.display,
            "description_short": self.descriptionShort,
            "description_processing": self.descriptionProcessing,
            "description_from_producer": self.descriptionFromProducer,
            # TODO: make description_key work with lists
            "description_key": self.descriptionKey,
            "processing_level": self.processingLevel,
            # TODO: move it to dataset level
            "update_period_days": self.updatePeriodDays,
            "presentation": self.presentation.to_meta_dict() if self.presentation else None,
        }
        return utils.prune_none(d)


class UpdateIndicatorRequest(BaseModel):
    indicator: Indicator
    dataApiUrl: str
    dryRun: bool = False
    triggerETL: bool = False
