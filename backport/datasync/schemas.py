import datetime as dt
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Extra


class VariableData(BaseModel):
    years: List[int]
    entities: List[int]
    values: List[Any]

    class Config:
        extra = Extra.forbid


class VariableDisplayDataTableConfig(BaseModel):
    hideAbsoluteChange: Optional[bool]
    hideRelativeChange: Optional[bool]


class VariableDisplay(BaseModel):
    name: Optional[str]
    unit: Optional[str]
    shortUnit: Optional[str]
    isProjection: Optional[bool]
    includeInTable: Optional[bool]
    conversionFactor: Optional[float]
    numDecimalPlaces: Optional[int]
    tolerance: Optional[float]
    yearIsDay: Optional[bool]
    zeroDay: Optional[str]
    entityAnnotationsMap: Optional[str]
    tableDisplay: Optional[VariableDisplayDataTableConfig]
    color: Optional[str]

    class Config:
        extra = Extra.forbid


class VariableSource(BaseModel):
    id: int
    name: str
    dataPublishedBy: str
    dataPublisherSource: str
    link: str
    retrievedDate: str
    additionalInfo: str

    class Config:
        extra = Extra.forbid


class DimensionProperties(BaseModel):
    id: int
    name: Optional[str] = None
    code: Optional[str] = None

    class Config:
        extra = Extra.forbid


class Dimension(BaseModel):
    values: List[DimensionProperties]

    class Config:
        extra = Extra.forbid


class VariableMetadata(BaseModel):
    name: str
    unit: str
    shortUnit: Optional[str]
    code: Optional[str]
    description: Optional[str]
    createdAt: dt.datetime
    updatedAt: dt.datetime
    coverage: str
    timespan: str
    datasetId: int
    columnOrder: int
    datasetName: str
    nonRedistributable: bool
    display: VariableDisplay
    originalMetadata: Optional[str]
    grapherConfig: Optional[str]
    source: VariableSource
    type: str
    dimensions: Dict[str, Dimension]

    class Config:
        extra = Extra.forbid
