import datetime as dt
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Extra, Field

from etl.files import yaml_dump


class YAMLSourceMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    published_by: str
    publication_year: Optional[int] = None
    date_accessed: dt.date = Field(default_factory=dt.date.today)
    url: Optional[str] = None


class YAMLDatasetMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    namespace: str
    version: Union[str, dt.date]
    short_name: str
    title: str
    description: str
    sources: List[YAMLSourceMeta] = Field(default_factory=list)

    @property
    def path(self):
        return f"{self.namespace}/{self.version}/{self.short_name}"


class YAMLVariableMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    title: str
    short_unit: Union[str, None] = None
    unit: str
    description: Optional[str] = None
    display: Optional[dict[str, Any]] = None
    sources: List[YAMLSourceMeta] = Field(default_factory=list)


class YAMLTableMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    title: Optional[str]
    description: Optional[str]
    variables: dict[str, YAMLVariableMeta]


class YAMLMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    dataset: YAMLDatasetMeta
    tables: dict[str, YAMLTableMeta]

    def to_yaml(self) -> str:
        return yaml_dump(self.dict(exclude_none=True, exclude_unset=True))  # type: ignore
