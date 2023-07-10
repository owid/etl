#
#  meta.py
#
#  Metadata helpers.
#

import datetime as dt
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, NewType, Optional, TypeVar, Union

import yaml
from dataclasses_json import dataclass_json

T = TypeVar("T")


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # make sure to call `to_dict` of nested objects as well
    cls.to_dict = lambda self, **kwargs: {  # type: ignore
        k: getattr(self, k).to_dict(**kwargs) if hasattr(getattr(self, k), "to_dict") else v
        for k, v in orig(self, **kwargs).items()
        if not k.startswith("_") and v not in [None, [], {}]
    }

    return cls


SOURCE_EXISTS_OPTIONS = Literal["fail", "append", "replace"]


YearDateLatest = NewType("YearDateLatest", str)


# DEPRECATED: use Origin instead
@pruned_json
@dataclass_json
@dataclass
class Source:
    """Notes on importing sources to grapher:
    - Field `source.description` gets mapped to `Internal notes`, but we rather use it for `additional_info`
    - The most important fields are `published_by` and `additional_info`
    - In admin for dataset (i.e. /admin/datasets/1234) only the first source of a dataset is shown and
        can be edited. The other ones are not visible.
    """

    name: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source_data_url: Optional[str] = None
    owid_data_url: Optional[str] = None
    date_accessed: Optional[str] = None
    publication_date: Optional[str] = None
    publication_year: Optional[int] = None
    # specific fields for grapher
    # NOTE: it's not clear how to map description & name to fields in grapher, so
    # we're keeping both for the time being. We might consolidate them in the future
    published_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Source":
        ...

    def update(self, **kwargs: Dict[str, Any]) -> None:
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)


@pruned_json
@dataclass_json
@dataclass
class Origin:
    # Dataset title written by OWID (without a year)
    dataset_title_owid: Optional[str] = None
    # Our description of the dataset
    dataset_description_owid: Optional[str] = None
    # The description for this dataset used by the producer
    dataset_description_producer: Optional[str] = None
    # The name of the institution (without a year) or the main authors of the paper
    producer: Optional[str] = None
    # The full citation that the producer asks for
    citation_producer: Optional[str] = None
    # The authorative URL of the dataset
    dataset_url_main: Optional[str] = None
    # Direct URL to download the dataset
    dataset_url_download: Optional[str] = None
    # Date when the dataset was accessed
    date_accessed: Optional[str] = None
    # Publication date or, if the exact date is not known, publication year
    date_published: Optional[YearDateLatest] = None

    def __post_init__(self):
        if self.date_published:
            # convert date to string
            if isinstance(self.date_published, dt.date):
                self.date_published = YearDateLatest(str(self.date_published))

            if self.date_published != "latest" and not is_year_or_date(self.date_published):
                raise ValueError("date_published should be either a year or a date or latest")

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Origin":
        ...

    def update(self, **kwargs: Dict[str, Any]) -> None:
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)


@pruned_json
@dataclass_json
@dataclass
class License:
    name: Optional[str] = None
    url: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "License":
        ...

    def __bool__(self):
        return bool(self.name or self.url)


@pruned_json
@dataclass_json
@dataclass
class VariableMeta:
    """Allowed fields for `display` attribute used for grapher:
        name
        zeroDay
        yearIsDay
        includeInTable
        numDecimalPlaces
        conversionFactor
        entityAnnotationsMap
    Fields `unit` and `shortUnit` are copied from attributes `unit` and `short_unit`
    on VariableMeta object

    NOTE: consider using its own object for `display` instead of dict and also possibly
    underscoring fields and converting them back to camelCase before inserting to grapher
    """

    title: Optional[str] = None
    description: Optional[str] = None
    sources: List[Source] = field(default_factory=list)
    origins: List[Origin] = field(default_factory=list)
    licenses: List[License] = field(default_factory=list)
    unit: Optional[str] = None
    short_unit: Optional[str] = None
    display: Optional[Dict[str, Any]] = None
    additional_info: Optional[Dict[str, Any]] = None
    processing_log: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "VariableMeta":
        ...

    def _repr_html_(self):
        # Render a nice display of the table metadata
        record = self.to_dict()
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-family: sans-serif; font-size: 1.5em; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">variable meta</p>
             {}
        """.format(
            getattr(self, "_name", None), to_html(record)
        )


@pruned_json
@dataclass_json
@dataclass
class DatasetMeta:
    """
    The metadata for this entire dataset kept in JSON (e.g. mydataset/index.json).

    The number of fields is limited, but should handle everything that we get from
    Walden. There is a lot more opportunity to store more metadata at the table and
    the variable level.
    """

    channel: Optional[str] = None
    namespace: Optional[str] = None
    # NOTE: short_name should be underscore and validate in setter, however this
    # is nontrivial to do with `dataclass_json` (see https://github.com/lidatong/dataclasses-json/issues/176)
    short_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    origins: List[Origin] = field(default_factory=list)
    # sources is deprecated, use origins instead
    sources: List[Source] = field(default_factory=list)
    licenses: List[License] = field(default_factory=list)
    is_public: bool = True
    additional_info: Optional[Dict[str, Any]] = None
    version: Optional[str] = None

    # an md5 checksum of the ingredients used to make this dataset
    source_checksum: Optional[str] = None

    def __post_init__(self) -> None:
        """Imply version from publication_date or publication_year if not given
        in __init__."""
        if self.version is None:
            if len(self.sources) == 1:
                (source,) = self.sources
                if source.publication_date:
                    self.version = str(source.publication_date)
                else:
                    self.version = str(source.publication_year)

    def save(self, filename: Union[str, Path]) -> None:
        filename = Path(filename).as_posix()
        with open(filename, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2, default=str)

    @classmethod
    def load(cls, filename: str) -> "DatasetMeta":
        with open(filename) as istream:
            return cls.from_dict(json.load(istream))

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DatasetMeta":
        ...

    def update_from_yaml(self, path: Union[Path, str], if_source_exists: SOURCE_EXISTS_OPTIONS = "fail") -> None:
        """The main reason for wanting to do this is to manually override what goes into Grapher before an export."""
        with open(path) as istream:
            annot = yaml.safe_load(istream)

        dataset_sources = annot.get("dataset", {}).get("sources", []) or []

        # update sources of dataset, if there are no sources in the new dataset, don't update existing ones
        if if_source_exists == "replace" and dataset_sources:
            self.sources = []

        new_sources = []
        for source_annot in dataset_sources:
            # if there's an existing source, update it
            ds_sources = [s for s in self.sources if s.name == source_annot["name"]]
            if ds_sources:
                ds_sources[0].update(**source_annot)
            # there is already a source in a dataset, raise an error
            elif self.sources and if_source_exists == "fail":
                raise ValueError(f"Source {self.sources[0].name} would be overwritten by source {source_annot['name']}")
            # otherwise append it
            else:
                new_sources.append(Source(**source_annot))

        self.sources.extend(new_sources)

        # update dataset
        for k, v in annot.get("dataset", {}).items():
            if k != "sources":
                setattr(self, k, v)

    @property
    def uri(self) -> str:
        """Return unique URI for this dataset if"""
        assert self.channel, "DatasetMeta.channel is not set"
        assert self.namespace, "DatasetMeta.namespace is not set"
        assert self.version, "DatasetMeta.version is not set"
        assert self.short_name, "DatasetMeta.short_name is not set"
        return f"{self.channel}/{self.namespace}/{self.version}/{self.short_name}"


@pruned_json
@dataclass_json
@dataclass
class TableMeta:
    # data about this table
    short_name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

    # a reference back to the dataset
    dataset: Optional[DatasetMeta] = field(compare=False, default=None)
    primary_key: List[str] = field(default_factory=list)

    @property
    def checked_name(self) -> str:
        if not self.short_name:
            raise Exception("table has no short_name")

        return self.short_name

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(dict: Dict[str, Any]) -> "TableMeta":
        ...

    def _repr_html_(self):
        # Render a nice display of the table metadata
        record = self.to_dict()
        short_name = record.pop("short_name")
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-family: sans-serif; font-size: 1.5em; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">table meta</p>
             {}
        """.format(
            short_name, to_html(record)
        )


def to_html(record: Any) -> Optional[str]:
    if isinstance(record, dict):
        rows = []
        for k, v in record.items():
            if not v:
                continue
            v_str = to_html(v)
            rows.append(
                """<tr><th style="text-align: right; font-family: sans-serif; vertical-align: top; padding: 0.2em 1em;"><strong>{}</strong></th><td style="text-align: left; padding: 0.2em 1em;">{}</td></tr>""".format(
                    k, v_str
                )
            )
        return '<table style="margin: 0em"><tbody>{}</tbody></table>'.format("".join(rows))

    elif isinstance(record, list):
        record = list(filter(None, record))
        if not record:
            return None

        rows = []
        for item in record:
            rows.append("<li>{}</li>".format(to_html(item)))
        return '<ul style="text-align: left; margin-top: 0em; margin-bottom: 0em">{}</ul>'.format("".join(rows))

    else:
        return str(record)


def is_year_or_date(s: str) -> bool:
    """Matches dates in "yyyy-mm-dd" format or years in "yyyy" format."""
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    year_pattern = r"^\d{4}$"

    if re.match(date_pattern, s) or re.match(year_pattern, s):
        return True
    else:
        return False
