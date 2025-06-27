#
#  meta.py
#
#  Metadata helpers.
#

import dataclasses
import datetime as dt
import json
import re
import sys
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Literal, NewType, TypedDict, TypeVar

# For Python 3.10 compatibility
if sys.version_info >= (3, 11):
    from typing import NotRequired, Required, Self
else:
    from typing_extensions import NotRequired, Required, Self

import mistune
import pandas as pd
from dataclasses_json import DataClassJsonMixin

from . import jinja
from .processing_log import ProcessingLog
from .utils import dataclass_from_dict, hash_any, parse_numeric_list, pruned_json

SOURCE_EXISTS_OPTIONS = Literal["fail", "append", "replace"]

VARIABLE_TYPE = Literal["float", "int", "mixed", "string", "ordinal", "categorical"]

YearDateLatest = NewType("YearDateLatest", str)


T = TypeVar("T")


class MetaBase(DataClassJsonMixin):
    def __hash__(self):
        """Hash that uniquely identifies an object (without needing frozen dataclass)."""
        return hash_any(self)

    def __eq__(self, other: Self) -> bool:  # type: ignore
        if not isinstance(other, self.__class__):
            return False
        return self.__hash__() == other.__hash__()

    def to_dict(self, encode_json: bool = False) -> dict[str, Any]:  # type: ignore
        return super().to_dict(encode_json=encode_json)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> T:  # type: ignore
        # NOTE: this is much faster than using dataclasses_json
        return dataclass_from_dict(cls, d)  # type: ignore

    def update(self, **kwargs: dict[str, Any]) -> None:
        """Update object with new values."""
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

    def copy(self, deep=True) -> Self:
        """Return a copy of the object."""
        if not deep:
            return dataclasses.replace(self)  # type: ignore
        else:
            return _deepcopy_dataclass(self)

    def save(self, filename: str | Path) -> None:
        filename = Path(filename).as_posix()
        with open(filename, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2, default=str)

    @classmethod
    def load(cls, filename: str) -> Self:
        with open(filename) as istream:
            return cls.from_dict(json.load(istream))


@pruned_json
@dataclass(eq=False)
class License(MetaBase):
    name: str | None = None
    url: str | None = None

    def __bool__(self):
        return bool(self.name or self.url)


# DEPRECATED: use Origin instead
@pruned_json
@dataclass(eq=False)
class Source(MetaBase):
    """Notes on importing sources to grapher:
    - Field `source.description` gets mapped to `Internal notes`, but we rather use it for `additional_info`
    - The most important fields are `published_by` and `additional_info`
    - In admin for dataset (i.e. /admin/datasets/1234) only the first source of a dataset is shown and
        can be edited. The other ones are not visible.
    """

    name: str | None = None
    description: str | None = None
    url: str | None = None
    source_data_url: str | None = None
    owid_data_url: str | None = None
    date_accessed: str | None = None
    publication_date: str | None = None
    publication_year: int | None = None
    # specific fields for grapher
    # NOTE: it's not clear how to map description & name to fields in grapher, so
    # we're keeping both for the time being. We might consolidate them in the future
    published_by: str | None = None


@pruned_json
@dataclass(eq=False)
class Origin(MetaBase):
    # Producer name
    # Name of the institution or the author(s) that produced the data product.
    producer: str
    # Title of the original data product
    title: str
    # Description of the data product
    description: str | None = None
    # Title of the snapshot
    # Subset of data that we extract from the data product. Only fill if it does not coincide with the title of the data product.
    title_snapshot: str | None = None
    # Description of the snapshot
    # Subset of data that we extract from the data product). Only when the data product and the snapshot do not coincide, the description_snapshot
    # will contain additional information to the description of the data product.
    description_snapshot: str | None = None
    # The full citation
    citation_full: str | None = None
    # These will be often empty and then producer is used instead, but for the (relatively common) cases
    # where the data product is more famous than the authors we would use this (e.g. VDEM instead of the first authors)
    attribution: str | None = None
    attribution_short: str | None = None
    # This is also often empty but if not then it will be part of the short citation (e.g. for VDEM)
    version_producer: str | None = None
    # The authorative URL of the dataset
    url_main: str | None = None
    # Direct URL to download the dataset
    url_download: str | None = None
    # Date when the dataset was accessed
    date_accessed: str | None = None
    # Publication date or, if the exact date is not known, publication year
    date_published: YearDateLatest | None = None
    # License of the dataset
    license: License | None = None

    def __post_init__(self):
        if self.date_published:
            # convert date or int to string
            if isinstance(self.date_published, (dt.date, int)):
                self.date_published = YearDateLatest(str(self.date_published))

            if self.date_published != "latest" and not is_year_or_date(self.date_published):
                raise ValueError("date_published should be either a year or a date or latest")


# Minor is for cases where we only harmonized the countries or similar
# Major is for cases where we do more, like create new aggregations, combine multiple indicators, etc.
PROCESSING_LEVELS = Literal["minor", "major"]

# Hierarchy of processing levels.
PROCESSING_LEVELS_ORDER = {
    "minor": 1,
    "major": 2,
}


@pruned_json
@dataclass(eq=False)
class FaqLink(MetaBase):
    gdoc_id: str
    fragment_id: str


GrapherConfig = dict[str, Any]


@pruned_json
@dataclass(eq=False)
class VariablePresentationMeta(MetaBase):
    # Any fields of grapher config can be set here - title and subtitle *should* be set whenever possible
    grapher_config: GrapherConfig | None = None
    # The text for the header of the data page
    title_public: str | None = None
    # Shown next to title to differentiate similar indicators e.g. "future projections" or "historical values"
    title_variant: str | None = None
    # Shown next to title to differentiate similar indicators e.g. "WHO" or "IHME"
    attribution_short: str | None = None
    # A short text to use to credit the source e.g. at the bottom of charts. Autofilled from the list of origins (see below). Semicolon separated if there are multiple.
    attribution: str | None = None
    # List of topic tags
    topic_tags: list[str] = field(default_factory=list)

    # Fields that are more work to add but of high value

    # List of google doc ids + fragment id
    faqs: list[FaqLink] = field(default_factory=list)


@pruned_json
@dataclass(eq=False)
class VariableMeta(MetaBase):
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

    title: str | None = None
    # This shouldn't be used for data pages, use `description_short`, `description_key` or `description_processing` instead
    description: str | None = None
    # A 1-2 sentence description - used internally or as fallback for description_key
    description_short: str | None = None
    # How did the origin describe this variable?
    description_from_producer: str | None = None
    # List of bullet points for the description key (can use markdown formatting)
    description_key: list[str] = field(default_factory=list)
    origins: list[Origin] = field(default_factory=list)  # Origins is the new replacement for sources
    # Use of `licenses` is discouraged, they should be captured in origins.
    licenses: list[License] = field(default_factory=list)
    unit: str | None = None
    short_unit: str | None = None
    # We keep display for the time being as the "less powerful sibling" of grapherConfig below
    display: dict[str, Any] | None = None
    additional_info: dict[str, Any] | None = None  # Only used for internal bookkeeping

    # How much processing did we do to this data?
    processing_level: PROCESSING_LEVELS | None = None
    # List of processing steps, in the future autogenerated
    processing_log: ProcessingLog = field(default_factory=ProcessingLog)

    presentation: VariablePresentationMeta | None = None

    # A short summary of what was done to process this indicator
    description_processing: str | None = None

    # This one is the license that we give the data. Normally it will be empty and then it will
    # be our usual license (CC-BY) but in cases where special restriction apply this is where
    # we would capture this.
    license: License | None = None

    # This is the old sources that we keep for compatibility. Use is strongly discouraged going forward
    sources: list[Source] = field(default_factory=list)

    # The type of the variable, automatically inferred from the data if empty
    type: VARIABLE_TYPE | None = None

    # List of categories for ordinal type indicators
    sort: list[str] = field(default_factory=list)

    # Dimensions
    # Dictionary of dimensions
    dimensions: dict[str, Any] | None = None
    # Original short name and title of the indicator before flattening
    original_short_name: str | None = None
    # TODO: it's possible that we might not need `original_title` at all
    original_title: str | None = None

    @property
    def schema_version(self) -> int:
        """Schema version is used to easily understand everywhere what metadata standard was used
        for authoring this variable metadata. Defaults to 1 for our legacy variables. "Modern" variables
        that fill in the presentation key and use origins should record 2 here.
        """
        if self.origins or self.presentation:
            return 2
        else:
            return 1

    def _repr_html_(self):
        # Render a nice display of the table metadata
        record = self.to_dict()
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-family: sans-serif; font-size: 1.5em; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">variable meta</p>
             {}
        """.format(getattr(self, "_name", None), to_html(record))

    def render(self, dim_dict: dict[str, Any], remove_dods: bool = False) -> "VariableMeta":
        """Render Jinja in all fields of VariableMeta. Return a new VariableMeta object.

        :param dim_dict: dictionary of dimensions to render
        :param remove_dods: remove references to details on demand from a text

        Usage:
            from owid.catalog import Dataset
            from etl import paths

            ds = Dataset(paths.DATA_DIR / "garden/emissions/2025-02-12/ceds_air_pollutants")
            tb = ds['ceds_air_pollutants']
            tb.emissions.m.render({'pollutant': 'CO', 'sector': 'Transport'})
        """
        meta = jinja._expand_jinja(self.copy(), dim_dict, remove_dods=remove_dods)

        meta = update_variable_metadata(meta)

        return meta

    def copy(self, deep=True) -> Self:
        m = super().copy(deep)
        m._name = getattr(self, "_name", None)  # type: ignore
        return m


@pruned_json
@dataclass(eq=False)
class DatasetMeta(MetaBase):
    """
    The metadata for this entire dataset kept in JSON (e.g. mydataset/index.json).

    The number of fields is limited, but should handle everything that we get from
    Snapshot. There is a lot more opportunity to store more metadata at the table and
    the variable level.
    """

    channel: str | None = None
    namespace: str | None = None
    # NOTE: short_name should be underscore and validate in setter, however this
    # is nontrivial to do with `dataclass_json` (see https://github.com/lidatong/dataclasses-json/issues/176)
    short_name: str | None = None
    title: str | None = None
    description: str | None = None
    # sources is deprecated, use origins on indicator level instead
    sources: list[Source] = field(default_factory=list)
    licenses: list[License] = field(default_factory=list)
    is_public: bool = True
    additional_info: dict[str, Any] | None = None
    version: str | None = None
    # update period in days
    update_period_days: int | None = None
    # prohibit redistribution (disable chart download)
    non_redistributable: bool = False

    # an md5 checksum of the ingredients used to make this dataset
    source_checksum: str | None = None

    def __post_init__(self) -> None:
        """Imply version from publication_date or publication_year if not given
        in __init__."""
        if self.version is None:
            if len(self.sources) == 1:
                (source,) = self.sources
                if source.publication_date:
                    self.version = str(source.publication_date)
                elif source.publication_year:
                    self.version = str(source.publication_year)
                else:
                    self.version = None

    def _params_yaml(self) -> dict:
        """Parameters passed to YAML for dynamic interpolation."""
        params = {}
        if self.version and self.version != "latest":
            params["YEAR"] = pd.to_datetime(self.version).year
        return params

    def update_from_yaml(
        self,
        path: Path | str,
        if_source_exists: SOURCE_EXISTS_OPTIONS = "fail",
    ) -> None:
        """The main reason for wanting to do this is to manually override what goes into Grapher before an export."""
        from owid.catalog import utils

        annot = utils.dynamic_yaml_load(path, self._params_yaml())

        # None is treated differently from [] - if given empty list, we clear sources
        dataset_sources = annot.get("dataset", {}).get("sources")
        if dataset_sources is not None:
            # update sources of dataset, if there are no sources in the new dataset, don't update existing ones
            if if_source_exists == "replace":
                self.sources = []

            new_sources = []
            for source_annot in dataset_sources:
                # if there's an existing source, update it
                ds_sources = [s for s in self.sources if s.name == source_annot["name"]]
                if ds_sources:
                    ds_sources[0].update(**source_annot)
                # there is already a source in a dataset, raise an error
                elif self.sources and if_source_exists == "fail":
                    raise ValueError(
                        f"Source {self.sources[0].name} would be overwritten by source {source_annot['name']}"
                    )
                # otherwise append it
                else:
                    new_sources.append(Source(**source_annot))

            self.sources.extend(new_sources)

        # update dataset
        for k, v in annot.get("dataset", {}).items():
            if k not in ("sources",):
                setattr(self, k, v)

    @property
    def uri(self) -> str:
        """Return unique URI for this dataset if"""
        assert self.channel, "DatasetMeta.channel is not set"
        assert self.namespace, "DatasetMeta.namespace is not set"
        assert self.version, "DatasetMeta.version is not set"
        assert self.short_name, "DatasetMeta.short_name is not set"
        return f"{self.channel}/{self.namespace}/{self.version}/{self.short_name}"


class TableDimension(TypedDict):
    name: Required[str]
    slug: Required[str]
    description: NotRequired[str | None]


@pruned_json
@dataclass(eq=False)
class TableMeta(MetaBase):
    # data about this table
    short_name: str | None = None
    title: str | None = None
    description: str | None = None

    # a reference back to the dataset
    dataset: DatasetMeta | None = field(compare=False, default=None)
    primary_key: list[str] = field(default_factory=list)

    # table dimensions
    dimensions: list[TableDimension] | None = None

    @property
    def checked_name(self) -> str:
        if not self.short_name:
            raise Exception("table has no short_name")

        return self.short_name

    def _repr_html_(self):
        # Render a nice display of the table metadata
        record = self.to_dict()
        short_name = record.pop("short_name")
        return """
             <h2 style="margin-bottom: 0em"><pre>{}</pre></h2>
             <p style="font-variant: small-caps; font-family: sans-serif; font-size: 1.5em; color: grey; margin-top: -0.2em; margin-bottom: 0.2em">table meta</p>
             {}
        """.format(short_name, to_html(record))

    @property
    def uri(self) -> str:
        """Return unique URI for this table."""
        assert self.dataset, "TableMeta.dataset is not set"
        assert self.short_name, "TableMeta.short_name is not set"
        dataset_uri = self.dataset.uri.rstrip("/")
        return f"{dataset_uri}/{self.short_name}"


def to_html(record: Any) -> str | None:
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
            rows.append(f"<li>{to_html(item)}</li>")
        return '<ul style="text-align: left; margin-top: 0em; margin-bottom: 0em">{}</ul>'.format("".join(rows))

    else:
        return mistune.html(str(record))  # type: ignore


def is_year_or_date(s: str) -> bool:
    """Matches dates in "yyyy-mm-dd" format or years in "yyyy" format."""
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    year_pattern = r"^\d{4}$"

    if re.match(date_pattern, s) or re.match(year_pattern, s):
        return True
    else:
        return False


def _deepcopy_dataclass(dc) -> Any:
    """Create a deep copy of a dataclass. This is much faster than running copy.deepcopy."""
    dc = dataclasses.replace(dc)
    for k, v in dc.__dict__.items():
        if is_dataclass(v):
            setattr(dc, k, _deepcopy_dataclass(v))
        elif isinstance(v, list):
            lis = [_deepcopy_dataclass(x) if is_dataclass(x) else x for x in v]
            # make sure to preserve the type of the list if we subclass it
            if type(v) != list:  # noqa
                lis = type(v)(lis)
            setattr(dc, k, lis)
        elif isinstance(v, dict):
            setattr(dc, k, {x: _deepcopy_dataclass(y) if is_dataclass(y) else y for x, y in v.items()})
        else:
            pass
    return dc


def update_variable_metadata(meta: VariableMeta) -> VariableMeta:
    """Post-process variable metadata and fix issues before rendering or exporting to grapher.
    Things like converting strings to numbers, removing empty fields, post-processing jinja
    rendering, etc.
    """
    # Grapher uses units from field `display` instead of fields `unit` and `short_unit`
    # before we fix grapher data model, copy them to `display`.
    meta.display = meta.display or {}

    # Copy unit and short_unit to display if they exist
    if meta.short_unit:
        meta.display.setdefault("shortUnit", meta.short_unit)
    if meta.unit:
        meta.display.setdefault("unit", meta.unit)

    # Convert numDecimalPlaces from string to int if needed
    if meta.display and isinstance(meta.display.get("numDecimalPlaces"), str):
        meta.display["numDecimalPlaces"] = int(meta.display["numDecimalPlaces"])

    # Prune empty fields from description_key
    if meta.description_key:
        meta.description_key = [x for x in meta.description_key if x.strip()]

    # Convert from string to proper type when it comes from YAML
    grapher_config = getattr(getattr(meta, "presentation", None), "grapher_config", {}) or {}
    color_scale = grapher_config.get("map", {}).get("colorScale", {})

    # Convert strings to lists when needed
    gconf = getattr(meta.presentation, "grapher_config", None)
    if gconf:
        try:
            color_scale = gconf["map"]["colorScale"]
            if isinstance(color_scale["customNumericValues"], str):
                color_scale["customNumericValues"] = parse_numeric_list(color_scale["customNumericValues"])
        except KeyError:
            pass

    # Prune faqs with empty fragment_id
    if meta.presentation and meta.presentation.faqs:
        faqs: list[FaqLink] = []
        for faq in meta.presentation.faqs:
            if not faq.fragment_id.strip():
                continue
            else:
                faqs.append(faq)
        meta.presentation.faqs = faqs

    return meta
