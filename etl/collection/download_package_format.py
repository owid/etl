"""Python port of owid-grapher's download-package formatting code.

    !!! KEEP IN SYNC WITH owid-grapher !!!

    This module is a deliberate, line-for-line port of the citation / readme /
    metadata.json formatting that owid-grapher uses to build a chart's own
    `.zip` download. It exists because the "complete dataset" package for
    MDIMs is built once here at ETL publish time and stored in R2, rather
    than assembled per-request by a Cloudflare Function -- see the module
    docstring in `download_package.py` for why.

    The upstream sources, all in owid-grapher:

      * packages/@ourworldindata/utils/src/metadataHelpers.ts
            getOriginAttributionFragments, getAttributionFragmentsFromVariable,
            getETLPathComponents, getLastUpdatedFromVariable,
            getNextUpdateFromVariable, getPhraseForProcessingLevel,
            prepareSourcesForDisplay, getYearSuffixFromOrigin,
            getCitationShort, getCitationLong, formatSourceDate, getDateRange
      * functions/_common/readmeTools.ts
            getTitle, getAttribution, getSource, getDescription,
            getKeyDataLines, getCitationLines, getDescriptionLines,
            getSources, getDataProcessingLines, columnReadmeText
      * functions/_common/metadataTools.ts
            the metadata.json column shape and titleLong construction
      * packages/@ourworldindata/utils/src/Util.ts
            stripDetailOnDemandLinks

    Each function below names its upstream counterpart. If you change the
    wording, the field set, or the ordering on either side, change it on both
    -- a chart download and an MDIM complete-dataset download are supposed to
    be the same format, and nothing automated will notice if they drift.

Input shape: every function here takes the public indicator metadata dict as
served at `api.ourworldindata.org/v1/indicators/<id>.metadata.json` (the same
JSON the Cloudflare Function fetches, obtained here via
`etl.grapher.io.load_variable_metadata`). That is what makes this a pure
formatting port -- both sides start from byte-identical input.
"""

from __future__ import annotations

import calendar
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

# JS `new Date().toISOString()` and Cloudflare Workers both operate in UTC;
# every date below is therefore treated as a naive UTC date so the port can't
# shift a day depending on where ETL happens to run.
MONTH_NAMES = list(calendar.month_name)[1:]


# ---------------------------------------------------------------------------
# Small helpers standing in for lodash / dayjs
# ---------------------------------------------------------------------------


def _uniq(values: list[str]) -> list[str]:
    """lodash `_.uniq` -- de-duplicate, preserving first-seen order."""
    seen: set[str] = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _uniq_by_label(items: list[dict]) -> list[dict]:
    """lodash `_.uniqBy(items, "label")`."""
    seen: set[Any] = set()
    out = []
    for item in items:
        label = item.get("label")
        if label not in seen:
            seen.add(label)
            out.append(item)
    return out


def _compact(values: list[Any]) -> list[Any]:
    """lodash `_.compact` -- drop falsy values (None and "" both go)."""
    return [v for v in values if v]


def _exclude_undefined(values: list[Any]) -> list[Any]:
    """`excludeUndefined` -- drops undefined only, so "" survives."""
    return [v for v in values if v is not None]


def _parse_date(value: str | None, formats: tuple[str, ...]) -> date | None:
    if not value:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _add_months(d: date, months: int) -> date:
    """dayjs `.add(n, "month")` -- clamps the day to the target month's length."""
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def format_source_date(value: str | None, fmt: str) -> str | None:
    """`formatSourceDate` (metadataHelpers.ts).

    dayjs is asked to parse with ["YYYY-MM-DD", "DD/MM/YYYY"] and falls back to
    returning the raw string when it can't. `fmt` is a dayjs format token
    string; only the two the readme actually uses are supported.
    """
    parsed = _parse_date(value, ("%Y-%m-%d", "%d/%m/%Y"))
    if parsed is None:
        return value or None
    if fmt == "MMMM D, YYYY":
        return f"{MONTH_NAMES[parsed.month - 1]} {parsed.day}, {parsed.year}"
    if fmt == "MMMM YYYY":
        return f"{MONTH_NAMES[parsed.month - 1]} {parsed.year}"
    raise ValueError(f"Unsupported dayjs format token: {fmt}")


# ---------------------------------------------------------------------------
# metadataHelpers.ts
# ---------------------------------------------------------------------------


def strip_detail_on_demand_links(text: str) -> str:
    """`stripDetailOnDemandLinks` (Util.ts) -- `[label](#dod:key)` -> `label`."""
    return re.sub(r"\[([^\]]+)\]\(#dod:([A-Za-z0-9_-]+)\)", r"\1", text)


_MARKDOWN_LIST_MARKER = re.compile(r"^([-*+]|\d+[.)])\s")


def _collapse_description_key_item(item: str) -> str:
    """`collapseDescriptionKeyItem` (OwidVariable.ts)."""
    parts: list[str] = []
    for raw_line in item.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if not parts:
            parts.append(line)
        elif _MARKDOWN_LIST_MARKER.match(line):
            parts.append("\n  " + line)
        else:
            parts.append(" " + line)
    return "".join(parts)


def normalize_description_key(value: str | list[str] | None) -> str | None:
    """`normalizeDescriptionKey` (OwidVariable.ts).

    descriptionKey used to be an array of bullet points and is now free-form
    markdown, but persisted indicator metadata JSON on R2 still carries arrays
    -- which is exactly what this module reads. Grapher normalizes at every
    ingress point; so must we, or the readme gets a Python list repr where a
    bulleted list belongs.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    items = [item.strip() for item in value if item.strip()]
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return "\n".join(f"- {_collapse_description_key_item(item)}" for item in items)


def get_origin_attribution_fragments(origins: list[dict] | None) -> list[str]:
    """`getOriginAttributionFragments`."""
    if not origins:
        return []
    fragments = []
    for origin in origins:
        published = _parse_date(origin.get("datePublished"), ("%Y-%m-%d", "%Y"))
        year_suffix = f" ({published.year})" if published else ""
        attribution = origin.get("attribution")
        # `??` in the original: an explicit empty string is kept as-is.
        fragments.append(attribution if attribution is not None else f"{origin.get('producer')}{year_suffix}")
    return fragments


def get_attribution_fragments_from_variable(variable: dict) -> list[str]:
    """`getAttributionFragmentsFromVariable`."""
    attribution = (variable.get("presentation") or {}).get("attribution")
    if attribution:
        return [attribution]
    origin_fragments = get_origin_attribution_fragments(variable.get("origins"))
    name = (variable.get("source") or {}).get("name")
    return _uniq(_compact([name, *origin_fragments]))


def get_etl_path_version(catalog_path: str) -> str | None:
    """`getETLPathComponents(...).version` -- third `/`-separated segment."""
    parts = catalog_path.split("/")
    return parts[2] if len(parts) > 2 else None


def get_last_updated_from_variable(variable: dict) -> str | None:
    """`getLastUpdatedFromVariable`."""
    version = get_etl_path_version(variable.get("catalogPath") or "")
    if version and re.fullmatch(r"\d{4}-\d{2}-\d{2}", version):
        return version

    origin_dates = _exclude_undefined([o.get("dateAccessed") for o in (variable.get("origins") or [])])
    parsed = [d for d in (_parse_date(v, ("%Y-%m-%d",)) for v in origin_dates) if d is not None]
    if not parsed:
        return None
    return max(parsed).strftime("%Y-%m-%d")


def get_next_update_from_variable(variable: dict, today: date) -> str | None:
    """`getNextUpdateFromVariable`.

    `today` is passed in rather than read from the clock so a single package
    build is internally consistent (and so tests are deterministic).
    """
    update_period_days = variable.get("updatePeriodDays")
    if not update_period_days:
        return None
    last_updated = get_last_updated_from_variable(variable)
    # dayjs(undefined) is "now" in the original.
    base = _parse_date(last_updated, ("%Y-%m-%d",)) or today
    next_update = base + timedelta(days=update_period_days)
    if next_update < today:
        next_update = _add_months(today, 1)
    return next_update.strftime("%Y-%m-%d")


def get_phrase_for_processing_level(processing_level: str | None) -> str:
    """`getPhraseForProcessingLevel`."""
    if processing_level == "major":
        return "with major processing"
    if processing_level == "minor":
        return "with minor processing"
    return "processed"


def _prepare_origin_for_display(origin: dict) -> dict:
    """`prepareOriginForDisplay`."""
    label = origin.get("producer") or ""
    title = origin.get("title")
    if title and title != label:
        label += " – " + title
    return {
        "label": label,
        "description": origin.get("description"),
        "retrievedOn": origin.get("dateAccessed"),
        "retrievedFrom": origin.get("urlMain"),
        "citation": origin.get("citationFull"),
    }


def prepare_sources_for_display(variable: dict) -> list[dict]:
    """`prepareSourcesForDisplay`."""
    source = variable.get("source") or {}
    origins = variable.get("origins") or []

    sources: list[dict] = []
    if source.get("name") and (source.get("dataPublishedBy") or source.get("retrievedDate") or source.get("link")):
        sources.append(
            {
                "label": source.get("name"),
                "dataPublishedBy": source.get("dataPublishedBy"),
                "retrievedOn": source.get("retrievedDate"),
                "retrievedFrom": source.get("link"),
            }
        )
    sources.extend(_prepare_origin_for_display(origin) for origin in origins)
    return sources


def _year_suffix_from_origin(origin: dict) -> str:
    """`getYearSuffixFromOrigin`."""
    parsed = _parse_date(origin.get("dateAccessed"), ("%Y-%m-%d", "%Y")) or _parse_date(
        origin.get("datePublished"), ("%Y-%m-%d", "%Y")
    )
    return f" ({parsed.year})" if parsed else ""


def get_citation_short(origins: list[dict], attributions: list[str] | None, processing_level: str | None) -> str:
    """`getCitationShort`."""
    producers_with_year = _uniq([f"{o.get('producer')}{_year_suffix_from_origin(o)}" for o in origins])
    phrase = get_phrase_for_processing_level(processing_level)

    fragments = attributions if attributions is not None else producers_with_year
    shortened = f"{fragments[0]} and other sources" if len(fragments) > 3 else "; ".join(fragments)
    return f"{shortened} – {phrase} by Our World in Data"


def get_citation_long(
    indicator_title: dict,
    origins: list[dict],
    source: dict,
    attributions: list[str] | None,
    attribution_short: str | None,
    title_variant: str | None,
    processing_level: str | None,
) -> str:
    """`getCitationLong`.

    The upstream signature also takes `citationUrl` and `archivalDate`; both
    are passed as undefined for download packages, which drops the trailing
    "Retrieved <today> from ..." clause. Omitted here rather than carried as
    dead parameters -- and worth knowing that it's what keeps this artifact
    genuinely static: no per-download date leaks into a citation.
    """
    if attribution_short and title_variant:
        source_short_name = f"{attribution_short} – {title_variant}"
    else:
        source_short_name = attribution_short or title_variant

    producers_with_year = _uniq([f"{o.get('producer')}{_year_suffix_from_origin(o)}" for o in origins])
    phrase = get_phrase_for_processing_level(processing_level)

    fragments = attributions if attributions is not None else producers_with_year
    citation_longer = f"{'; '.join(fragments)} – {phrase} by Our World in Data"
    title_with_fragments = " – ".join(_exclude_undefined([indicator_title.get("title"), source_short_name]))
    origins_long = "; ".join(
        _uniq(
            [
                "{}, “{}{}”".format(
                    o.get("producer"),
                    o.get("title") if o.get("title") is not None else o.get("titleSnapshot"),
                    " " + o["versionProducer"] if o.get("versionProducer") else "",
                )
                for o in origins
            ]
        )
    )

    if origins_long:
        original_data = f"{origins_long} [original data]."
    elif source.get("name"):
        original_data = f"{source['name']} [original data]."
    else:
        original_data = None

    return " ".join(
        _exclude_undefined(
            [
                f"{citation_longer}.",
                f"“{title_with_fragments}” [dataset].",
                original_data,
            ]
        )
    )


def get_date_range(date_range: str) -> str | None:
    """`getDateRange` -- "1990-2020" -> "1990–2020", with BCE/CE handling."""
    match = re.fullmatch(r"\s*(?P<start>-?\d+)\s*[-–]\s*(?P<end>-?\d+)\s*", date_range)
    if not match:
        return None
    first_year = int(match.group("start"))
    last_year = int(match.group("end"))

    formatted_first = f"{abs(first_year)} BCE" if first_year < 0 else str(first_year)
    if last_year < 0:
        formatted_last = f"{abs(last_year)} BCE"
    elif first_year < 0:
        formatted_last = f"{last_year} CE"
    else:
        formatted_last = str(last_year)

    if last_year < 0 or first_year < 0:
        return f"{formatted_first} – {formatted_last}"
    return f"{formatted_first}–{formatted_last}"


# ---------------------------------------------------------------------------
# The "column" abstraction
#
# Upstream these functions take a CoreColumn backed by a live OwidTable, but
# only ever read four things off it (.def, .source, .titlePublicOrDisplayName,
# .unitConversionFactor) -- see the toColumnShim comment in
# owid-grapher's mdimDownloadFunctions.ts. This dataclass is that same shim.
# ---------------------------------------------------------------------------


@dataclass
class IndicatorColumn:
    """A single indicator's public metadata, plus grapher's derived fields."""

    meta: dict

    @property
    def presentation(self) -> dict:
        return self.meta.get("presentation") or {}

    @property
    def display(self) -> dict:
        return self.meta.get("display") or {}

    @property
    def source(self) -> dict:
        return self.meta.get("source") or {}

    @property
    def source_name(self) -> str | None:
        return self.source.get("name")

    @property
    def processing_level(self) -> str | None:
        return self.meta.get("processingLevel")

    @property
    def title_public_or_display_name(self) -> dict:
        """`CoreColumn.titlePublicOrDisplayName` (CoreTableColumns.ts).

        The attribution fragments ride along ONLY when `titlePublic` is set;
        without it the title is a bare display name with no fragments at all.
        Easy to get wrong -- the Cloudflare Function this module replaced set
        them unconditionally, which appended a spurious
        " – HYDE, Gapminder, UN – Long-run data" to every indicator that has an
        attributionShort but no titlePublic. Caught by diffing against
        production's own /grapher/<slug>.readme.md.
        """
        title_public = self.presentation.get("titlePublic")
        if title_public:
            return {
                "title": title_public,
                "attributionShort": self.presentation.get("attributionShort"),
                "titleVariant": self.presentation.get("titleVariant"),
            }
        return {
            "title": self.display.get("name") or self.meta.get("name") or "",
            "attributionShort": None,
            "titleVariant": None,
        }

    @property
    def unit_conversion_factor(self) -> float | None:
        return self.display.get("conversionFactor")

    @property
    def description_key(self) -> str | None:
        return normalize_description_key(self.meta.get("descriptionKey"))

    @property
    def attribution_fragments(self) -> list[str]:
        return get_attribution_fragments_from_variable(self.meta)

    def citation_short(self) -> str:
        return get_citation_short(self.meta.get("origins") or [], self.attribution_fragments, self.processing_level)

    def citation_long(self) -> str:
        return get_citation_long(
            self.title_public_or_display_name,
            self.meta.get("origins") or [],
            self.source,
            self.attribution_fragments,
            self.presentation.get("attributionShort"),
            self.presentation.get("titleVariant"),
            self.processing_level,
        )


# ---------------------------------------------------------------------------
# readmeTools.ts
# ---------------------------------------------------------------------------

MARKDOWN_NEWLINE_ENDING = "  "


def get_title(col: IndicatorColumn) -> str:
    """`getTitle` -- the readme's per-indicator heading."""
    parts = col.title_public_or_display_name
    title = parts["title"]
    attribution_short = parts["attributionShort"]
    title_variant = parts["titleVariant"]
    if attribution_short and title_variant:
        return f"{title} – {title_variant} – {attribution_short}"
    if title_variant:
        return f"{title} – {title_variant}"
    if attribution_short:
        return f"{title} – {attribution_short}"
    return title


def get_attribution(col: IndicatorColumn) -> str:
    """`getAttribution`."""
    attribution = ", ".join(col.attribution_fragments)
    if attribution == "":
        return col.source_name or ""
    return attribution


def get_source(attribution: str, col: IndicatorColumn) -> str:
    """`getSource`."""
    if attribution.lower() != "our world in data":
        phrase = get_phrase_for_processing_level(col.processing_level)
        return f"{attribution} – {phrase} by Our World In Data"
    return attribution


def _description_lines(col: IndicatorColumn) -> list[str]:
    """`getDescription` -- descriptionShort or description, split into lines."""
    description = col.meta.get("descriptionShort") or col.meta.get("description")
    if not description:
        return []
    return [line.strip() for line in description.split("\n")]


def _key_data_lines(col: IndicatorColumn, today: date) -> list[str]:
    """`getKeyDataLines`."""
    lines = []

    last_updated = get_last_updated_from_variable(col.meta)
    if last_updated:
        lines.append(f"Last updated: {format_source_date(last_updated, 'MMMM D, YYYY')}" + MARKDOWN_NEWLINE_ENDING)

    next_update = get_next_update_from_variable(col.meta, today)
    if next_update:
        lines.append(f"Next update: {format_source_date(next_update, 'MMMM YYYY')}" + MARKDOWN_NEWLINE_ENDING)

    timespan = col.meta.get("timespan")
    date_range = get_date_range(timespan) if timespan else None
    if date_range:
        lines.append(f"Date range: {date_range}" + MARKDOWN_NEWLINE_ENDING)

    unit = col.meta.get("unit")
    if unit:
        lines.append(f"Unit: {unit}" + MARKDOWN_NEWLINE_ENDING)

    factor = col.unit_conversion_factor
    if factor and factor != 1:
        lines.append(f"Unit conversion factor: {_format_js_number(factor)}" + MARKDOWN_NEWLINE_ENDING)

    return lines


def _format_js_number(value: float) -> str:
    """JS template-literal number formatting: 1000 prints as "1000", not "1000.0"."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _citation_lines(col: IndicatorColumn) -> list[str]:
    """`getCitationLines`.

    Note the upstream quirk faithfully reproduced here: this block rebuilds the
    attribution fragments from `{...def, source: {name: def.sourceName}}` --
    i.e. with the source reduced to just its name -- rather than reusing the
    variable's full source object. It happens to produce the same fragments,
    since only `source.name` is ever read, but don't "simplify" it away
    without checking the TS again.
    """
    citation_short = get_citation_short(
        col.meta.get("origins") or [],
        get_attribution_fragments_from_variable({**col.meta, "source": {"name": col.source_name}}),
        col.processing_level,
    )
    return [
        "",
        "### How to cite this data",
        "",
        "#### In-line citation",
        "If you have limited space (e.g. in data visualizations), you can use this abbreviated in-line citation:"
        + MARKDOWN_NEWLINE_ENDING,
        citation_short,
        "",
        "#### Full citation",
        col.citation_long(),
    ]


def _description_section_lines(col: IndicatorColumn, attribution: str) -> list[str]:
    """`getDescriptionLines`."""
    lines = []
    description_key = col.description_key
    if description_key:
        lines += ["", "### What you should know about this data", description_key.strip()]

    description_from_producer = col.meta.get("descriptionFromProducer")
    if description_from_producer:
        lines += [
            "",
            f"### How is this data described by its producer - {attribution}?",
            description_from_producer.strip(),
        ]

    additional_info = col.meta.get("additionalInfo")
    if additional_info:
        lines += ["", "### Additional information about this data", additional_info.strip()]

    return lines


def _sources_lines(col: IndicatorColumn) -> list[str]:
    """`getSources`."""
    sources = _uniq_by_label(prepare_sources_for_display(col.meta))
    if not sources:
        return []

    lines = ["", "### Source" if len(sources) == 1 else "### Sources"]
    for source in sources:
        lines += ["", f"#### {source['label']}"]
        if source.get("dataPublishedBy"):
            lines.append(f"Data published by: {source['dataPublishedBy'].strip()}" + MARKDOWN_NEWLINE_ENDING)
        if source.get("retrievedOn"):
            lines.append(f"Retrieved on: {source['retrievedOn'].strip()}" + MARKDOWN_NEWLINE_ENDING)
        if source.get("retrievedFrom"):
            lines.append(f"Retrieved from: {source['retrievedFrom'].strip()}" + MARKDOWN_NEWLINE_ENDING)
    return lines


def _data_processing_lines(col: IndicatorColumn) -> list[str]:
    """`getDataProcessingLines`."""
    description_processing = col.meta.get("descriptionProcessing")
    if not description_processing:
        return []
    return ["", "#### Notes on our processing step for this indicator", description_processing]


def column_readme_text(col: IndicatorColumn, today: date) -> list[str]:
    """`columnReadmeText` -- one indicator's full readme section, as lines."""
    attribution = get_attribution(col)
    return [
        "",
        f"## {get_title(col)}",
        *_description_lines(col),
        *_key_data_lines(col, today),
        "",
        *_citation_lines(col),
        f"Source: {get_source(attribution, col)}",
        *_description_section_lines(col, attribution),
        *_sources_lines(col),
        *_data_processing_lines(col),
        "",
    ]


# ---------------------------------------------------------------------------
# metadataTools.ts
# ---------------------------------------------------------------------------


def variable_type_to_column_type(variable_type: str | None) -> str:
    """`variableTypeToColumnType` (LegacyToOwidTable.ts) -- "float" -> "Numeric"."""
    return {
        "ordinal": "Ordinal",
        "string": "String",
        "int": "Integer",
        "float": "Numeric",
    }.get(variable_type or "", "NumberOrString")


def compute_title_long(col: IndicatorColumn) -> str:
    """`titleLong` construction in `assembleMetadata`.

    Deliberately not the same as `get_title()`: this one uses a plain " - "
    before the modifier and an en-dash inside it. Both appear in a real
    download, in different places.
    """
    parts = col.title_public_or_display_name
    attribution_short = parts["attributionShort"]
    title_variant = parts["titleVariant"]
    if attribution_short and title_variant:
        attribution_string = f"{attribution_short} – {title_variant}"
    else:
        attribution_string = attribution_short or title_variant
    if attribution_string:
        return f"{parts['title']} - {attribution_string}"
    return parts["title"]


def metadata_column_entry(col: IndicatorColumn, variable_id: int, full_metadata_url: str, today: date) -> dict:
    """One entry of metadata.json's "columns" object.

    Keys are emitted in the same order as the TS object literal, and keys whose
    value is None are dropped -- `JSON.stringify` omits undefined properties,
    so keeping them would show up as a diff against a real chart download.
    """
    entry = {
        "titleShort": col.title_public_or_display_name["title"],
        "titleLong": compute_title_long(col),
        "descriptionShort": col.meta.get("descriptionShort"),
        "descriptionKey": col.description_key,
        "descriptionProcessing": col.meta.get("descriptionProcessing"),
        "unit": col.meta.get("unit"),
        "shortUnit": col.meta.get("shortUnit"),
        "timespan": col.meta.get("timespan"),
        "type": variable_type_to_column_type(col.meta.get("type")),
        "owidVariableId": variable_id,
        "shortName": col.meta.get("shortName"),
        "lastUpdated": get_last_updated_from_variable(col.meta),
        "nextUpdate": get_next_update_from_variable(col.meta, today),
        "citationShort": col.citation_short(),
        "citationLong": col.citation_long(),
        "fullMetadata": full_metadata_url,
    }
    return {k: v for k, v in entry.items() if v is not None}


def dumps_like_json_stringify(value: Any) -> str:
    """`JSON.stringify(value, undefined, 2)` -- 2-space indent, no ASCII escaping.

    Also applies `stripDetailOnDemandLinks` to every string, matching the
    `_.cloneDeepWith` pass at the end of `assembleMetadata`.
    """

    def strip(node: Any) -> Any:
        if isinstance(node, str):
            return strip_detail_on_demand_links(node)
        if isinstance(node, dict):
            return {k: strip(v) for k, v in node.items()}
        if isinstance(node, list):
            return [strip(v) for v in node]
        return node

    return json.dumps(strip(value), indent=2, ensure_ascii=False)
