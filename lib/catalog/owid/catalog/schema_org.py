"""Map OWID catalog metadata to Schema.org Dataset JSON-LD."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import jinja2

from owid.catalog.core.jinja import _expand_jinja_text, _uses_jinja
from owid.catalog.core.meta import (
    DatasetMeta,
    License,
    Origin,
    TableMeta,
    VariableMeta,
    description_key_to_string,
)
from owid.catalog.core.utils import remove_details_on_demand

DEFAULT_CATALOG_BASE_URL = "https://catalog.ourworldindata.org"
DEFAULT_LOGO_URL = "https://ourworldindata.org/owid-logo.svg"
DEFAULT_THUMBNAIL_URL = DEFAULT_LOGO_URL
MAX_VARIABLES_MEASURED = 100
MAX_DIMENSION_VALUES_LISTED = 40
# Dimensions that every table has and that are already conveyed by temporalCoverage /
# spatialCoverage — not worth a PropertyValue of their own.
ENTITY_TIME_DIMENSIONS = {"country", "year", "date"}
KNOWN_LICENSE_URLS = {
    "CC BY 4.0": "https://creativecommons.org/licenses/by/4.0/",
    "CC-BY 4.0": "https://creativecommons.org/licenses/by/4.0/",
    "Creative Commons Attribution 4.0 International": "https://creativecommons.org/licenses/by/4.0/",
}


@dataclass
class TableSchemaInput:
    """Metadata needed to describe one catalog table as Schema.org."""

    short_name: str
    metadata: TableMeta
    variables: dict[str, VariableMeta]
    formats: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    temporal_coverage: str | None = None
    spatial_coverage: str | None = None
    # For long-format tables: distinct values per dimension column (slug -> sorted values)
    # and one representative dimension combination observed in the data, used to render
    # Jinja-templated variable metadata into an example instead of leaking the raw template.
    dimension_values: dict[str, list[Any]] = field(default_factory=dict)
    representative_dimensions: dict[str, Any] = field(default_factory=dict)


def dataset_to_schema_org(
    *,
    dataset_path: str,
    page_path: str,
    version: str | None = None,
    dataset_meta: DatasetMeta,
    tables: list[TableSchemaInput],
    base_url: str = DEFAULT_CATALOG_BASE_URL,
) -> dict[str, Any]:
    """Convert one catalog dataset folder to Schema.org Dataset JSON-LD.

    The catalog folder is the top-level Dataset. For single-table datasets, table
    metadata is flattened into the top-level Dataset. For multi-table datasets,
    each table is represented as a nested Dataset via ``hasPart``.

    ``page_path`` is the stable, version-agnostic short key (``"<namespace>/<dataset>"``)
    used for the public landing page URL and ``@id``. ``dataset_path`` is the full,
    dated catalog path (``"garden/<namespace>/<version>/<dataset>"``) and is only used
    for ``identifier``, documenting where the dataset actually lives in the catalog.
    ``version`` is the real dated version to report (falls back to ``dataset_meta.version``
    when not given, e.g. for datasets whose metadata version is a genuine "latest" alias
    with no dated folder of its own).
    """
    dataset_path = dataset_path.strip("/")
    page_path = page_path.strip("/")
    dataset_url = f"{base_url.rstrip('/')}/{page_path}/"
    # Data files themselves stay at their real, dated catalog location (old dated files persist
    # on R2 after a new version ships), so distribution contentUrls are built from dataset_path,
    # not the short page_path used for the landing page's own url/@id.
    file_base_url = f"{base_url.rstrip('/')}/{dataset_path}/"
    resolved_version = version or dataset_meta.version

    title = _dataset_title(dataset_meta, tables)
    description = _dataset_description(dataset_meta)
    origins = _unique_origins(tables)
    license_url = _license_url(dataset_meta, origins, tables)

    result: dict[str, Any] = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "@id": f"{dataset_url}#dataset",
        "url": dataset_url,
        "identifier": dataset_path,
        "name": title,
        "description": description,
        "publisher": {
            "@type": "Organization",
            "name": "Our World in Data",
            "url": "https://ourworldindata.org",
            "logo": DEFAULT_LOGO_URL,
        },
        "includedInDataCatalog": {
            "@type": "DataCatalog",
            "name": "Our World in Data catalog",
            "url": base_url.rstrip("/") + "/",
        },
        "isAccessibleForFree": True,
        "thumbnailUrl": DEFAULT_THUMBNAIL_URL,
    }

    # NOTE: we deliberately emit no `sameAs`. It is Google Dataset Search's record-merging
    # signal — any record sharing a target URL (e.g. a third-party mirror claiming our
    # GitHub repo) would get folded into one entry with ours, instead of our record
    # standing alone and competing on ranking.
    # NOTE: we also emit no `citation` (related academic articles): a DOI-mined citation
    # list kept misrepresenting auxiliary sources as the dataset's primary reference
    # (e.g. the Maddison GDP paper as owid_energy's only citation). Source credit is
    # fully covered by `isBasedOn`.

    if resolved_version:
        result["version"] = str(resolved_version)
        date_modified = _first_valid_date([resolved_version])
        if date_modified:
            result["dateModified"] = date_modified
    if license_url:
        result["license"] = license_url

    # Creator is the author of this artifact (the OWID-processed dataset), matching how
    # compiled datasets are marked up elsewhere (HuggingFace, Zenodo, Google's own examples).
    # Upstream producers keep credit in isBasedOn (name + URL) and citation.
    result["creator"] = {
        "@type": "Organization",
        "name": "Our World in Data",
        "url": "https://ourworldindata.org",
    }

    date_published = _first_valid_date(origin.date_published for origin in origins)
    if date_published:
        result["datePublished"] = date_published

    based_on = _is_based_on(origins)
    if based_on:
        result["isBasedOn"] = based_on

    keywords = _keywords(tables)
    if keywords:
        result["keywords"] = keywords

    temporal_coverage = _temporal_coverage(tables)
    if temporal_coverage:
        result["temporalCoverage"] = temporal_coverage

    spatial_coverage = _spatial_coverage(tables)
    if spatial_coverage:
        result["spatialCoverage"] = spatial_coverage

    if len(tables) == 1:
        table = tables[0]
        variables = _variable_measured(table)
        if variables:
            result["variableMeasured"] = variables
        distributions = _distributions(file_base_url, table)
        if distributions:
            result["distribution"] = distributions
    elif tables:
        result["hasPart"] = [_table_dataset(dataset_url, file_base_url, table, dataset_meta) for table in tables]

    return _drop_empty(result)


def table_description(table: TableSchemaInput, dataset_meta: DatasetMeta) -> str | None:
    """Return the best available description for a single table node.

    ``TableMeta.description`` is a mostly-internal field, never set with a public-facing
    description in mind, so it is never used for JSON-LD. For the ``hasPart`` representation
    we instead fall back to descriptions that already exist in the metadata — the producer's
    (origin) description, then the dataset-level description — rather than leaving the node
    blank. Nothing is synthesized here; if none of these are populated the node has no
    description.
    """
    origins = _unique_origins([table])
    description = _first_non_empty(origin.description_snapshot or origin.description for origin in origins)
    if description:
        return description
    if dataset_meta.description:
        return dataset_meta.description
    return None


def _table_dataset(
    dataset_url: str, file_base_url: str, table: TableSchemaInput, dataset_meta: DatasetMeta
) -> dict[str, Any]:
    name = table.metadata.title or table.short_name.replace("_", " ").title()
    result: dict[str, Any] = {
        "@type": "Dataset",
        "@id": f"{dataset_url}#table-{table.short_name}",
        "name": name,
        "identifier": table.short_name,
    }
    description = table_description(table, dataset_meta)
    if description:
        result["description"] = description

    variables = _variable_measured(table)
    if variables:
        result["variableMeasured"] = variables

    distributions = _distributions(file_base_url, table)
    if distributions:
        result["distribution"] = distributions

    if table.temporal_coverage:
        result["temporalCoverage"] = table.temporal_coverage
    if table.spatial_coverage:
        result["spatialCoverage"] = table.spatial_coverage

    return _drop_empty(result)


def _variable_measured(table: TableSchemaInput) -> list[dict[str, Any]]:
    variables = []
    primary_key = set(table.primary_key or table.metadata.primary_key or [])

    # In long-format tables one physical column holds many logical indicators; the dimension
    # columns select among them. They are part of the primary key (so the loop below skips
    # them), yet they carry the information a consumer needs to slice the table — emit them
    # first, with their observed values.
    dimension_items = _dimension_property_values(table)
    dimension_slugs = {item["identifier"] for item in dimension_items}
    variables.extend(dimension_items[:MAX_VARIABLES_MEASURED])

    for name, meta in table.variables.items():
        if name in primary_key or name in dimension_slugs:
            continue
        if len(variables) >= MAX_VARIABLES_MEASURED:
            break
        item: dict[str, Any] = {
            "@type": "PropertyValue",
            "name": _variable_title(name, meta),
            "identifier": name,
        }
        description = _variable_description(meta) or _example_description(meta, table)
        if description:
            item["description"] = description
        if meta.unit and not _uses_jinja(meta.unit):
            # A templated unit (e.g. "international-$ in <<ppp_version>> prices") is only
            # correct for one slice of the column; omit it rather than emit a wrong or raw value.
            item["unitText"] = meta.unit
        variables.append(_drop_empty(item))
    return variables


def _dimension_property_values(table: TableSchemaInput) -> list[dict[str, Any]]:
    items = []
    for dimension in table.metadata.dimensions or []:
        slug = dimension["slug"]
        if slug in ENTITY_TIME_DIMENSIONS:
            continue
        parts = ["Dimension column: selects one of the data series stored in this table."]
        dimension_description = dimension.get("description")
        if dimension_description and not _uses_jinja(dimension_description):
            parts.append(dimension_description)
        values = table.dimension_values.get(slug) or []
        if values:
            listed = ", ".join(str(value) for value in values[:MAX_DIMENSION_VALUES_LISTED])
            if len(values) > MAX_DIMENSION_VALUES_LISTED:
                listed += f", … ({len(values)} values in total)"
            parts.append(f"Values: {listed}.")
        items.append(
            {
                "@type": "PropertyValue",
                "name": dimension.get("name") or slug,
                "identifier": slug,
                "description": " ".join(parts),
            }
        )
    return items


def _example_description(meta: VariableMeta, table: TableSchemaInput) -> str | None:
    """Render a Jinja-templated description for one representative dimension combination.

    Used when every description field of a variable is templated (long-format tables), so
    the plain-text fallback chain in ``_variable_description`` comes up empty. The rendered
    text describes a single slice of the column, so it is explicitly labelled as an example.
    """
    dimensions = table.representative_dimensions
    if not dimensions:
        return None
    for template in (meta.description_short, meta.description_from_producer, meta.description, meta.title):
        if not template or not _uses_jinja(template):
            continue
        try:
            rendered = _expand_jinja_text(template, dimensions, remove_dods=True)
        except jinja2.TemplateError:
            continue
        if not isinstance(rendered, str) or not rendered.strip():
            continue
        rendered = rendered.strip()
        if not rendered.endswith((".", "!", "?")):
            rendered += "."
        example = ", ".join(f"{slug}={value}" for slug, value in dimensions.items())
        varies_by = ", ".join(dimensions)
        return f"For example, for {example}: {rendered} Varies by the dimension columns: {varies_by}."
    return None


def _distributions(file_base_url: str, table: TableSchemaInput) -> list[dict[str, Any]]:
    """Build DataDownload entries pointing at the real, dated file location (not the short page URL)."""
    result = []
    for format in table.formats:
        result.append(
            {
                "@type": "DataDownload",
                "name": f"{table.short_name}.{format}",
                "encodingFormat": _encoding_format(format),
                "contentUrl": f"{file_base_url}{table.short_name}.{format}",
            }
        )
    return result


def _dataset_title(dataset_meta: DatasetMeta, tables: list[TableSchemaInput]) -> str:
    if dataset_meta.title:
        return dataset_meta.title
    if len(tables) == 1 and tables[0].metadata.title:
        return tables[0].metadata.title
    if dataset_meta.short_name:
        return dataset_meta.short_name.replace("_", " ").title()
    return "Untitled OWID catalog dataset"


def _dataset_description(dataset_meta: DatasetMeta) -> str:
    """Return the dataset's own description — never guessed from indicator origins or tables.

    A dataset's variables can span several unrelated origins: its own primary source plus
    auxiliary indicators (population, GDP, regions, ...) pulled in for per-capita or aggregate
    columns. Picking a description from any single one of those origins says nothing reliable
    about the dataset as a whole, and which one gets picked is an implementation detail (e.g.
    column order) rather than a meaningful choice. ``TableMeta.description`` isn't a substitute
    either — it's a mostly-internal field that authors rarely set with a public-facing dataset
    description in mind. Every public dataset is expected to set its own ``dataset.description``
    in its ``.meta.yml``; ``assess_dataset_quality`` blocks datasets that don't, via the
    ``missing_description`` check, before this function is ever called. Raising here — rather
    than inventing something — is a backstop in case that gate is ever bypassed.
    """
    if dataset_meta.description:
        return dataset_meta.description
    raise ValueError(
        f"Dataset '{dataset_meta.short_name}' has no description set. "
        "Add `dataset.description` to its .meta.yml — it must not be guessed from indicator origins."
    )


def _unique_origins(tables: list[TableSchemaInput]) -> list[Origin]:
    """Return unique origins, ordered by how many variables reference each one.

    Column order is meaningless here: helper columns (ISO codes, population, GDP) often come
    first in a table, and everything derived from the origin list (creator, citation,
    isBasedOn, datePublished) should lead with the dataset's main source, which is the origin
    the most variables reference — not whichever origin the first column happens to use.
    """
    origins: dict[Any, Origin] = {}
    counts: dict[Any, int] = {}
    for table in tables:
        for variable in table.variables.values():
            for origin in variable.origins:
                key = (origin.producer, origin.title, origin.url_main, origin.url_download, origin.citation_full)
                origins.setdefault(key, origin)
                counts[key] = counts.get(key, 0) + 1
    order = sorted(origins, key=lambda key: -counts[key])
    return [origins[key] for key in order]


def _license_url(dataset_meta: DatasetMeta, origins: list[Origin], tables: list[TableSchemaInput]) -> str | None:
    # The record describes the OWID-compiled dataset, so a dataset-level license declared in
    # its .meta.yml (e.g. CC BY for owid_co2, matching its GitHub repo) speaks for the whole
    # artifact and wins. Origin licenses are a per-source fallback: the "first" one is just
    # the most-referenced source's license, which can misrepresent the compilation (owid_co2
    # used to advertise GCB's ICOS data license).
    for license in dataset_meta.licenses:
        url = _license_to_url(license)
        if url:
            return url
    for origin in origins:
        url = _license_to_url(origin.license)
        if url:
            return url
    for table in tables:
        for variable in table.variables.values():
            url = _license_to_url(variable.license)
            if url:
                return url
            for license in variable.licenses:
                url = _license_to_url(license)
                if url:
                    return url
    return None


def license_to_url(license: License | None) -> str | None:
    """Return a resolvable license URL, including canonical URLs for known license names."""
    if not license:
        return None
    if license.url and _looks_like_url(license.url):
        return license.url
    if license.name:
        return KNOWN_LICENSE_URLS.get(license.name.strip())
    return None


def _license_to_url(license: License | None) -> str | None:
    return license_to_url(license)


def _is_based_on(origins: list[Origin]) -> list[dict[str, Any]] | dict[str, Any] | None:
    items = []
    seen = set()
    for origin in origins:
        url = origin.url_main or origin.url_download
        if not url or not _looks_like_url(url) or url in seen:
            continue
        seen.add(url)
        item: dict[str, Any] = {"@type": "CreativeWork", "url": url}
        if origin.title:
            item["name"] = origin.title
        items.append(item)
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return items[:5]


def _temporal_coverage(tables: list[TableSchemaInput]) -> str | None:
    years = []
    for table in tables:
        coverage = table.temporal_coverage
        if not coverage:
            continue
        if "/" in coverage:
            start, end = coverage.split("/", 1)
            years.extend([start, end])
        else:
            years.append(coverage)
    valid_years = sorted({year for year in years if re.match(r"^\d{4}$", year)})
    if not valid_years:
        return None
    if valid_years[0] == valid_years[-1]:
        return valid_years[0]
    return f"{valid_years[0]}/{valid_years[-1]}"


def _spatial_coverage(tables: list[TableSchemaInput]) -> str | None:
    if any(table.spatial_coverage for table in tables):
        return "Worldwide"
    return None


def _keywords(tables: list[TableSchemaInput]) -> list[str]:
    """Topic tags ordered by how many variables carry each one, most-tagged first.

    Column order would put whichever tag the first column happens to carry in front (e.g.
    "Economic Growth" leading owid_co2 because the helper gdp column comes early), whereas
    consumers — like the landing page's "explore the charts" link — treat the first keyword
    as the dataset's primary topic.
    """
    counts: dict[str, int] = {}
    for table in tables:
        for variable in table.variables.values():
            presentation = variable.presentation
            if not presentation:
                continue
            for tag in presentation.topic_tags:
                counts[tag] = counts.get(tag, 0) + 1
    # Python's sort is stable, so equal-count tags keep their first-seen order.
    return sorted(counts, key=lambda tag: -counts[tag])


def _variable_title(name: str, meta: VariableMeta) -> str:
    # Jinja-templated candidates are skipped: a template only makes sense once rendered with
    # dimension values, and the raw text is noise for JSON-LD consumers. The identifier is
    # the honest fallback for a column whose title varies by slice.
    if meta.presentation and meta.presentation.title_public and not _uses_jinja(meta.presentation.title_public):
        return meta.presentation.title_public
    if meta.title and not _uses_jinja(meta.title):
        return meta.title
    return name


def _variable_description(meta: VariableMeta) -> str | None:
    description_key = meta.description_key
    if isinstance(description_key, list):
        description_key = description_key_to_string(description_key)
    candidates = (
        meta.description_short,
        description_key,
        meta.description_from_producer,
        meta.description,
    )
    for candidate in candidates:
        if candidate and not _uses_jinja(candidate):
            # Descriptions may contain markdown detail-on-demand links ("[term](#dod:term)")
            # that only resolve on ourworldindata.org; keep just the link text here.
            return remove_details_on_demand(candidate)
    return None


def _encoding_format(format: str) -> str:
    return {
        "csv": "text/csv",
        "json": "application/json",
        "parquet": "application/vnd.apache.parquet",
        "feather": "application/vnd.apache.arrow.file",
    }.get(format, format)


def _first_non_empty(values: Any) -> str | None:
    for value in values:
        if value:
            return str(value)
    return None


def _first_valid_date(values: Any) -> str | None:
    for value in values:
        if not value or value == "latest":
            continue
        text = str(value)
        if re.match(r"^\d{4}(-\d{2}-\d{2})?$", text):
            return text
    return None


def _looks_like_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _drop_empty(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _drop_empty(v) for k, v in value.items() if v not in (None, "", [], {})}
    if isinstance(value, list):
        return [_drop_empty(v) for v in value if v not in (None, "", [], {})]
    return value
