"""Map OWID catalog metadata to Schema.org Dataset JSON-LD."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from owid.catalog.core.meta import DatasetMeta, License, Origin, TableMeta, VariableMeta

DEFAULT_CATALOG_BASE_URL = "https://catalog.ourworldindata.org"
DEFAULT_LOGO_URL = "https://ourworldindata.org/owid-logo.svg"
DEFAULT_THUMBNAIL_URL = DEFAULT_LOGO_URL
MAX_VARIABLES_MEASURED = 100
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


def dataset_to_schema_org(
    *,
    dataset_path: str,
    dataset_meta: DatasetMeta,
    tables: list[TableSchemaInput],
    base_url: str = DEFAULT_CATALOG_BASE_URL,
) -> dict[str, Any]:
    """Convert one catalog dataset folder to Schema.org Dataset JSON-LD.

    The catalog folder is the top-level Dataset. For single-table datasets, table
    metadata is flattened into the top-level Dataset. For multi-table datasets,
    each table is represented as a nested Dataset via ``hasPart``.
    """
    dataset_path = dataset_path.strip("/")
    dataset_url = f"{base_url.rstrip('/')}/{dataset_path}/"

    title = _dataset_title(dataset_meta, tables)
    description = _dataset_description(dataset_meta, tables)
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

    if dataset_meta.version:
        result["version"] = str(dataset_meta.version)
        date_modified = _first_valid_date([dataset_meta.version])
        if date_modified:
            result["dateModified"] = date_modified
    if license_url:
        result["license"] = license_url

    creator = _creator_from_origins(origins)
    if creator:
        result["creator"] = creator

    citation = _first_non_empty(origin.citation_full for origin in origins)
    if citation:
        result["citation"] = citation

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
        distributions = _distributions(dataset_url, table)
        if distributions:
            result["distribution"] = distributions
    elif tables:
        result["hasPart"] = [_table_dataset(dataset_url, table) for table in tables]

    return _drop_empty(result)


def _table_dataset(dataset_url: str, table: TableSchemaInput) -> dict[str, Any]:
    name = table.metadata.title or table.short_name.replace("_", " ").title()
    result: dict[str, Any] = {
        "@type": "Dataset",
        "@id": f"{dataset_url}#table-{table.short_name}",
        "name": name,
        "identifier": table.short_name,
    }
    if table.metadata.description:
        result["description"] = table.metadata.description

    variables = _variable_measured(table)
    if variables:
        result["variableMeasured"] = variables

    distributions = _distributions(dataset_url, table)
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
    for name, meta in table.variables.items():
        if name in primary_key:
            continue
        item: dict[str, Any] = {
            "@type": "PropertyValue",
            "name": _variable_title(name, meta),
            "identifier": name,
        }
        description = _variable_description(meta)
        if description:
            item["description"] = description
        if meta.unit:
            item["unitText"] = meta.unit
        variables.append(_drop_empty(item))
        if len(variables) >= MAX_VARIABLES_MEASURED:
            break
    return variables


def _distributions(dataset_url: str, table: TableSchemaInput) -> list[dict[str, Any]]:
    result = []
    for format in table.formats:
        result.append(
            {
                "@type": "DataDownload",
                "name": f"{table.short_name}.{format}",
                "encodingFormat": _encoding_format(format),
                "contentUrl": f"{dataset_url}{table.short_name}.{format}",
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


def _dataset_description(dataset_meta: DatasetMeta, tables: list[TableSchemaInput]) -> str:
    if dataset_meta.description:
        return dataset_meta.description
    if len(tables) == 1 and tables[0].metadata.description:
        return tables[0].metadata.description
    origins = _unique_origins(tables)
    description = _first_non_empty(origin.description_snapshot or origin.description for origin in origins)
    if description:
        return description
    return "Dataset published in the Our World in Data catalog."


def _unique_origins(tables: list[TableSchemaInput]) -> list[Origin]:
    origins: list[Origin] = []
    seen = set()
    for table in tables:
        for variable in table.variables.values():
            for origin in variable.origins:
                key = (origin.producer, origin.title, origin.url_main, origin.url_download, origin.citation_full)
                if key not in seen:
                    seen.add(key)
                    origins.append(origin)
    return origins


def _license_url(dataset_meta: DatasetMeta, origins: list[Origin], tables: list[TableSchemaInput]) -> str | None:
    for origin in origins:
        url = _license_to_url(origin.license)
        if url:
            return url
    for license in dataset_meta.licenses:
        url = _license_to_url(license)
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


def _creator_from_origins(origins: list[Origin]) -> dict[str, str] | list[dict[str, str]] | None:
    producers = []
    for origin in origins:
        producer = origin.attribution or origin.producer
        if producer and producer not in producers:
            producers.append(producer)
    creators = [{"@type": "Organization", "name": producer} for producer in producers[:5]]
    if not creators:
        return None
    if len(creators) == 1:
        return creators[0]
    return creators


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
    keywords = []
    for table in tables:
        for variable in table.variables.values():
            presentation = variable.presentation
            if not presentation:
                continue
            for tag in presentation.topic_tags:
                if tag not in keywords:
                    keywords.append(tag)
    return keywords


def _variable_title(name: str, meta: VariableMeta) -> str:
    if meta.presentation and meta.presentation.title_public:
        return meta.presentation.title_public
    if meta.title:
        return meta.title
    return name


def _variable_description(meta: VariableMeta) -> str | None:
    if meta.description_short:
        return meta.description_short
    if meta.description_key:
        return " ".join(meta.description_key)
    if meta.description_from_producer:
        return meta.description_from_producer
    if meta.description:
        return meta.description
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
