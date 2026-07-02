"""Quality gates for catalog JSON-LD generation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field

from owid.catalog.core.datasets import CHANNEL
from owid.catalog.core.meta import DatasetMeta
from owid.catalog.schema_org import TableSchemaInput, license_to_url, table_description

# Root-level file/dir names that live directly under catalog_dir and must not be
# shadowed by a short-key namespace segment.
RESERVED_ROOT_NAMES = {"robots.txt", "jsonld_quality_report.json"}


@dataclass
class DatasetQualityResult:
    catalog_path: str
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    table_warnings: dict[str, list[str]] = field(default_factory=dict)

    @property
    def is_eligible(self) -> bool:
        return not self.blockers


def is_reserved_namespace(namespace: str) -> bool:
    """Return True if ``namespace`` would collide with a reserved top-level catalog_dir name.

    Reserved names are the catalog channels (``garden``, ``meadow``, ``snapshot``, ...),
    fixed root-level artifact files (``robots.txt``, the quality report), and the sitemap
    file family (``sitemap.xml``, ``sitemap-index.xml``, ``sitemap-<N>.xml``).
    """
    if namespace in set(CHANNEL.__args__):
        return True
    if namespace in RESERVED_ROOT_NAMES:
        return True
    if namespace.startswith("sitemap") and namespace.endswith(".xml"):
        return True
    return False


def find_duplicate_short_key_paths(entries: Iterable[tuple[str, str, str]]) -> set[str]:
    """Return catalog_paths whose ``<namespace>/<dataset>`` short key is shared by another entry.

    ``entries`` is an iterable of ``(catalog_path, namespace, dataset)`` tuples, one per
    candidate emitted dataset in the current build. This is a batch-level check: a single
    dataset can't tell it collides with another without seeing the whole set.
    """
    entries = list(entries)
    counts = Counter(f"{namespace}/{dataset}" for _, namespace, dataset in entries)
    return {catalog_path for (catalog_path, namespace, dataset) in entries if counts[f"{namespace}/{dataset}"] > 1}


def assess_dataset_quality(
    *,
    catalog_path: str,
    namespace: str,
    dataset_meta: DatasetMeta,
    tables: list[TableSchemaInput],
    duplicate_short_key: bool = False,
) -> DatasetQualityResult:
    result = DatasetQualityResult(catalog_path=catalog_path)

    if not dataset_meta.is_public:
        result.blockers.append("private_dataset")

    if dataset_meta.non_redistributable:
        result.blockers.append("non_redistributable")

    if is_reserved_namespace(namespace):
        result.blockers.append("reserved_namespace")

    if duplicate_short_key:
        result.blockers.append("duplicate_short_key")

    if not _has_title(dataset_meta, tables):
        result.blockers.append("missing_title")

    if not _has_description(dataset_meta):
        result.blockers.append("missing_description")

    if not _has_license_url(dataset_meta, tables):
        result.blockers.append("missing_license_url")

    if not _has_provenance(tables):
        result.blockers.append("missing_provenance")

    if not any(table.formats for table in tables):
        result.blockers.append("missing_public_distribution")

    for table in tables:
        warnings = []
        # A table rarely sets its own description (a mostly-internal field); the emitted
        # JSON-LD falls back to the producer (origin) or dataset description, so only warn
        # when no description resolves from any of those existing sources.
        if not table_description(table, dataset_meta):
            warnings.append("missing_table_description")
        variable_count = len(
            [name for name in table.variables if name not in set(table.primary_key or table.metadata.primary_key)]
        )
        if variable_count == 0:
            warnings.append("missing_measured_variables")
        if variable_count > 100:
            warnings.append("wide_table_variableMeasured_truncated")
        if warnings:
            result.table_warnings[table.short_name] = warnings

    return result


def _has_title(dataset_meta: DatasetMeta, tables: list[TableSchemaInput]) -> bool:
    return bool(dataset_meta.title or dataset_meta.short_name or any(table.metadata.title for table in tables))


def _has_description(dataset_meta: DatasetMeta) -> bool:
    # Matches the contract in owid.catalog.schema_org._dataset_description: neither an
    # indicator origin's description (e.g. an auxiliary population/GDP column) nor
    # TableMeta.description (a mostly-internal field) counts as the dataset having a
    # description of its own — only an explicit dataset.description does.
    return bool(dataset_meta.description)


def _has_license_url(dataset_meta: DatasetMeta, tables: list[TableSchemaInput]) -> bool:
    if any(license_to_url(license) for license in dataset_meta.licenses):
        return True
    for table in tables:
        for variable in table.variables.values():
            if license_to_url(variable.license):
                return True
            if any(license_to_url(license) for license in variable.licenses):
                return True
            if any(license_to_url(origin.license) for origin in variable.origins):
                return True
    return False


def _has_provenance(tables: list[TableSchemaInput]) -> bool:
    for table in tables:
        for variable in table.variables.values():
            for origin in variable.origins:
                if origin.producer or origin.citation_full or origin.url_main or origin.url_download:
                    return True
    return False
