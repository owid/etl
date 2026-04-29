"""Quality gates for catalog JSON-LD generation."""

from __future__ import annotations

from dataclasses import dataclass, field

from owid.catalog.core.meta import DatasetMeta
from owid.catalog.schema_org import TableSchemaInput, license_to_url


@dataclass
class DatasetQualityResult:
    catalog_path: str
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    table_warnings: dict[str, list[str]] = field(default_factory=dict)

    @property
    def is_eligible(self) -> bool:
        return not self.blockers


def assess_dataset_quality(
    *,
    catalog_path: str,
    dataset_meta: DatasetMeta,
    tables: list[TableSchemaInput],
) -> DatasetQualityResult:
    result = DatasetQualityResult(catalog_path=catalog_path)

    if not dataset_meta.is_public:
        result.blockers.append("private_dataset")

    if not _has_title(dataset_meta, tables):
        result.blockers.append("missing_title")

    if not _has_description(dataset_meta, tables):
        result.blockers.append("missing_description")

    if not _has_license_url(dataset_meta, tables):
        result.blockers.append("missing_license_url")

    if not _has_provenance(tables):
        if dataset_meta.sources:
            result.blockers.append("legacy_sources_without_origins")
        else:
            result.blockers.append("missing_provenance")

    if not any(table.formats for table in tables):
        result.blockers.append("missing_public_distribution")

    for table in tables:
        warnings = []
        if not table.metadata.description:
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


def _has_description(dataset_meta: DatasetMeta, tables: list[TableSchemaInput]) -> bool:
    if dataset_meta.description or any(table.metadata.description for table in tables):
        return True
    for table in tables:
        for variable in table.variables.values():
            for origin in variable.origins:
                if origin.description or origin.description_snapshot:
                    return True
    return False


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
