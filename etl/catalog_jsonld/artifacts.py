"""Build local JSON-LD artifacts for OWID catalog datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.feather as feather
import pyarrow.parquet as pq
from owid.catalog.api.legacy import CHANNEL, LocalCatalog
from owid.catalog.core.datasets import SUPPORTED_FORMATS, Dataset
from owid.catalog.core.meta import TableMeta, VariableMeta
from owid.catalog.schema_org import DEFAULT_CATALOG_BASE_URL, TableSchemaInput, dataset_to_schema_org

from etl.catalog_jsonld.quality import DatasetQualityResult, assess_dataset_quality
from etl.catalog_jsonld.sitemap import sitemap_xml
from etl.paths import DATA_DIR

QUALITY_REPORT_FILENAME = "jsonld_quality_report.json"
SITEMAP_FILENAME = "sitemap.xml"
DATASET_JSONLD_FILENAME = "dataset.jsonld"


@dataclass
class JsonLdBuildResult:
    emitted: list[str] = field(default_factory=list)
    skipped: list[DatasetQualityResult] = field(default_factory=list)
    warnings: list[DatasetQualityResult] = field(default_factory=list)


def build_catalog_jsonld_artifacts(
    *,
    catalog_dir: Path = DATA_DIR,
    channel: CHANNEL = "garden",
    base_url: str = DEFAULT_CATALOG_BASE_URL,
    dry_run: bool = False,
) -> JsonLdBuildResult:
    """Generate dataset JSON-LD files, sitemap, and quality report locally."""
    catalog = LocalCatalog(catalog_dir, channels=(channel,))
    latest_paths = latest_dataset_paths(catalog.frame, channel=channel)
    result = JsonLdBuildResult()
    sitemap_urls = []

    for catalog_path in latest_paths:
        ds = Dataset(catalog_dir / catalog_path)
        tables = load_table_schema_inputs(ds)
        quality = assess_dataset_quality(catalog_path=catalog_path, dataset_meta=ds.metadata, tables=tables)
        if not quality.is_eligible:
            result.skipped.append(quality)
            if not dry_run:
                stale_jsonld = Path(ds.path) / DATASET_JSONLD_FILENAME
                if stale_jsonld.exists():
                    stale_jsonld.unlink()
            continue

        jsonld = dataset_to_schema_org(
            dataset_path=catalog_path,
            dataset_meta=ds.metadata,
            tables=tables,
            base_url=base_url,
        )
        result.emitted.append(catalog_path)
        if quality.warnings or quality.table_warnings:
            result.warnings.append(quality)

        sitemap_urls.append(f"{base_url.rstrip('/')}/{catalog_path}/")
        if not dry_run:
            with open(Path(ds.path) / DATASET_JSONLD_FILENAME, "w") as ostream:
                json.dump(jsonld, ostream, indent=2, ensure_ascii=False)
                ostream.write("\n")

    if not dry_run:
        (catalog_dir / SITEMAP_FILENAME).write_text(sitemap_xml(sitemap_urls))
        (catalog_dir / QUALITY_REPORT_FILENAME).write_text(json.dumps(quality_report(result), indent=2) + "\n")

    return result


def latest_dataset_paths(frame: pd.DataFrame, *, channel: CHANNEL = "garden") -> list[str]:
    """Return latest public dataset-folder paths for a catalog channel."""
    df = frame.loc[(frame["channel"] == channel) & (frame["is_public"] == True)].copy()  # noqa: E712
    if df.empty:
        return []
    df["dataset_path"] = df["path"].map(lambda p: str(p).rsplit("/", 1)[0])
    df = df.sort_values("version")
    latest = df.drop_duplicates(["channel", "namespace", "dataset"], keep="last")
    return sorted(latest["dataset_path"].unique().tolist())


def load_table_schema_inputs(ds: Dataset) -> list[TableSchemaInput]:
    tables = []
    dataset_path = Path(ds.path)
    for meta_path in ds._metadata_files:
        table_meta = _load_table_meta(Path(meta_path))
        if not table_meta.short_name:
            continue
        table_meta.dataset = ds.metadata
        formats = _available_formats(dataset_path, table_meta.short_name)
        temporal_coverage, spatial_coverage = _coverage_from_table_data(
            dataset_path=dataset_path,
            table_name=table_meta.short_name,
            formats=formats,
            primary_key=table_meta.primary_key,
        )
        tables.append(
            TableSchemaInput(
                short_name=table_meta.short_name,
                metadata=table_meta,
                variables=_load_variable_meta(Path(meta_path)),
                formats=formats,
                primary_key=table_meta.primary_key,
                temporal_coverage=temporal_coverage,
                spatial_coverage=spatial_coverage,
            )
        )
    return tables


def quality_report(result: JsonLdBuildResult) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "emitted": len(result.emitted),
            "skipped": len(result.skipped),
            "warnings": len(result.warnings),
        },
        "emitted": result.emitted,
        "skipped": [_quality_to_record(item, include_blockers=True) for item in result.skipped],
        "warnings": [_quality_to_record(item, include_blockers=False) for item in result.warnings],
    }


def _load_table_meta(meta_path: Path) -> TableMeta:
    data = json.loads(meta_path.read_text())
    data.pop("fields", None)
    return TableMeta.from_dict(data)


def _load_variable_meta(meta_path: Path) -> dict[str, VariableMeta]:
    data = json.loads(meta_path.read_text())
    return {name: VariableMeta.from_dict(meta) for name, meta in data.get("fields", {}).items()}


def _available_formats(dataset_path: Path, table_name: str) -> list[str]:
    return [format for format in SUPPORTED_FORMATS if (dataset_path / f"{table_name}.{format}").exists()]


def _coverage_from_table_data(
    *, dataset_path: Path, table_name: str, formats: list[str], primary_key: list[str]
) -> tuple[str | None, str | None]:
    columns = set(primary_key)
    wanted_columns = [column for column in ("year", "country") if column in columns]
    if not wanted_columns:
        return None, None

    tb = _read_coverage_columns(
        dataset_path=dataset_path, table_name=table_name, formats=formats, columns=wanted_columns
    )
    if tb is None:
        return None, "Worldwide" if "country" in columns else None

    temporal_coverage = _temporal_coverage_from_years(tb["year"]) if "year" in tb.columns else None
    spatial_coverage = "Worldwide" if "country" in tb.columns or "country" in columns else None
    return temporal_coverage, spatial_coverage


def _read_coverage_columns(
    *, dataset_path: Path, table_name: str, formats: list[str], columns: list[str]
) -> pd.DataFrame | None:
    for format in formats:
        path = dataset_path / f"{table_name}.{format}"
        try:
            if format == "feather":
                return feather.read_table(path, columns=columns).to_pandas()
            if format == "parquet":
                return pq.read_table(path, columns=columns).to_pandas()
            if format == "csv":
                return pd.read_csv(path, usecols=columns)
        except (OSError, ValueError, KeyError, ImportError):
            continue
    return None


def _temporal_coverage_from_years(years: pd.Series) -> str | None:
    numeric_years = pd.to_numeric(years, errors="coerce").dropna()
    if numeric_years.empty:
        return None
    start = str(int(numeric_years.min()))
    end = str(int(numeric_years.max()))
    if start == end:
        return start
    return f"{start}/{end}"


def _quality_to_record(item: DatasetQualityResult, *, include_blockers: bool) -> dict[str, Any]:
    record: dict[str, Any] = {"catalog_path": item.catalog_path}
    if include_blockers:
        record["reasons"] = item.blockers
        record["severity"] = "blocker"
    if item.warnings:
        record["warnings"] = item.warnings
    if item.table_warnings:
        record["tables"] = [
            {"table": table, "warnings": warnings} for table, warnings in sorted(item.table_warnings.items())
        ]
    return record
