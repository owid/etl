"""Build local JSON-LD artifacts for OWID catalog datasets."""

from __future__ import annotations

import json
import re
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
from owid.catalog.schema_org import (
    DEFAULT_CATALOG_BASE_URL,
    ENTITY_TIME_DIMENSIONS,
    TableSchemaInput,
    dataset_to_schema_org,
)
from structlog import get_logger

from etl.catalog_jsonld.quality import (
    DatasetQualityResult,
    assess_dataset_quality,
    find_duplicate_short_key_paths,
    jsonld_contains_raw_jinja,
)
from etl.catalog_jsonld.sitemap import SitemapEntry, sitemap_xml
from etl.dag_helpers import graph_nodes, load_dag
from etl.paths import DATA_DIR

log = get_logger()

QUALITY_REPORT_FILENAME = "jsonld_quality_report.json"
SITEMAP_FILENAME = "sitemap.xml"
DATASET_JSONLD_FILENAME = "dataset.jsonld"

_VERSION_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class LatestDatasetPath:
    """One resolved ``<namespace>/<dataset>`` entry: its full catalog path, and version.

    ``catalog_path`` is the dated catalog-folder path (e.g. ``garden/emissions/2025-12-04/owid_co2``).
    ``short_key`` is the stable, version-agnostic public page key (e.g. ``emissions/owid_co2``).
    """

    catalog_path: str
    namespace: str
    dataset: str
    version: str

    @property
    def short_key(self) -> str:
        return f"{self.namespace}/{self.dataset}"


@dataclass
class JsonLdBuildResult:
    emitted: list[str] = field(default_factory=list)
    emitted_entries: list[LatestDatasetPath] = field(default_factory=list)
    skipped: list[DatasetQualityResult] = field(default_factory=list)
    skipped_entries: list[LatestDatasetPath] = field(default_factory=list)
    warnings: list[DatasetQualityResult] = field(default_factory=list)
    archived_entries: list[LatestDatasetPath] = field(default_factory=list)
    superseded_entries: list[LatestDatasetPath] = field(default_factory=list)


def build_catalog_jsonld_artifacts(
    *,
    catalog_dir: Path = DATA_DIR,
    channel: CHANNEL = "garden",
    base_url: str = DEFAULT_CATALOG_BASE_URL,
    dry_run: bool = False,
    only: set[str] | None = None,
    active_steps: set[str] | None = None,
) -> JsonLdBuildResult:
    """Generate dataset JSON-LD files, sitemap, and quality report locally.

    JSON-LD files are written to a stable, version-agnostic short-key tree
    (``catalog_dir / "<namespace>" / "<dataset>" / "dataset.jsonld"``) rather than inside the
    dataset's own dated catalog folder, so the public landing page URL doesn't change every
    time the dataset gets a new version. When ``only`` is given, restrict generation to
    datasets whose ``"<namespace>/<dataset>"`` is in the set (version-agnostic allowlist);
    otherwise only datasets that opt in via ``DatasetMeta.jsonld`` are considered.

    ``active_steps`` overrides the set of active DAG step URIs used to exclude stale,
    archived on-disk builds (see :func:`latest_dataset_paths`). Defaults to the real DAG;
    tests should pass an explicit set instead of relying on ``dag/*.yml``.
    """
    catalog = LocalCatalog(catalog_dir, channels=(channel,))
    latest_paths = latest_dataset_paths(catalog.frame, channel=channel, only=only, active_steps=active_steps)
    result = JsonLdBuildResult()
    sitemap_entries: list[SitemapEntry] = []

    # Every on-disk build whose step isn't in the active DAG is excluded from `latest_paths`
    # above, so a superseded or fully-archived version never becomes an emitted or skipped
    # entry. Without this, a dataset.jsonld published before the step went inactive (at its
    # old dated path, and — for a fully-archived dataset with no active replacement at all —
    # its stable short key too) would linger on R2 forever, since nothing would ever schedule
    # its deletion again.
    result.archived_entries, result.superseded_entries = find_inactive_dataset_entries(
        catalog.frame, channel=channel, only=only, active_steps=active_steps
    )
    for entry in result.archived_entries:
        if not dry_run:
            _remove_if_exists(catalog_dir / entry.catalog_path / DATASET_JSONLD_FILENAME)
            _remove_if_exists(catalog_dir / entry.namespace / entry.dataset / DATASET_JSONLD_FILENAME)
    for entry in result.superseded_entries:
        # Its short key is legitimately owned by the active version emitted elsewhere in this
        # same build (or by nothing, if that active version is itself quality-skipped) — only
        # this specific old dated path needs cleaning up, never the short key.
        if not dry_run:
            _remove_if_exists(catalog_dir / entry.catalog_path / DATASET_JSONLD_FILENAME)

    duplicate_catalog_paths = find_duplicate_short_key_paths(
        (entry.catalog_path, entry.namespace, entry.dataset) for entry in latest_paths
    )

    for entry in latest_paths:
        catalog_path = entry.catalog_path
        ds = Dataset(catalog_dir / catalog_path)
        # Without an explicit allowlist, only datasets that opt in via `dataset: jsonld: true`
        # in their metadata are considered (canary rollout of the catalog-discovery project).
        # Unflagged datasets are invisible to this build: not emitted, not reported, and any
        # previously published artifacts are left untouched.
        if only is None and not ds.metadata.jsonld:
            continue
        tables = load_table_schema_inputs(ds)
        quality = assess_dataset_quality(
            catalog_path=catalog_path,
            namespace=entry.namespace,
            dataset_meta=ds.metadata,
            tables=tables,
            duplicate_short_key=catalog_path in duplicate_catalog_paths,
        )
        if not quality.is_eligible:
            result.skipped.append(quality)
            result.skipped_entries.append(entry)
            if not dry_run:
                _remove_if_exists(Path(ds.path) / DATASET_JSONLD_FILENAME)
                # A prior build may have emitted this dataset at its stable short key;
                # remove the local copy so it can't linger after the dataset stops
                # being eligible (the R2 copy is deleted by the publish step).
                _remove_if_exists(catalog_dir / entry.namespace / entry.dataset / DATASET_JSONLD_FILENAME)
            continue

        jsonld = dataset_to_schema_org(
            dataset_path=catalog_path,
            page_path=entry.short_key,
            version=entry.version,
            dataset_meta=ds.metadata,
            tables=tables,
            base_url=base_url,
        )
        # Safety net: metadata fields can be Jinja templates (long-format tables render them
        # per dimension combination elsewhere). schema_org guards the known fields, but any
        # template that still leaks into the output must never ship — skip the dataset instead.
        if jsonld_contains_raw_jinja(jsonld):
            quality.blockers.append("raw_jinja_in_jsonld")
            result.skipped.append(quality)
            result.skipped_entries.append(entry)
            log.warning("catalog_jsonld.raw_jinja_in_jsonld", dataset=catalog_path)
            if not dry_run:
                _remove_if_exists(Path(ds.path) / DATASET_JSONLD_FILENAME)
                _remove_if_exists(catalog_dir / entry.namespace / entry.dataset / DATASET_JSONLD_FILENAME)
            continue

        result.emitted.append(catalog_path)
        result.emitted_entries.append(entry)
        if quality.warnings or quality.table_warnings:
            result.warnings.append(quality)

        sitemap_entries.append(
            SitemapEntry(
                url=f"{base_url.rstrip('/')}/{entry.short_key}/",
                # Non-date versions (e.g. "latest") carry no real modification date; omit
                # lastmod rather than stamping the build date, which would falsely mark the
                # page as modified on every publish.
                lastmod=entry.version if _VERSION_DATE_RE.match(entry.version) else None,
            )
        )
        if not dry_run:
            # The dataset used to be served at its dated catalog-folder path; that location is
            # no longer written to, so clean up anything left over from a prior publish.
            _remove_if_exists(Path(ds.path) / DATASET_JSONLD_FILENAME)

            target_dir = catalog_dir / entry.namespace / entry.dataset
            target_dir.mkdir(parents=True, exist_ok=True)
            with open(target_dir / DATASET_JSONLD_FILENAME, "w") as ostream:
                json.dump(jsonld, ostream, indent=2, ensure_ascii=False)
                ostream.write("\n")

    if not dry_run:
        (catalog_dir / SITEMAP_FILENAME).write_text(sitemap_xml(sitemap_entries))
        (catalog_dir / QUALITY_REPORT_FILENAME).write_text(json.dumps(quality_report(result), indent=2) + "\n")

    return result


def _remove_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def latest_dataset_paths(
    frame: pd.DataFrame,
    *,
    channel: CHANNEL = "garden",
    only: set[str] | None = None,
    active_steps: set[str] | None = None,
) -> list[LatestDatasetPath]:
    """Return latest public dataset entries for a catalog channel.

    Private datasets are always excluded (``is_public == True`` filter). Datasets whose step
    is no longer in the active DAG are also excluded — otherwise a stale on-disk build left
    over from before a dataset was re-versioned (e.g. an archived ``.../latest/...`` step
    superseded by a dated version) can outrank the real latest version, since ``"latest"``
    sorts after any ``YYYY-MM-DD`` version string. ``active_steps`` defaults to the real DAG
    (``graph_nodes(load_dag())``, matching keys and dependency values alike — a step can be
    active purely as someone else's dependency without its own top-level DAG entry); pass an
    explicit set to override it (e.g. in tests). When ``only`` is
    provided, the result is further restricted to datasets whose ``"<namespace>/<dataset>"``
    is in the set. Matching is version-agnostic so it survives data re-versioning. Allowlist
    entries that match no dataset are logged as a warning (typo / renamed dataset).
    """
    df = frame.loc[(frame["channel"] == channel) & (frame["is_public"] == True)].copy()  # noqa: E712
    if df.empty:
        if only:
            for dataset_key in sorted(only):
                log.warning("catalog_jsonld.allowlist_entry_unmatched", dataset=dataset_key, channel=channel)
        return []
    if active_steps is None:
        active_steps = graph_nodes(load_dag())
    df["step"] = (
        "data://" + df["channel"] + "/" + df["namespace"] + "/" + df["version"].astype(str) + "/" + df["dataset"]
    )
    df = df[df["step"].isin(active_steps)]
    if df.empty:
        if only:
            for dataset_key in sorted(only):
                log.warning("catalog_jsonld.allowlist_entry_unmatched", dataset=dataset_key, channel=channel)
        return []
    df["dataset_path"] = df["path"].map(lambda p: str(p).rsplit("/", 1)[0])
    df = df.sort_values("version")
    latest = df.drop_duplicates(["channel", "namespace", "dataset"], keep="last")

    if only is not None:
        latest = latest.copy()
        latest["dataset_key"] = latest["namespace"].astype(str).str.cat(latest["dataset"].astype(str), sep="/")
        for dataset_key in sorted(only - set(latest["dataset_key"])):
            log.warning("catalog_jsonld.allowlist_entry_unmatched", dataset=dataset_key, channel=channel)
        latest = latest[latest["dataset_key"].isin(only)]

    entries = [
        LatestDatasetPath(
            catalog_path=str(row.dataset_path),
            namespace=str(row.namespace),
            dataset=str(row.dataset),
            version=str(row.version),
        )
        for row in latest.itertuples()
    ]
    return sorted(entries, key=lambda entry: entry.catalog_path)


def find_inactive_dataset_entries(
    frame: pd.DataFrame,
    *,
    channel: CHANNEL = "garden",
    only: set[str] | None = None,
    active_steps: set[str] | None = None,
) -> tuple[list[LatestDatasetPath], list[LatestDatasetPath]]:
    """Return every on-disk build whose step is not in the active DAG, split into two lists:

    - ``archived``: builds belonging to a ``<namespace>/<dataset>`` group with NO active-DAG
      representative at all (the step was removed outright, with no replacement). A prior
      build may have published a dataset.jsonld for them at both their dated catalog path and
      their stable short key, and both need cleaning up now that the step is gone for good.
    - ``superseded``: builds belonging to a group that DOES have an active representative
      (just not this particular on-disk version — e.g. an old ``.../latest/...`` build left
      behind after re-versioning to a dated one). Only their dated-path JSON-LD needs cleaning
      up; their short key is legitimately owned by the active version instead.

    Every inactive on-disk version is returned (not just the newest per group), since any of
    them may carry a leftover dated-path dataset.jsonld from when it used to be current.
    Respects ``only``/``active_steps`` the same way :func:`latest_dataset_paths` does.
    """
    df = frame.loc[(frame["channel"] == channel) & (frame["is_public"] == True)].copy()  # noqa: E712
    if df.empty:
        return [], []
    if active_steps is None:
        active_steps = graph_nodes(load_dag())
    df["step"] = (
        "data://" + df["channel"] + "/" + df["namespace"] + "/" + df["version"].astype(str) + "/" + df["dataset"]
    )
    df["dataset_key"] = df["namespace"].astype(str).str.cat(df["dataset"].astype(str), sep="/")
    if only is not None:
        df = df[df["dataset_key"].isin(only)]
        if df.empty:
            return [], []
    is_active = df["step"].isin(active_steps)
    active_keys = set(df.loc[is_active, "dataset_key"])
    inactive = df[~is_active]
    if inactive.empty:
        return [], []
    inactive = inactive.copy()
    inactive["dataset_path"] = inactive["path"].map(lambda p: str(p).rsplit("/", 1)[0])
    # `frame` has one row per table, but `dataset_path` strips the table name — dedupe so a
    # multi-table dataset doesn't produce repeated identical entries (inflated report counts,
    # redundant HEAD/DELETE calls on the same key).
    inactive = inactive.drop_duplicates(["channel", "namespace", "version", "dataset"])

    def to_entries(rows: pd.DataFrame) -> list[LatestDatasetPath]:
        entries = [
            LatestDatasetPath(
                catalog_path=str(row.dataset_path),
                namespace=str(row.namespace),
                dataset=str(row.dataset),
                version=str(row.version),
            )
            for row in rows.itertuples()
        ]
        return sorted(entries, key=lambda entry: entry.catalog_path)

    archived = to_entries(inactive[~inactive["dataset_key"].isin(active_keys)])
    superseded = to_entries(inactive[inactive["dataset_key"].isin(active_keys)])
    return archived, superseded


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
        dimension_values, representative_dimensions = _dimension_values_from_table_data(
            dataset_path=dataset_path,
            table_name=table_meta.short_name,
            formats=formats,
            table_meta=table_meta,
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
                dimension_values=dimension_values,
                representative_dimensions=representative_dimensions,
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
            "archived": len(result.archived_entries),
        },
        "emitted": result.emitted,
        "skipped": [_quality_to_record(item, include_blockers=True) for item in result.skipped],
        "warnings": [_quality_to_record(item, include_blockers=False) for item in result.warnings],
        "archived": [entry.catalog_path for entry in result.archived_entries],
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

    tb = _read_table_columns(dataset_path=dataset_path, table_name=table_name, formats=formats, columns=wanted_columns)
    if tb is None:
        return None, "Worldwide" if "country" in columns else None

    temporal_coverage = _temporal_coverage_from_years(tb["year"]) if "year" in tb.columns else None
    spatial_coverage = "Worldwide" if "country" in tb.columns or "country" in columns else None
    return temporal_coverage, spatial_coverage


def _dimension_values_from_table_data(
    *, dataset_path: Path, table_name: str, formats: list[str], table_meta: TableMeta
) -> tuple[dict[str, list[Any]], dict[str, Any]]:
    """Read distinct values per dimension column, plus one representative dimension combination.

    Long-format tables declare their dimension columns in ``TableMeta.dimensions``; the values
    only exist in the data. The representative combination (the most frequent one) is used to
    render Jinja-templated variable metadata into a concrete example.
    """
    slugs = [
        dimension["slug"]
        for dimension in table_meta.dimensions or []
        if dimension["slug"] not in ENTITY_TIME_DIMENSIONS
    ]
    if not slugs:
        return {}, {}
    df = _read_table_columns(dataset_path=dataset_path, table_name=table_name, formats=formats, columns=slugs)
    if df is None:
        return {}, {}

    dimension_values: dict[str, list[Any]] = {}
    for slug in slugs:
        values = [_as_python_scalar(value) for value in df[slug].dropna().unique()]
        try:
            values.sort()
        except TypeError:
            values.sort(key=str)
        dimension_values[slug] = values

    representative_dimensions: dict[str, Any] = {}
    combos = df.dropna()
    if not combos.empty:
        top = combos.value_counts().index[0]
        if not isinstance(top, tuple):
            top = (top,)
        representative_dimensions = {slug: _as_python_scalar(value) for slug, value in zip(combos.columns, top)}
    return dimension_values, representative_dimensions


def _as_python_scalar(value: Any) -> Any:
    # numpy scalars compare fine but serialize badly (json.dump chokes on np.int64).
    return value.item() if hasattr(value, "item") else value


def _read_table_columns(
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
