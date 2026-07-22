"""Build a "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).
Two ways to build the wide table this package is based on:

- `build_download_package(tables=[...], ...)` — pass the tables a script
  already has in memory, keyed on each column's own `dimensions` metadata
  (the pattern `adjust_dimensions_schooling`-style steps use). Fast, but only
  as good as whatever the script itself decorated its in-memory copies with.
- `build_download_package_for_collection(collection, ...)` — generic: derives
  the indicator list and their dimension values from the Collection's own
  *views* (skipping grouped/comparison views via `View.is_grouped`, and
  deduplicating by catalog path so an indicator reused across several views
  is only included once), then loads each underlying table fresh from the
  on-disk catalog. Doesn't require the calling script to hand-build anything,
  and reads the pristine on-disk column metadata rather than whatever a
  script may have overwritten in memory (avoiding at least one class of bugs
  in the process -- see mdim-downloads/solution-space/etl-feasibility.md).
"""

from __future__ import annotations

import json
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from owid.catalog import Dataset as CatalogDataset
from owid.catalog import Table, s3_utils
from owid.catalog import processing as pr
from structlog import get_logger

from etl.paths import DATA_DIR

if TYPE_CHECKING:
    from etl.collection.model.core import Collection

log = get_logger()

# Grapher tables are always keyed on country + one of these time columns --
# annual data uses "year", daily data (e.g. covid) uses "date". Never both,
# and (so far, empirically) never anything else.
TIME_COLUMN_CANDIDATES = ("year", "date")


class MixedTimeGranularityError(ValueError):
    """Raised when a collection's indicators mix annual ("year") and daily
    ("date") tables -- joining those needs a resampling decision this
    prototype doesn't make for you. See mdim-downloads status.md."""


def _time_column(tb: Table) -> str:
    candidates = [c for c in TIME_COLUMN_CANDIDATES if c in tb.columns]
    if len(candidates) != 1:
        raise ValueError(f"Expected exactly one of {TIME_COLUMN_CANDIDATES} in columns, found {candidates}")
    return candidates[0]


@dataclass
class DownloadPackageResult:
    zip_path: Path
    file_count: int
    row_count: int
    size_bytes: int
    last_updated: str

    def to_config(self, url: str) -> dict:
        """Shape matching MultiDimDataPageConfig.downloadPackage on the grapher side."""
        return {
            "url": url,
            "fileCount": self.file_count,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
            "lastUpdated": self.last_updated,
        }


def _dimension_suffix(col_dimensions: dict) -> str:
    parts = [f"{key}_{col_dimensions[key]}" for key in sorted(col_dimensions) if col_dimensions[key]]
    return "__".join(parts)


def _wide_column_name(short_name: str, col_dimensions: dict) -> str:
    suffix = _dimension_suffix(col_dimensions)
    return f"{short_name}__{suffix}" if suffix else short_name


def _outer_join_on_key(tables: list[Table]) -> Table:
    time_cols = {_time_column(tb) for tb in tables}
    if len(time_cols) > 1:
        raise MixedTimeGranularityError(
            f"Tables use different time columns ({sorted(time_cols)}) -- can't outer-join "
            "annual and daily data without a resampling decision."
        )
    key = ["country", time_cols.pop()]
    wide = None
    for tb in tables:
        wide = tb if wide is None else pr.merge(wide, tb, on=key, how="outer")
    assert wide is not None, "no tables to join"
    return wide.sort_values(key).reset_index(drop=True)


def build_wide_table(tables: list[Table]) -> Table:
    """Outer-join tables on country + year/date, renaming each value column from its
    own `dimensions` metadata (e.g. {"metric_type": "average_years_schooling", "sex":
    "both"} -> "average_years_schooling__level_all__sex_both")."""
    renamed = []
    for tb in tables:
        key_cols = ("country", _time_column(tb))
        rename = {}
        for col in tb.columns:
            if col in key_cols:
                continue
            dims = getattr(tb[col].metadata, "dimensions", None) or {}
            short = tb[col].metadata.original_short_name or col
            rename[col] = _wide_column_name(short, dims)
        renamed.append(tb.rename(columns=rename))
    return _outer_join_on_key(renamed)


def _dimension_keys(tables: list[Table]) -> set[str]:
    keys: set[str] = set()
    for tb in tables:
        for col in tb.columns:
            dims = getattr(tb[col].metadata, "dimensions", None) or {}
            keys.update(k for k, v in dims.items() if v)
    return keys


def _iter_used_indicators(collection: Collection):
    """Yield (catalog_path, view_dimensions) once per distinct indicator, in
    first-seen order, skipping views created by group_views() -- those just
    re-display already-included indicators under a synthetic comparison
    dimension, they don't add new data."""
    seen: set[str] = set()
    for view in collection.views:
        if view.is_grouped:
            continue
        for ind in view.indicators.y or []:
            if ind.catalogPath in seen:
                continue
            seen.add(ind.catalogPath)
            yield ind.catalogPath, view.dimensions


def _split_catalog_path(catalog_path: str) -> tuple[str, str, str]:
    """ "grapher/un/2025-05-07/undp_hdr/undp_hdr_sex#mys" -> (dataset_dir, table_name, column)."""
    dataset_part, column = catalog_path.split("#")
    *dataset_segments, table_name = dataset_part.split("/")
    return "/".join(dataset_segments), table_name, column


def build_wide_table_for_collection(collection: Collection) -> Table:
    """Generic version of build_wide_table: resolves the indicator list and their
    dimension values from the collection's own views, and loads each underlying
    table fresh from the on-disk catalog (no dependency on any script's in-memory
    tables)."""
    by_table: dict[tuple[str, str], list[tuple[str, dict]]] = defaultdict(list)
    for catalog_path, dims in _iter_used_indicators(collection):
        dataset_dir, table_name, column = _split_catalog_path(catalog_path)
        by_table[(dataset_dir, table_name)].append((column, dims))

    dataset_cache: dict[str, CatalogDataset] = {}
    renamed_tables = []
    for (dataset_dir, table_name), cols in by_table.items():
        if dataset_dir not in dataset_cache:
            dataset_cache[dataset_dir] = CatalogDataset(DATA_DIR / dataset_dir)
        tb = dataset_cache[dataset_dir][table_name].reset_index()

        rename = {}
        keep = ["country", _time_column(tb)]
        for column, dims in cols:
            if column not in tb.columns:
                log.warning(
                    "download_package.column_missing",
                    dataset_dir=dataset_dir,
                    table_name=table_name,
                    column=column,
                )
                continue
            short = tb[column].metadata.original_short_name or column
            rename[column] = _wide_column_name(short, dims)
            keep.append(column)
        renamed_tables.append(tb[keep].rename(columns=rename))

    return _outer_join_on_key(renamed_tables)


def _render_readme(title: str, slug: str, value_columns: list[str], time_column: str) -> str:
    columns_list = "\n".join(f"- `{c}`" for c in value_columns)
    return f"""# {title} — complete dataset

Prototype package for the mdim-downloads project. Bundles every dimension
combination of this dataset into one wide-format CSV, instead of the
per-view download you get from the chart itself.

## Files

- `{slug}.csv` — one row per country/{time_column}, one column per indicator
  × dimension combination.
- `manifest.json` — file list, indicator/row counts, generation date.
- This README.

## Columns

`country`, `{time_column}`, then:

{columns_list}

Column names encode their dimension values (e.g. `sex_both`, `level_all`) —
see manifest.json for the full dimension list.
"""


def _write_package_files(
    wide: Table,
    dest_dir: Path,
    title: str,
    slug: str,
    dimension_keys: set[str],
) -> DownloadPackageResult:
    dest_dir.mkdir(parents=True, exist_ok=True)

    time_column = _time_column(wide)
    value_columns = [c for c in wide.columns if c not in ("country", time_column)]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    csv_name = f"{slug}.csv"
    csv_path = dest_dir / csv_name
    wide.to_csv(csv_path, index=False)

    manifest = {
        "slug": slug,
        "title": title,
        "generatedAt": now,
        "files": [csv_name, "manifest.json", "readme.md"],
        "fileCount": len(value_columns),
        "rowCount": len(wide),
        "dimensions": sorted(dimension_keys),
    }
    manifest_path = dest_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    readme_path = dest_dir / "readme.md"
    readme_path.write_text(_render_readme(title, slug, value_columns, time_column))

    zip_path = dest_dir / f"{slug}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in (csv_path, manifest_path, readme_path):
            zf.write(p, p.name)

    log.info(
        "download_package.built",
        zip_path=str(zip_path),
        rows=len(wide),
        indicators=len(value_columns),
        size_bytes=zip_path.stat().st_size,
    )

    return DownloadPackageResult(
        zip_path=zip_path,
        file_count=len(value_columns),
        row_count=len(wide),
        size_bytes=zip_path.stat().st_size,
        last_updated=now,
    )


def build_download_package(
    tables: list[Table],
    dest_dir: Path,
    title: str,
    slug: str,
) -> DownloadPackageResult:
    """Build the package from tables the caller already has in memory."""
    wide = build_wide_table(tables)
    return _write_package_files(wide, dest_dir, title, slug, _dimension_keys(tables))


def build_download_package_for_collection(
    collection: Collection,
    dest_dir: Path,
    title: str | None = None,
    slug: str | None = None,
) -> DownloadPackageResult:
    """Build the package generically from the collection's own views -- no
    script-specific wiring needed beyond calling this once before c.save()."""
    wide = build_wide_table_for_collection(collection)
    dimension_keys = {d.slug for d in collection.dimensions}
    return _write_package_files(
        wide,
        dest_dir,
        title or collection.title["title"],
        slug or collection.short_name,
        dimension_keys,
    )


# Buckets with a known public HTTPS domain -- mirrors the pattern already used
# for owid_co2.py / owid_energy.py / income_distribution.py etc. (S3_BUCKET_NAME
# = "owid-public", served at owid-public.owid.io). Add more here if this ever
# moves to a different bucket.
PUBLIC_BUCKET_DOMAINS = {
    "owid-public": "https://owid-public.owid.io",
}


def upload_to_r2(zip_path: Path, s3_key: str, public: bool = True) -> str:
    """Upload the built zip to R2. `s3_key` is bucket/path, e.g.
    "owid-public/data/mdim-downloads/years_of_schooling/years_of_schooling.zip".

    Returns a public https URL if the bucket is in PUBLIC_BUCKET_DOMAINS,
    otherwise the s3:// URI (which needs credentials to fetch -- not usable
    as a browser download link). `public=True` only sets the object's ACL to
    public-read; the https URL is only reachable if the bucket also has a
    known public domain, which is what PUBLIC_BUCKET_DOMAINS records."""
    bucket, key = s3_key.split("/", 1)
    s3_utils.upload(f"s3://{s3_key}", zip_path, public=public, downloadable=True)
    if bucket in PUBLIC_BUCKET_DOMAINS:
        return f"{PUBLIC_BUCKET_DOMAINS[bucket]}/{key}"
    return f"s3://{s3_key}"
