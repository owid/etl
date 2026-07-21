"""Build a "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).
Given the source tables that feed a collection's views (each with country +
year keys, and per-column `dimensions` metadata), this joins them into one
wide table and writes a CSV + manifest.json + README, zipped together.

Scope note: this assembles the wide table from the tables the calling script
already has in memory, keyed on the per-column `dimensions` metadata those
tables carry (the pattern `adjust_dimensions_schooling`-style steps already
use). It does not yet re-resolve an arbitrary collection's indicators from
catalog paths alone — see the "net-new" pieces in
mdim-downloads/solution-space/etl-feasibility.md.
"""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

log = get_logger()


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


def build_wide_table(tables: list[Table]) -> Table:
    """Outer-join tables on country+year, renaming each value column from its own
    `dimensions` metadata (e.g. {"metric_type": "average_years_schooling", "sex": "both"}
    -> "average_years_schooling__level_all__sex_both")."""
    wide = None
    for tb in tables:
        rename = {}
        for col in tb.columns:
            if col in ("country", "year"):
                continue
            dims = getattr(tb[col].metadata, "dimensions", None) or {}
            short = tb[col].metadata.original_short_name or col
            rename[col] = _wide_column_name(short, dims)
        tb = tb.rename(columns=rename)
        wide = tb if wide is None else pr.merge(wide, tb, on=["country", "year"], how="outer")
    assert wide is not None, "build_wide_table requires at least one table"
    return wide.sort_values(["country", "year"]).reset_index(drop=True)


def _dimension_keys(tables: list[Table]) -> set[str]:
    keys: set[str] = set()
    for tb in tables:
        for col in tb.columns:
            dims = getattr(tb[col].metadata, "dimensions", None) or {}
            keys.update(k for k, v in dims.items() if v)
    return keys


def _render_readme(title: str, slug: str, value_columns: list[str]) -> str:
    columns_list = "\n".join(f"- `{c}`" for c in value_columns)
    return f"""# {title} — complete dataset

Prototype package for the mdim-downloads project. Bundles every dimension
combination of this dataset into one wide-format CSV, instead of the
per-view download you get from the chart itself.

## Files

- `{slug}.csv` — one row per country/year, one column per indicator ×
  dimension combination.
- `manifest.json` — file list, indicator/row counts, generation date.
- This README.

## Columns

`country`, `year`, then:

{columns_list}

Column names encode their dimension values (e.g. `sex_both`, `level_all`) —
see manifest.json for the full dimension list.
"""


def build_download_package(
    tables: list[Table],
    dest_dir: Path,
    title: str,
    slug: str,
) -> DownloadPackageResult:
    dest_dir.mkdir(parents=True, exist_ok=True)

    wide = build_wide_table(tables)
    value_columns = [c for c in wide.columns if c not in ("country", "year")]
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
        "dimensions": sorted(_dimension_keys(tables)),
    }
    manifest_path = dest_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    readme_path = dest_dir / "readme.md"
    readme_path.write_text(_render_readme(title, slug, value_columns))

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
