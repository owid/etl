"""Build the "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).

Mirrors how a regular chart's own download works: the zip is built fresh per
request by a Cloudflare Function on the grapher side
(functions/_common/mdimDownloadFunctions.ts), not baked once and stored at a
fixed URL. ETL's job is only the part that genuinely benefits from running at
publish time rather than per-request: joining every view's indicator into one
wide table (no per-view HTTP fetch) and resolving each indicator's real
grapher variable ID. `stage_download_package_for_collection()` uploads that
join (wide.csv) plus an indicator index to R2; the grapher-side function
fetches those two small files on every download request and does the rest
(real per-indicator metadata, citations, readme) live.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
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


def _resolve_time_column(tb: Table) -> tuple[Table, str]:
    """Return (possibly-converted table, time column name).

    Grapher has a second daily-data convention this module didn't originally
    account for: some indicators store a day-offset integer in a column
    literally named "year" (`display.yearIsDay=True`, `display.zeroDay` gives
    the reference date) rather than in an actual "date" column. Confirmed on
    covid/latest/cases_deaths: display == {"zeroDay": "2020-01-21",
    "yearIsDay": True, ...}. Left unconverted, that integer gets silently
    treated as a calendar year (e.g. -17, 2350) -- wrong, not just mislabeled.
    Convert it to a real "date" column before it reaches any join/output.
    """
    if "date" in tb.columns:
        # Normalize dtype regardless of source -- real "date" columns show up
        # as category, datetime64, or object depending on the table, and a
        # collection can combine several of those. Merging mismatched dtypes
        # on the join key raises ("merge on object and datetime64[ns] columns
        # for key 'date'"), so every table's "date" is cast to the same
        # plain ISO-string form before it reaches any join.
        tb = tb.copy()
        tb["date"] = pd.to_datetime(tb["date"]).dt.date.astype(str)
        return tb, "date"
    if "year" not in tb.columns:
        raise ValueError(f"Expected one of {TIME_COLUMN_CANDIDATES} in columns, found {list(tb.columns)}")

    value_cols = [c for c in tb.columns if c not in ("country", "year")]
    displays = [getattr(tb[c].metadata, "display", None) or {} for c in value_cols]
    if not any(d.get("yearIsDay") for d in displays):
        return tb, "year"

    zero_days = {d["zeroDay"] for d in displays if d.get("yearIsDay") and d.get("zeroDay")}
    if len(zero_days) != 1:
        raise ValueError(f"Expected exactly one zeroDay for yearIsDay columns, found {zero_days}")
    zero_day = pd.Timestamp(zero_days.pop())

    tb = tb.copy()
    tb["date"] = (zero_day + pd.to_timedelta(tb["year"], unit="D")).dt.date.astype(str)
    tb = tb.drop(columns=["year"])
    return tb, "date"


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


def build_wide_table_for_collection(collection: Collection) -> tuple[Table, dict[str, str]]:
    """Resolves the indicator list and their dimension values from the
    collection's own views, and loads each underlying table fresh from the
    on-disk catalog (no dependency on any script's in-memory tables). Returns
    (wide_table, {wide_column_name: catalog_path}) -- the second is needed to
    hand off metadata assembly to grapher, which looks up indicators by
    catalog path / variable ID, not by our wide-CSV column name."""
    by_table: dict[tuple[str, str], list[tuple[str, dict]]] = defaultdict(list)
    for catalog_path, dims in _iter_used_indicators(collection):
        dataset_dir, table_name, column = _split_catalog_path(catalog_path)
        by_table[(dataset_dir, table_name)].append((column, dims))

    dataset_cache: dict[str, CatalogDataset] = {}
    renamed_tables = []
    column_to_catalog_path: dict[str, str] = {}
    for (dataset_dir, table_name), cols in by_table.items():
        if dataset_dir not in dataset_cache:
            dataset_cache[dataset_dir] = CatalogDataset(DATA_DIR / dataset_dir)
        tb = dataset_cache[dataset_dir][table_name].reset_index()
        tb, time_col = _resolve_time_column(tb)

        rename = {}
        keep = ["country", time_col]
        for column, dims in cols:
            if column not in tb.columns:
                log.warning(
                    "download_package.column_missing",
                    dataset_dir=dataset_dir,
                    table_name=table_name,
                    column=column,
                )
                continue
            wide_name = _wide_column_name(tb[column].metadata.original_short_name or column, dims)
            rename[column] = wide_name
            column_to_catalog_path[wide_name] = f"{dataset_dir}/{table_name}#{column}"
            keep.append(column)
        renamed_tables.append(tb[keep].rename(columns=rename))

    return _outer_join_on_key(renamed_tables), column_to_catalog_path


def resolve_variable_ids(catalog_paths: list[str]) -> dict[str, int]:
    """Look up real grapher variable IDs for a list of catalog paths, via the
    same DB the ETL step already upserted these indicators into."""
    from sqlalchemy.orm import Session

    from etl.config import OWID_ENV
    from etl.grapher.model import Variable

    with Session(OWID_ENV.engine) as session:
        return Variable.catalog_paths_to_variable_ids(session, catalog_paths)


# Buckets with a known public HTTPS domain -- mirrors the pattern already used
# for owid_co2.py / owid_energy.py / income_distribution.py etc. (S3_BUCKET_NAME
# = "owid-public", served at owid-public.owid.io). Add more here if this ever
# moves to a different bucket.
PUBLIC_BUCKET_DOMAINS = {
    "owid-public": "https://owid-public.owid.io",
}


@dataclass
class StagedPackageResult:
    csv_url: str
    indicators_url: str
    row_count: int
    indicator_count: int

    def to_config(self) -> dict:
        """Shape matching MultiDimDataPageConfig.downloadPackage on the grapher
        side. No `url` here -- the browser-facing download link is the dynamic
        build route, computed from the page's own slug on the grapher side,
        not stored (same convention as a chart's own `.zip` link)."""
        return {
            "csvUrl": self.csv_url,
            "indicatorsUrl": self.indicators_url,
            "fileCount": self.indicator_count,
            "rowCount": self.row_count,
        }


def stage_download_package_for_collection(
    collection: Collection,
    dest_dir: Path,
    s3_prefix: str,
) -> StagedPackageResult:
    """Builds the wide table (the part that benefits from local data access --
    no per-view HTTP fetches, validated across 12 real MDIMs) and uploads it
    plus an indicator/column-name index to R2. A Cloudflare Function on the
    grapher side (fetchCompleteDatasetZipForGrapher in
    mdimDownloadFunctions.ts) fetches these on every download request and
    builds the real metadata.json + readme.md + zip live, reusing its
    existing (tested, correct) citation/title-formatting code instead of a
    Python reimplementation -- see mdim-downloads/solution-space/etl-feasibility.md.

    `s3_prefix` is bucket/path, e.g. "owid-public/data/mdim-downloads/years_of_schooling".
    This is the permanent, canonical location grapher reads from on every
    request -- not a staging area for a later build step.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    wide, column_to_catalog_path = build_wide_table_for_collection(collection)

    variable_ids = resolve_variable_ids(list(column_to_catalog_path.values()))
    missing = [p for p in column_to_catalog_path.values() if p not in variable_ids]
    if missing:
        log.warning("download_package.variable_id_missing", catalog_paths=missing)

    indicators = [
        {
            "wideColumnName": wide_name,
            "catalogPath": catalog_path,
            "owidVariableId": variable_ids.get(catalog_path),
        }
        for wide_name, catalog_path in column_to_catalog_path.items()
        if catalog_path in variable_ids
    ]

    # Chart-level context the grapher-side build needs for metadata.json's
    # `chart` block (mirroring the single-chart download's shape) -- only
    # available here, so it rides along with the indicator index.
    index = {
        "title": collection.title.get("title"),
        "titleVariant": collection.title.get("title_variant"),
        "defaultSelection": collection.default_selection,
        "indicators": indicators,
    }

    csv_path = dest_dir / "wide.csv"
    wide.to_csv(csv_path, index=False)
    indicators_path = dest_dir / "indicators.json"
    indicators_path.write_text(json.dumps(index, indent=2))

    csv_key = f"{s3_prefix}/wide.csv"
    indicators_key = f"{s3_prefix}/indicators.json"
    s3_utils.upload(f"s3://{csv_key}", csv_path, public=True)
    s3_utils.upload(f"s3://{indicators_key}", indicators_path, public=True)

    bucket = s3_prefix.split("/", 1)[0]
    base_url = PUBLIC_BUCKET_DOMAINS.get(bucket, f"s3://{bucket}") + "/" + s3_prefix.split("/", 1)[1]

    log.info(
        "download_package.staged",
        rows=len(wide),
        indicators=len(indicators),
        csv_url=f"{base_url}/wide.csv",
    )

    return StagedPackageResult(
        csv_url=f"{base_url}/wide.csv",
        indicators_url=f"{base_url}/indicators.json",
        row_count=len(wide),
        indicator_count=len(indicators),
    )
