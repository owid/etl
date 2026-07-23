"""Build the "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).

Mirrors how a regular chart's own download works: the zip is built fresh per
request by a Cloudflare Function on the grapher side
(functions/_common/mdimDownloadFunctions.ts), not baked once and stored at a
fixed URL. ETL's job is the part that genuinely benefits from running once at
publish time rather than per-request: joining every view's indicator into one
wide table (no per-view HTTP fetch), resolving each indicator's real grapher
variable ID, AND writing the CSV in its final downloadable shape (Entity/Code/
Year columns, long display-name headers, numbers formatted) -- so the
Cloudflare Function never has to parse or rebuild the CSV. On a real
covid-scale MDIM (~590k rows), doing that row-by-row rebuild inside a Worker
would risk the 128MB isolate memory limit; doing it once in pandas at build
time doesn't. The Cloudflare Function fetches the finished CSV + a small
indicator index from R2 on every download request and does the rest (real
per-indicator metadata, citations, readme) live, so metadata still stays
fresh from the Data API.

KNOWN LIMITATION -- there is still a size ceiling, just a much higher one.
The Cloudflare Function fetches the whole CSV into memory and hands it to
littlezipper, which buffers the zip's output too (see the doc comment in
mdimDownloadFunctions.ts) -- roughly 2x the CSV's own size on top of it, all
within the Worker's 128MB isolate limit. Measured 2026-07-23 against the
largest real table in the codebase (covid/latest/cases_deaths, 589,632 rows):
the resulting wide CSV was 34.8MB and the live route on staging returned 200
in ~2s. That's the actual biggest case in the current MDIM set and it's
comfortably fine, but a future MDIM with a much wider table (many more
indicators than covid_deaths' 6, or more rows) could eventually cross the
line -- there was no complaint against a real ceiling, only headroom that
looked large before it was measured.

If that happens, the fallback (considered and deliberately not built,
because nothing needs it yet) is to split into two downloads instead of one
zip: link directly to this module's already-final-format CSV in R2 (zero
Cloudflare Function cost, since the file needs no further processing) plus a
separate, small, dynamically-built zip containing just metadata.json +
readme.md (cheap regardless of row count, since it's O(indicators) not
O(rows)). The cost is UX, not implementation: two links/files instead of one
zip shaped like a chart's own download. All the ETL-side work above (entity
codes, long names) stays exactly as useful for that path as for this one.
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
    dimension, they don't add new data.

    KNOWN GAP -- that assumption isn't always true. A subagent spot-check
    (2026-07-23) of natural-disasters-deaths found its "all disasters
    combined" view is missing from the "complete" package. That MDIM calls
    add_total_indicator_for_map() to add a genuinely NEW total indicator
    that exists only on a grouped view's map tab, not on any regular view --
    skipping all grouped views here means this function never sees it, so
    the wide table silently ends up incomplete for any MDIM using that
    pattern. Not fixed yet: doing so means telling apart "grouped view that
    only re-displays existing indicators" from "grouped view that also
    introduces a new one," which isn't something `view.is_grouped` alone
    can answer."""
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


def _resolve_entity_codes(names: list[str]) -> dict[str, str]:
    """Look up OWID entity codes for a list of country/region names, via the
    same DB -- no need to go through the Data API for this."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from etl.config import OWID_ENV
    from etl.grapher.model import Entity

    with Session(OWID_ENV.engine) as session:
        query = select(Entity.name, Entity.code).where(Entity.name.in_(names))
        return {name: code for name, code in session.execute(query) if code}


def _fetch_variables(variable_ids: list[int]):
    """Fetch full Variable rows (title/display/presentation fields) for a list
    of variable IDs -- the same fields the Data API serves, straight from the
    DB ETL already upserted them into."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from etl.config import OWID_ENV
    from etl.grapher.model import Variable

    with Session(OWID_ENV.engine) as session:
        query = select(Variable).where(Variable.id.in_(variable_ids))
        return {var.id: var for var in session.scalars(query).all()}


def _compute_long_column_name(var) -> str:
    """Mirrors computeLongColumnName in mdimDownloadFunctions.ts (owid-grapher)
    field-for-field. Computed once here, in ETL, rather than from live Data API
    data in the Cloudflare Function -- it becomes both the CSV header and
    metadata.json's column key, so the two can't drift from each other."""
    display_name = (var.display or {}).get("name")
    title_public = var.titlePublic or display_name or var.name
    title = title_public
    if var.attributionShort and var.titleVariant:
        title = f"{title} – {var.titleVariant} – {var.attributionShort}"
    elif var.titleVariant:
        title = f"{title} – {var.titleVariant}"
    elif var.attributionShort:
        title = f"{title} – {var.attributionShort}"
    if display_name and display_name != title_public:
        title = f"{title} ({display_name})"
    return title


def _format_numeric_series(s: pd.Series) -> pd.Series:
    """Print whole numbers without a trailing ".0" ("11", not "11.0") and NaN
    as an empty cell -- matches how grapher's own CSV writer serializes
    values. One-time cost at ETL build time, not per download request.

    Also rounds to ~6 significant figures before printing. Most OWID
    indicator columns are stored as float32; naively widening to float64 and
    calling str() surfaces float32's inherent rounding error as a long,
    meaningless decimal tail (e.g. float32(183.3) prints as
    "183.3000030517578"). Found spot-checking real covid/natural-disasters
    packages 2026-07-23. Rounds via %g (so scientific notation never survives
    into the final string -- %g would use it for small values, this reformats
    the already-rounded value with plain fixed-point notation instead)."""
    if not pd.api.types.is_numeric_dtype(s):
        return s

    def fmt(v):
        if pd.isna(v):
            return ""
        f = float(v)
        if f.is_integer():
            return str(int(f))
        rounded = float(f"{f:.6g}")
        if rounded.is_integer():
            return str(int(rounded))
        text = f"{rounded:.10f}".rstrip("0")
        return text if not text.endswith(".") else text + "0"

    return s.map(fmt)


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

    variables = _fetch_variables(list(variable_ids.values()))

    # Long display name per wide-table column, computed once here so it can't
    # drift from what the Cloudflare Function later uses as metadata.json's
    # column key -- becomes both the CSV header and that key.
    long_names: dict[str, str] = {}
    seen_long_names: dict[str, str] = {}
    for wide_name, catalog_path in column_to_catalog_path.items():
        var = variables.get(variable_ids.get(catalog_path))
        if var is None:
            continue
        long_name = _compute_long_column_name(var)
        existing = seen_long_names.get(long_name)
        if existing and existing != wide_name:
            log.warning(
                "download_package.column_name_collision",
                long_name=long_name,
                columns=[existing, wide_name],
            )
        seen_long_names[long_name] = wide_name
        long_names[wide_name] = long_name

    indicators = [
        {
            "wideColumnName": wide_name,
            "catalogPath": catalog_path,
            "owidVariableId": variable_ids.get(catalog_path),
            "longName": long_names.get(wide_name, wide_name),
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

    # Write the CSV in its FINAL downloadable shape -- Entity/Code/Year, long
    # display-name headers, numbers formatted -- so the Cloudflare Function
    # can pass the bytes straight into the zip without parsing a single row.
    time_col = _time_column(wide)
    time_header = "Day" if time_col == "date" else "Year"
    data_cols = [c for c in wide.columns if c not in ("country", time_col)]
    entity_codes = _resolve_entity_codes(wide["country"].unique().tolist())

    final = pd.DataFrame(
        {
            "Entity": wide["country"],
            # .astype(str) first -- "country" is often a categorical column,
            # and .map() on a Categorical can return a Categorical whose
            # .fillna("") then fails ("new category") if "" isn't already
            # one of its categories.
            "Code": wide["country"].astype(str).map(entity_codes).fillna(""),
            time_header: wide[time_col],
        }
    )
    for c in data_cols:
        final[long_names.get(c, c)] = _format_numeric_series(wide[c])

    csv_path = dest_dir / "wide.csv"
    final.to_csv(csv_path, index=False)
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
