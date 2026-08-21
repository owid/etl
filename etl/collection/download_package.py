"""Build the "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).

The whole package -- wide CSV + metadata.json + readme.md, zipped -- is built
here, once, at ETL publish time, and uploaded to R2. The grapher side does
nothing but link to it.

Three objects go up per collection, for two different audiences:

  * `<slug>.complete-dataset.zip` -- the download button. One click, a CSV that
    opens in a spreadsheet, and a readme explaining every column.
  * `<slug>.parquet` and `<slug>.metadata.json` -- the Data API section, for
    people and agents reading the data with code. Parquet because it can be
    queried in place: DuckDB over HTTP prunes to the columns asked for, so one
    indicator out of years-of-schooling costs 37kB rather than the whole file.
    See `_write_parquet`.

An earlier iteration split the work: ETL staged a wide CSV + an indicator index
to R2, and a Cloudflare Function assembled the zip fresh on every request, so
it could reuse grapher's own citation/readme formatting code instead of a
Python reimplementation. That mirrored how a regular chart's own `.zip`
download works. We moved off it deliberately (Marcel, 2026-07-27): there is
essentially no upside to building per-request for a dataset that only changes
when ETL republishes it, and Worker limits are restrictive enough to be a real
risk as MDIMs grow. What it costs is code duplication -- the formatting logic
now exists in both repos. See `download_package_format.py`, which is the port,
and keep the two in sync by hand.

What that buys, beyond avoiding the Worker limits:

  * No size ceiling at all. The old route held the whole CSV plus littlezipper's
    buffered zip output inside a 128MB Worker isolate; the largest real MDIM
    (covid_explorer: 101 indicators, 602k rows, 34.8MB CSV) fitted with less
    headroom than was comfortable, and there was a documented two-package
    fallback waiting for the first MDIM that didn't fit. R2 just serves bytes,
    so both the technical ceiling and the fallback design are gone. What
    remains is a policy one: `_check_package_size` refuses to publish a
    package big enough to suggest the wide CSV is the wrong format for that
    collection.
  * Real compression. littlezipper wrote stored (uncompressed) entries, so a
    34.8MB CSV was a ~34.8MB download. DEFLATE on a wide, sparse table does a
    lot better.
  * An exact `sizeBytes` for the UI, rather than an estimate.
  * One request for the user, and no per-download fan-out to the Data API.

The cost, other than duplication, is that metadata is frozen at publish time:
a citation or description edit only reaches the package when this step re-runs.
In practice a metadata change means a new ETL version, which re-runs the step
anyway -- but a manual edit made directly in the admin will not propagate.
"""

from __future__ import annotations

import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from owid.catalog import Dataset as CatalogDataset
from owid.catalog import Table, s3_utils
from owid.catalog import processing as pr
from owid.repack import repack_frame
from structlog import get_logger

from etl import config
from etl.collection.download_package_format import (
    IndicatorColumn,
    column_readme_text,
    dumps_like_json_stringify,
    get_attribution,
    get_title,
    metadata_column_entry,
)
from etl.paths import DATA_DIR

if TYPE_CHECKING:
    from etl.collection.model.core import Collection
from etl.grapher.helpers import SUB_YEARLY_TIME_INTERVALS

log = get_logger()

# Grapher tables are always keyed on country + one of these time columns --
# annual data uses "year", daily data (e.g. covid) uses "date". Never both,
# and (so far, empirically) never anything else.
TIME_COLUMN_CANDIDATES = ("year", "date")

# Tripwire, not a technical limit -- see _check_package_size.
MAX_PACKAGE_SIZE_BYTES = 10_000_000


class MixedTimeGranularityError(ValueError):
    """Raised when a collection's indicators mix annual ("year") and daily
    ("date") tables -- joining those needs a resampling decision this
    prototype doesn't make for you. See mdim-downloads status.md."""


class DownloadPackageTooLargeError(Exception):
    """Raised when the built package exceeds `MAX_PACKAGE_SIZE_BYTES`."""


class DuplicateColumnNameError(Exception):
    """Raised when two wide-table columns resolve to the same display name."""


def _check_column_names_unique(page_slug: str, columns: list[tuple[str, str, int, IndicatorColumn]]) -> None:
    """Refuse to publish a package whose columns don't have distinct names.

    `_long_column_name` is used three times over -- as the CSV header, as the
    Parquet field name, and as the key of that column's entry in
    `metadata.json` -- so two columns sharing one is not a cosmetic problem:

      * `metadata.json` is a dict keyed by that name, so one of the two
        indicators loses its entry entirely, silently.
      * Parquet permits duplicate field names, but readers don't cope. pandas
        refuses the file outright (`ArrowInvalid: Multiple matches for
        FieldRef.Name(...)`), so a single collision makes the whole package
        unreadable there. DuckDB is worse in a way: it renames the second
        column to `<name>_1` without complaint, so selecting the name by hand
        quietly returns only the first of the two.
      * CSV is the mild case -- pandas mangles the second header to
        `<name>.1` -- which is why this began life as a warning.

    Nothing upstream guarantees uniqueness, which is why this is checked rather
    than assumed. It holds across all 32 published MDIMs today, and the likely
    way to break it is an MDIM that joins two producers' versions of the same
    metric -- two indicators genuinely named "Population". If that happens the
    fix is a judgement call about those indicators (rename one upstream, or
    start folding the attribution into the column name), so it wants a human
    rather than a generated `_1` suffix, which is exactly the unhelpful thing
    DuckDB already does on its own.
    """
    seen: dict[str, list[str]] = defaultdict(list)
    for catalog_path, long_name, _variable_id, _col in columns:
        seen[long_name].append(catalog_path)
    collisions = {name: cols for name, cols in seen.items() if len(cols) > 1}
    if not collisions:
        return
    detail = "; ".join(f"{name!r} <- {sorted(cols)}" for name, cols in sorted(collisions.items()))
    raise DuplicateColumnNameError(
        f"Download package for {page_slug} has {len(collisions)} duplicate column "
        f"name(s): {detail}. See _check_column_names_unique in "
        "etl/collection/download_package.py."
    )


def _check_package_size(
    page_slug: str,
    zip_bytes: int,
    csv_bytes: int,
    max_size_bytes: int,
) -> None:
    """Refuse to publish a package that has outgrown a one-click browser download.

    Nothing technical breaks above the threshold: R2 serves an object of any
    size, and the zip is built once here rather than per request. The threshold
    is a tripwire, deliberately set an order of magnitude below where it would
    actually hurt, because the number it guards is a proxy for a design problem
    rather than the problem itself.

    That problem is the wide format. One CSV with one column per indicator per
    dimension combination is the right shape for the median MDIM -- today's are
    all a few MB zipped -- and it is what makes the package readable in a
    spreadsheet and keyed one-to-one to `metadata.json`'s per-column entries.
    But its width grows with the product of the dimension cardinalities while
    each new column is mostly empty, so a collection with a few more dimensions
    than usual produces a file that is enormous and almost entirely padding.
    A package that trips this is nearly always telling us the format is wrong
    for that collection, not that the data is unusually large.

    Note that this is a statement about the *zip* only. The Parquet published
    beside it does not have the problem: nulls are run-length-encoded definition
    levels, so the empty cells cost almost nothing (measured on
    years-of-schooling, wide 671kB vs long 680kB -- the shape barely matters),
    and a consumer reading it with DuckDB pulls only the columns they ask for.
    So an MDIM that trips this has a working programmatic path already; what it
    lacks is a sane one-click download.

    So when it trips, the fix is a judgement call about that collection, and
    the options are roughly:

      * Raise `max_size_bytes` for this one collection, if a multi-MB download
        is genuinely acceptable for its audience. Cheapest, and more often right
        than it looks, given the Parquet covers the people who would suffer most.
      * No zip for this collection, pointing people at the Parquet and the
        Python catalog library instead, which let them select the columns they
        actually want and skip the padding entirely.
      * Long format for this collection. Better in a text editor, worse in a
        spreadsheet, and the per-column metadata no longer lines up. Note that a
        format heuristic was tried before and abandoned -- the switching rule was
        hard to get right and cost us two code paths to maintain -- so prefer an
        explicit per-collection choice over reintroducing one. Least attractive
        of the three now that Parquet exists.

    Whichever it is, it wants a human decision, which is why this raises rather
    than warns.
    """
    if zip_bytes <= max_size_bytes:
        return
    raise DownloadPackageTooLargeError(
        f"Download package for {page_slug} is {zip_bytes / 1e6:.1f}MB zipped "
        f"({csv_bytes / 1e6:.1f}MB as CSV), over the {max_size_bytes / 1e6:.0f}MB "
        "threshold. This usually means the wide CSV is mostly empty cells; see "
        "_check_package_size in etl/collection/download_package.py for the options."
    )


def _time_column(tb: Table) -> str:
    candidates = [c for c in TIME_COLUMN_CANDIDATES if c in tb.columns]
    if len(candidates) != 1:
        raise ValueError(f"Expected exactly one of {TIME_COLUMN_CANDIDATES} in columns, found {candidates}")
    return candidates[0]


def _resolve_time_column(tb: Table) -> tuple[Table, str]:
    """Return (possibly-converted table, time column name).

    Sub-yearly data does not arrive in a "date" column. Grapher stores every
    interval shorter than a year as days-since-`display.zeroDay` integers in a
    column literally named "year" (`adapt_table_with_dates_to_grapher` writes
    it), with `display.timeInterval` saying how to read them. Left unconverted,
    those offsets are silently taken for calendar years -- covid's weekly cases
    published as `Year,-17` rather than 2020-01-04. So decode them to a real
    "date" column before they reach any join or output.

    `timeInterval` is the field to test: `yearIsDay` was removed, and
    `etl.grapher.helpers._validate_time_interval` now asserts against it. This
    module used to check the removed flag, which is why the bug above shipped --
    the branch could never be taken. "decade" stays on the year axis, since it
    codes a representative calendar year rather than an offset.
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
    displays = {c: (getattr(tb[c].metadata, "display", None) or {}) for c in value_cols}
    sub_yearly = {c for c, d in displays.items() if d.get("timeInterval") in SUB_YEARLY_TIME_INTERVALS}
    if not sub_yearly:
        return tb, "year"
    if len(sub_yearly) != len(value_cols):
        # Converting the table would turn the calendar years of the remaining columns
        # into nonsense dates, and leaving it alone does the same to the offsets. A
        # table this shape has to be split before it gets here.
        raise MixedTimeGranularityError(
            f"Table mixes sub-yearly and yearly columns on one 'year' column: "
            f"{sorted(sub_yearly)[:3]} are offsets, {sorted(set(value_cols) - sub_yearly)[:3]} are years."
        )

    zero_days = {d["zeroDay"] for c, d in displays.items() if c in sub_yearly and d.get("zeroDay")}
    if len(zero_days) != 1:
        raise ValueError(
            f"Expected exactly one zeroDay across the sub-yearly columns, found {sorted(zero_days)}. "
            "Without it the day offsets cannot be decoded."
        )
    zero_day = pd.Timestamp(zero_days.pop())

    tb = tb.copy()
    tb["date"] = (zero_day + pd.to_timedelta(tb["year"], unit="D")).dt.date.astype(str)
    tb = tb.drop(columns=["year"])
    return tb, "date"


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


def _used_indicators(collection: Collection) -> dict[str, list[dict]]:
    """Map each distinct indicator's catalog path to every dimension
    combination it is shown under -- keys and combinations both in first-seen
    order -- skipping views created by group_views(), since those just
    re-display already-included indicators under a synthetic comparison
    dimension and don't add new data.

    Usually a one-element list, but an indicator reachable from several views
    (a choice that doesn't affect that particular indicator) genuinely has
    more than one, and metadata.json reports all of them. Only the first is
    used to name the wide column, so column names stay stable regardless.

    KNOWN GAP -- the group_views() assumption above isn't always true. A spot-check
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
    used: dict[str, list[dict]] = {}
    for view in collection.views:
        if view.is_grouped:
            continue
        for ind in view.indicators.y or []:
            combinations = used.setdefault(ind.catalogPath, [])
            if view.dimensions not in combinations:
                combinations.append(view.dimensions)
    return used


def _split_catalog_path(catalog_path: str) -> tuple[str, str, str]:
    """ "grapher/un/2025-05-07/undp_hdr/undp_hdr_sex#mys" -> (dataset_dir, table_name, column)."""
    dataset_part, column = catalog_path.split("#")
    *dataset_segments, table_name = dataset_part.split("/")
    return "/".join(dataset_segments), table_name, column


def build_wide_table_for_collection(collection: Collection) -> tuple[Table, dict[str, list[dict]]]:
    """Resolves the indicator list and their dimension values from the
    collection's own views, and loads each underlying table fresh from the
    on-disk catalog (no dependency on any script's in-memory tables). Returns
    (wide_table, {catalog_path: dimension_combinations}) -- the second both
    enumerates the columns in first-seen order and carries what metadata.json
    needs to report each one's dimension structure.

    **The wide table's columns are named by catalog path**, which is the one name
    guaranteed to be unique: it is the key `_used_indicators` returns, so one
    column per indicator, and no two indicators can claim the same one. That
    matters more than it sounds. The name this replaced was rebuilt from the
    indicator's dimension-*stripped* short name plus the dimensions of the *view*
    that showed it -- and a view can show several indicators at once, which then
    have identical view dimensions by construction. poverty_pip is the measured
    case: its 34 indicators produced only 24 such names, with 4 names shared by
    14 indicators. Those are its stacked-area views, where one view shows three
    or four `headcount_between` bands ($1-3, $3-4.20, ...) that differ only in
    dimensions the view does not carry.

    Each collision silently overwrote the previous column's entry in the returned
    dicts, so the package would have shipped 24 columns and dropped 10 indicators
    without a word. It surfaced as a crash only because pandas refuses to assign
    a multi-column frame to a single column.

    These names are internal plumbing and never reach a reader: the CSV header,
    the Parquet field name and metadata.json's key are all `_long_column_name`,
    which is checked separately for uniqueness by `_check_column_names_unique`."""
    by_table: dict[tuple[str, str], list[tuple[str, list[dict]]]] = defaultdict(list)
    for catalog_path, combinations in _used_indicators(collection).items():
        dataset_dir, table_name, column = _split_catalog_path(catalog_path)
        by_table[(dataset_dir, table_name)].append((column, combinations))

    dataset_cache: dict[str, CatalogDataset] = {}
    renamed_tables = []
    column_to_dimensions: dict[str, list[dict]] = {}
    for (dataset_dir, table_name), cols in by_table.items():
        if dataset_dir not in dataset_cache:
            dataset_cache[dataset_dir] = CatalogDataset(DATA_DIR / dataset_dir)
        tb = dataset_cache[dataset_dir][table_name].reset_index()
        tb, time_col = _resolve_time_column(tb)

        rename = {}
        keep = ["country", time_col]
        for column, combinations in cols:
            if column not in tb.columns:
                log.warning(
                    "download_package.column_missing",
                    dataset_dir=dataset_dir,
                    table_name=table_name,
                    column=column,
                )
                continue
            catalog_path = f"{dataset_dir}/{table_name}#{column}"
            rename[column] = catalog_path
            column_to_dimensions[catalog_path] = combinations
            keep.append(column)
        renamed_tables.append(tb[keep].rename(columns=rename))

    return _outer_join_on_key(renamed_tables), column_to_dimensions


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


def resolve_page_slug(collection: Collection) -> str | None:
    """The MDIM's public page slug (e.g. "years-of-schooling"), or None if it has none.

    Not derivable from the catalog path -- grapher owns the mapping, and the
    slug is hyphenated where the catalog short name is underscored. Requires
    `collection.save()` to have run first, which is what creates the row.
    It matters here for more than the R2 filename: the readme and
    metadata.json both link to the real page URL.

    None means the collection has no data page. `put_mdim_config` sends only the
    config, never a slug -- that is assigned when someone publishes the MDIM in the
    admin -- so an MDIM whose export step runs but which was never published has a
    row and no slug. On a staging server that is the normal state of 13 of the 59
    multidim steps. Such a collection has nothing to attach a package to, which is
    why callers treat it as "nothing to publish" rather than an error.
    """
    from sqlalchemy.orm import Session

    from etl.config import OWID_ENV
    from etl.grapher.model import MultiDimDataPage

    with Session(OWID_ENV.engine) as session:
        mdim = MultiDimDataPage.load_mdim(session, catalogPath=collection.catalog_path)
    if mdim is None or not mdim.slug:
        return None
    return mdim.slug


def _fetch_indicator_metadata(variable_ids: list[int]) -> dict[int, dict]:
    """Fetch each indicator's public metadata JSON -- the very same artifact
    served at api.ourworldindata.org/v1/indicators/<id>.metadata.json.

    Using the published JSON rather than reading the DB columns directly is
    what makes `download_package_format.py` a pure formatting port: both it
    and owid-grapher's TypeScript start from byte-identical input.
    """
    from etl.config import OWID_ENV
    from etl.grapher.io import variable_metadata_df_from_s3

    metadata = variable_metadata_df_from_s3(variable_ids, workers=10, env=OWID_ENV)

    # A 404 comes back as an empty dict rather than raising, and an indicator
    # with no metadata would silently produce a readme section with no title,
    # no citation and no source -- fail loudly instead.
    missing = [vid for vid, meta in zip(variable_ids, metadata) if not meta]
    if missing:
        raise ValueError(f"No published metadata JSON for indicator(s) {missing} -- has the grapher step run?")

    return dict(zip(variable_ids, metadata))


def _dimension_definitions(collection: Collection) -> list[dict]:
    """metadata.json's top-level "dimensions" -- the collection's dimensions and
    choices, each as a stable slug plus the display name.

    This exists so a consumer can label a column's dimension combination
    without fetching the MDIM config separately: the per-column `dimensions`
    fields use slugs only, and these are the names they map to.
    """
    return [
        {
            "slug": dimension.slug,
            "name": dimension.name,
            "choices": [{"slug": choice.slug, "name": choice.name} for choice in dimension.choices],
        }
        for dimension in collection.dimensions
    ]


def _view_fields(combinations: list[dict], page_url: str) -> dict:
    """metadata.json's per-column `dimensions` / `url` / `otherViews` -- which
    dimension combination this column belongs to, and the page URL that opens it.

    Choice *slugs* rather than names, because they're stable across copy edits
    and they're the same tokens the page's query params use -- which is what
    makes the URL derivable, and gives a consumer the round trip from a CSV
    column back to the view it came from. Empty values are dropped because they
    identify nothing.

    Almost every column belongs to exactly one combination, so that one is
    inlined as flat `dimensions` + `url` fields and there's nothing else. A
    column *can* belong to several -- when a dimension is redundant for that
    indicator, e.g. un_wpp's age-0 deaths column, which is reachable as both
    `indicator=deaths` and `indicator=infant_deaths` because an infant death is
    a death at age 0. The extras then go in `otherViews`, so the rare case stays
    complete without every column paying for it with a nested list.

    The inlined one is simply the first view that referenced the indicator. It
    is not semantically privileged, and a consumer that wants every combination
    has to read `otherViews` too.
    """
    assert combinations, "a column exists because some view referenced it"
    entries = []
    for combination in combinations:
        dimensions = {slug: choice for slug, choice in combination.items() if choice}
        entries.append({"dimensions": dimensions, "url": f"{page_url}?{urlencode(dimensions)}"})

    primary, *rest = entries
    fields = {"dimensions": primary["dimensions"], "url": primary["url"]}
    if rest:
        fields["otherViews"] = rest
    return fields


def _long_column_name(col: IndicatorColumn) -> str:
    """The CSV header, the Parquet field name, and metadata.json's key.

    The indicator's own `name` -- its full ETL title -- rather than the display
    name that `get_title()` prefers. Display names are written for a chart, so
    they leave out whatever a drop-down selects: three LIS indicators named
    "Mean income (per day / per month / per year, after tax, equivalized)" all
    display as "Mean income (after tax)", which is both ambiguous and duplicated
    once the chart's period selector isn't there to supply the missing word. In
    a standalone file the self-contained name is the useful one.

    This also matches how grapher keys the per-chart `metadata.json` it serves
    from production -- `columns` there is keyed by `name`, with the display
    titles carried alongside as `titleShort`/`titleLong`. The readme's
    per-indicator headings still use `get_title()`, exactly as production's do.

    Measured over all 32 published MDIMs (568 indicators): unique everywhere,
    and 9 characters shorter at the median than the display-name construction
    this replaced. Uniqueness isn't guaranteed by anything upstream, though --
    see `_check_column_names_unique`.
    """
    return col.meta.get("name") or get_title(col)


def _write_parquet(
    keys: pd.DataFrame,
    wide: Table,
    columns: list[tuple[str, str, int, IndicatorColumn]],
    path: Path,
) -> None:
    """Write the wide table as Parquet, dtypes chosen by `repack_frame`.

    Why this exists alongside the CSV: Parquet is the artifact that can be
    *queried* rather than downloaded. DuckDB reads it over HTTP with column
    pruning, so pulling one indicator out of years-of-schooling transfers 37kB
    of a 671kB file instead of the whole 530kB zip. (That benefit is
    DuckDB-specific -- pandas' `read_parquet` goes through fsspec, which fetches
    the entire object and prunes locally, so a pandas user gains nothing but
    loses nothing either.)

    `repack_frame` is doing real work here, not just saving bytes. Written
    naively the file is 1.03MB; repacked it is 671kB, because it picks Float32
    for the indicator columns, category for Entity/Code and UInt16 for Year.
    Using it also means these files carry the same dtype conventions as
    everything else we publish to the catalog, rather than a rule invented here.

    One caveat worth knowing: `repack_series` accepts a Float32 downcast on
    `np.allclose(rtol=1e-5)` rather than exact equality. It tries `to_int`
    first, so whole-number indicators (populations, counts) stay exact as
    integers; a large-magnitude *non-integer* column is the case that loses
    absolute precision. That is the same trade every other repacked catalog
    table already makes.
    """
    # One concat rather than N column assignments, for the reason given where the
    # CSV frame is built.
    df = pd.concat(
        [keys] + [pd.Series(wide[catalog_path].values, name=long_name) for catalog_path, long_name, _, _ in columns],
        axis=1,
    )
    pq.write_table(
        pa.Table.from_pandas(repack_frame(df), preserve_index=False),
        path,
        compression="zstd",
    )


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


# Where to publish the package, split by environment exactly like
# BAKED_VARIABLES_PATH/DATA_API_URL (etl/config.py) already split the baked
# indicator JSONs -- same bucket pair, so every staging branch gets its own
# isolated path under api-staging.owid.io instead of every automated staging
# build overwriting the one production file at a fixed path (the
# owid-public/owid_co2.py-style scripts this originally copied don't need that
# isolation because they're only ever run manually against production).
def _download_package_location(filename: str) -> tuple[str, str]:
    """Returns (s3_url, public_url) for the package's zip."""
    if config.DATA_API_ENV == "production":
        return (
            f"s3://owid-api/v1/mdim-downloads/{filename}",
            f"https://api.ourworldindata.org/v1/mdim-downloads/{filename}",
        )
    return (
        f"s3://owid-api-staging/{config.DATA_API_ENV}/v1/mdim-downloads/{filename}",
        f"https://api-staging.owid.io/{config.DATA_API_ENV}/v1/mdim-downloads/{filename}",
    )


# Fixed timestamp for every zip entry, so rebuilding an unchanged package
# produces byte-identical output instead of a new object on every ETL run.
# 1980-01-01 is the earliest date the zip format can represent.
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)


def _readme(title: str, page_url: str, column_sections: list[str]) -> str:
    """Ported from `constructReadme`'s multi-column branch (readmeTools.ts),
    with the tolerance-column paragraph dropped (a complete-dataset package has
    no tolerance columns) and three MDIM-only additions that have no counterpart
    there: that the package covers every dimension combination, that column
    headers shouldn't be parsed, and how to read metadata.json's "dimensions"
    and per-column "dimensions"/"url" keys instead.

    !!! KEEP IN SYNC WITH owid-grapher's readmeTools.ts !!!
    """
    return f"""# {title} - Data package

This data package contains the data that powers the chart ["{title}"]({page_url}) on the Our World in Data website. It includes every dimension combination of this multidimensional dataset -- all metric/breakdown choices, not just the view selected on the chart.

## CSV Structure

The high level structure of the CSV file is that each row is an observation for an entity (usually a country or region) and a timepoint (usually a year).

The first two columns in the CSV file are "Entity" and "Code". "Entity" is the name of the entity (e.g. "United States"). "Code" is the OWID internal entity code that we use if the entity is a country or region. For most countries, this is the same as the [iso alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) code of the entity (e.g. "USA") - for non-standard countries like historical countries these are custom codes.

The third column is either "Year" or "Day". If the data is annual, this is "Year" and contains only the year as an integer. If the column is "Day", the column contains a date string in the form "YYYY-MM-DD".

The remaining columns are the data columns, each of which is a time series corresponding to one dimension combination of this dataset. Their headers are human-readable names; don't parse them to work out which dimension combination a column belongs to, use the .metadata.json file described below instead.

## Metadata.json structure

The .metadata.json file contains metadata about the data package. The "chart" key contains information to recreate the chart, like the title, subtitle etc.. The "columns" key contains information about each of the columns in the csv, like the unit, timespan covered, citation for the data etc..

The "dimensions" key lists this dataset's dimensions and the choices available for each, as a stable slug plus a display name. Every column entry then carries its own "dimensions" key naming the combination of choices that column belongs to, by slug, plus a "url" that opens that combination on our website. Together those let you go from a CSV column to its dimension choices, and back, without parsing column headers.

A few columns belong to more than one combination, which happens when a dimension makes no difference to that particular indicator. Those carry an extra "otherViews" key listing the remaining combinations in the same shape; if you need every combination a column appears under, read both keys.

## About the data

Our World in Data is almost never the original producer of the data - almost all of the data we use has been compiled by others. If you want to re-use data, it is your responsibility to ensure that you adhere to the sources' license and to credit them correctly. Please note that a single time series may have more than one source - e.g. when we stich together data from different time periods by different producers or when we calculate per capita metrics using population data from a second source.

### How we process data at Our World In Data
All data and visualizations on Our World in Data rely on data sourced from one or several original data providers. Preparing this original data involves several processing steps. Depending on the data, this can include standardizing country names and world region definitions, converting units, calculating derived indicators such as per capita measures, as well as adding or adapting metadata such as the name or the description given to an indicator.
[Read about our data pipeline](https://docs.owid.io/projects/etl/)

## Detailed information about each time series

{chr(10).join(column_sections)}
"""


@dataclass
class DownloadPackageResult:
    url: str
    parquet_url: str
    metadata_url: str
    row_count: int
    indicator_count: int
    size_bytes: int

    def to_config(self) -> dict:
        """Shape matching MultiDimDataPageConfig.downloadPackage on the grapher
        side. Every URL points straight at an R2 object -- there is no grapher
        route in front of them, so nothing has to be computed at render time.

        `url` is the zip behind the download button; `parquet_url` and
        `metadata_url` are the same data as separate objects, for the Data API
        section. They're separate keys rather than one derived from another
        because the grapher side should never be constructing R2 paths.
        """
        return {
            "url": self.url,
            "parquetUrl": self.parquet_url,
            "metadataUrl": self.metadata_url,
            "indicatorCount": self.indicator_count,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
        }


def build_download_package_for_collection(
    collection: Collection,
    dest_dir: Path,
    build_date: date | None = None,
    max_size_bytes: int = MAX_PACKAGE_SIZE_BYTES,
) -> DownloadPackageResult:
    """Build the complete-dataset zip and publish it to R2.

    Requires `collection.save()` to have run first: indicator catalog paths
    need to be fully expanded, the indicators need to exist in the DB so their
    variable IDs and published metadata can be resolved, and the MDIM needs a
    page slug.

    Raises `DownloadPackageTooLargeError` if the result exceeds
    `max_size_bytes`; raise it per collection only for the reasons in
    `_check_package_size`.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    build_date = build_date or pd.Timestamp.now(tz=timezone.utc).date()

    page_slug = resolve_page_slug(collection)
    if page_slug is None:
        raise ValueError(
            f"No published MDIM found for {collection.catalog_path!r} -- call collection.save() "
            "before building the download package, and check the MDIM has been published (it needs "
            "a slug). `Collection.save()` skips unpublished collections rather than calling this."
        )
    page_url = f"https://ourworldindata.org/grapher/{page_slug}"

    wide, column_to_dimensions = build_wide_table_for_collection(collection)

    catalog_paths = list(column_to_dimensions)
    variable_ids = resolve_variable_ids(catalog_paths)
    missing = [p for p in catalog_paths if p not in variable_ids]
    if missing:
        log.warning("download_package.variable_id_missing", catalog_paths=missing)

    resolved = [(path, variable_ids[path]) for path in catalog_paths if path in variable_ids]
    metadata_by_id = _fetch_indicator_metadata([variable_id for _, variable_id in resolved])

    # One name per column -- the CSV header, the Parquet field name and
    # metadata.json's column key, so none of the three can drift from the
    # others, and all three need it to be unique. The wide table itself is keyed
    # by catalog path; this is the name a reader sees.
    columns: list[tuple[str, str, int, IndicatorColumn]] = []
    for catalog_path, variable_id in resolved:
        col = IndicatorColumn(metadata_by_id[variable_id])
        columns.append((catalog_path, _long_column_name(col), variable_id, col))
    _check_column_names_unique(page_slug, columns)

    dimension_definitions = _dimension_definitions(collection)

    #
    # metadata.json + readme.md
    #
    title = collection.title.get("title")
    metadata_columns = {}
    readme_sections = []
    attributions = set()
    for catalog_path, long_name, variable_id, col in columns:
        attributions.add(get_attribution(col))
        metadata_columns[long_name] = {
            # MDIM-only. A single-chart download has no dimension structure, so
            # these keys have no counterpart in owid-grapher's assembleMetadata
            # -- an intended divergence from that format, not drift. Emitted
            # first so they're the first thing visible under a column key, above
            # the long description fields.
            **_view_fields(column_to_dimensions[catalog_path], page_url),
            **metadata_column_entry(
                col,
                variable_id,
                f"{config.DATA_API_URL}/{variable_id}.metadata.json",
                build_date,
            ),
        }
        readme_sections.append("\n".join(column_readme_text(col, build_date, heading=long_name)))

    metadata_json = dumps_like_json_stringify(
        {
            "chart": {
                "title": title,
                "citation": "; ".join(sorted(attributions)),
                "originalChartUrl": page_url,
                "selection": collection.default_selection or [],
            },
            "dimensions": dimension_definitions,
            "columns": metadata_columns,
            # Same key as a single-chart download for format parity, but here
            # it's necessarily the date the package was built, not downloaded.
            "dateDownloaded": build_date.isoformat(),
            "activeFilters": {},
        }
    )
    readme = _readme(title, page_url, readme_sections)

    #
    # The CSV, in its final downloadable shape: Entity/Code/Year, long
    # display-name headers, JS-style number formatting.
    #
    time_col = _time_column(wide)
    time_header = "Day" if time_col == "date" else "Year"
    entity_codes = _resolve_entity_codes(wide["country"].unique().tolist())

    keys = pd.DataFrame(
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
    # Concatenated in one go rather than assigned column by column: each
    # assignment into a DataFrame copies its block layout, so inserting N
    # columns one at a time is quadratic. poverty_pip has 306 of them, which is
    # enough for pandas to start warning about a fragmented frame.
    final = pd.concat(
        [keys]
        + [_format_numeric_series(wide[catalog_path]).rename(long_name) for catalog_path, long_name, _, _ in columns],
        axis=1,
    )

    csv_path = dest_dir / f"{page_slug}.csv"
    final.to_csv(csv_path, index=False)

    #
    # The same table as Parquet, for programmatic consumers. Built from the
    # unformatted values on purpose: `_format_numeric_series` exists to make
    # numbers print like grapher's CSV writer does, and stringly-formatted
    # numbers are the one thing a typed columnar format should not carry.
    #
    parquet_path = dest_dir / f"{page_slug}.parquet"
    _write_parquet(keys, wide, columns, parquet_path)

    #
    # Zip it. Entry order and names mirror a single-chart download.
    #
    zip_name = f"{page_slug}.complete-dataset.zip"
    zip_path = dest_dir / zip_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for name, data in [
            (f"{page_slug}.metadata.json", metadata_json.encode()),
            (f"{page_slug}.csv", csv_path.read_bytes()),
            ("readme.md", readme.encode()),
        ]:
            info = zipfile.ZipInfo(name, date_time=_ZIP_EPOCH)
            # A ZipInfo carries its own compress_type, defaulting to ZIP_STORED,
            # and it silently wins over the ZipFile's -- so this has to be set
            # explicitly or the whole package ships uncompressed.
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, data)

    size_bytes = zip_path.stat().st_size
    # Checked before the upload, so an oversized package never reaches R2.
    _check_package_size(
        page_slug=page_slug,
        zip_bytes=size_bytes,
        csv_bytes=csv_path.stat().st_size,
        max_size_bytes=max_size_bytes,
    )

    s3_url, public_url = _download_package_location(zip_name)
    # downloadable=True sets Content-Disposition, so clicking the R2 link saves
    # the file under its real name instead of opening it. content_type matters
    # for Cloudflare's edge -- without it R2 serves the object with no
    # Content-Type at all.
    s3_utils.upload(s3_url, zip_path, public=True, downloadable=True, content_type="application/zip")

    # The Parquet and metadata JSON go up as their own objects, for the Data API
    # section rather than the download button. No downloadable=True on these:
    # Content-Disposition would tell the browser to save a file, and the point
    # of the Parquet is to be read in place by a query.
    metadata_path = dest_dir / f"{page_slug}.metadata.json"
    metadata_path.write_text(metadata_json)
    parquet_s3, parquet_url = _download_package_location(parquet_path.name)
    metadata_s3, metadata_url = _download_package_location(metadata_path.name)
    s3_utils.upload(parquet_s3, parquet_path, public=True, content_type="application/vnd.apache.parquet")
    s3_utils.upload(metadata_s3, metadata_path, public=True, content_type="application/json")

    log.info(
        "download_package.published",
        rows=len(wide),
        indicators=len(columns),
        csv_bytes=csv_path.stat().st_size,
        zip_bytes=size_bytes,
        parquet_bytes=parquet_path.stat().st_size,
        url=public_url,
    )

    return DownloadPackageResult(
        url=public_url,
        parquet_url=parquet_url,
        metadata_url=metadata_url,
        row_count=len(wide),
        indicator_count=len(columns),
        size_bytes=size_bytes,
    )
