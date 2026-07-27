"""Build the "complete dataset" download package for a Collection (MDIM/Explorer).

Prototype for the mdim-downloads project (see owid-projects/mdim-downloads).

The whole package -- wide CSV + metadata.json + readme.md, zipped -- is built
here, once, at ETL publish time, and uploaded to R2. The grapher side does
nothing but link to it.

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
    so both the ceiling and the fallback design are gone.
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

import pandas as pd
from owid.catalog import Dataset as CatalogDataset
from owid.catalog import Table, s3_utils
from owid.catalog import processing as pr
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
    look each indicator's metadata up by variable ID."""
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


def _resolve_page_slug(collection: Collection) -> str:
    """The MDIM's public page slug (e.g. "years-of-schooling").

    Not derivable from the catalog path -- grapher owns the mapping, and the
    slug is hyphenated where the catalog short name is underscored. Requires
    `collection.save()` to have run first, which is what creates the row.
    It matters here for more than the R2 filename: the readme and
    metadata.json both link to the real page URL.
    """
    from sqlalchemy.orm import Session

    from etl.config import OWID_ENV
    from etl.grapher.model import MultiDimDataPage

    with Session(OWID_ENV.engine) as session:
        mdim = MultiDimDataPage.load_mdim(session, catalogPath=collection.catalog_path)
    if mdim is None or not mdim.slug:
        raise ValueError(
            f"No published MDIM found for {collection.catalog_path!r} -- call collection.save() "
            "before building the download package."
        )
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


def _long_column_name(col: IndicatorColumn) -> str:
    """The CSV header, and metadata.json's key for that column.

    `get_title()` (grapher's `getTitle`) plus a display-name disambiguator: an
    MDIM's wide table can hold several indicators that share a public title and
    differ only in their per-view display name, which would otherwise collide
    into one CSV column.
    """
    title = get_title(col)
    display_name = col.display.get("name")
    title_public = col.title_public_or_display_name["title"]
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
    with one sentence added to say the package covers every dimension
    combination, and the tolerance-column paragraph dropped (a complete-dataset
    package has no tolerance columns).

    !!! KEEP IN SYNC WITH owid-grapher's readmeTools.ts !!!
    """
    return f"""# {title} - Data package

This data package contains the data that powers the chart ["{title}"]({page_url}) on the Our World in Data website. It includes every dimension combination of this multidimensional dataset -- all metric/breakdown choices, not just the view selected on the chart.

## CSV Structure

The high level structure of the CSV file is that each row is an observation for an entity (usually a country or region) and a timepoint (usually a year).

The first two columns in the CSV file are "Entity" and "Code". "Entity" is the name of the entity (e.g. "United States"). "Code" is the OWID internal entity code that we use if the entity is a country or region. For most countries, this is the same as the [iso alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) code of the entity (e.g. "USA") - for non-standard countries like historical countries these are custom codes.

The third column is either "Year" or "Day". If the data is annual, this is "Year" and contains only the year as an integer. If the column is "Day", the column contains a date string in the form "YYYY-MM-DD".

The remaining columns are the data columns, each of which is a time series corresponding to one dimension combination of this dataset.

## Metadata.json structure

The .metadata.json file contains metadata about the data package. The "chart" key contains information to recreate the chart, like the title, subtitle etc.. The "columns" key contains information about each of the columns in the csv, like the unit, timespan covered, citation for the data etc..

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
    row_count: int
    indicator_count: int
    size_bytes: int

    def to_config(self) -> dict:
        """Shape matching MultiDimDataPageConfig.downloadPackage on the grapher
        side. `url` points straight at the R2 object -- there is no grapher
        route in front of it, so nothing has to be computed at render time."""
        return {
            "url": self.url,
            "indicatorCount": self.indicator_count,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
        }


def build_download_package_for_collection(
    collection: Collection,
    dest_dir: Path,
    build_date: date | None = None,
) -> DownloadPackageResult:
    """Build the complete-dataset zip and publish it to R2.

    Requires `collection.save()` to have run first: indicator catalog paths
    need to be fully expanded, the indicators need to exist in the DB so their
    variable IDs and published metadata can be resolved, and the MDIM needs a
    page slug.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    build_date = build_date or pd.Timestamp.now(tz=timezone.utc).date()

    page_slug = _resolve_page_slug(collection)
    page_url = f"https://ourworldindata.org/grapher/{page_slug}"

    wide, column_to_catalog_path = build_wide_table_for_collection(collection)

    variable_ids = resolve_variable_ids(list(column_to_catalog_path.values()))
    missing = [p for p in column_to_catalog_path.values() if p not in variable_ids]
    if missing:
        log.warning("download_package.variable_id_missing", catalog_paths=missing)

    resolved = [
        (wide_name, variable_ids[catalog_path])
        for wide_name, catalog_path in column_to_catalog_path.items()
        if catalog_path in variable_ids
    ]
    metadata_by_id = _fetch_indicator_metadata([variable_id for _, variable_id in resolved])

    # Long display name per wide-table column -- becomes both the CSV header
    # and metadata.json's column key, so the two can't drift from each other.
    columns: list[tuple[str, str, int, IndicatorColumn]] = []
    seen_long_names: dict[str, str] = {}
    for wide_name, variable_id in resolved:
        col = IndicatorColumn(metadata_by_id[variable_id])
        long_name = _long_column_name(col)
        existing = seen_long_names.get(long_name)
        if existing and existing != wide_name:
            log.warning(
                "download_package.column_name_collision",
                long_name=long_name,
                columns=[existing, wide_name],
            )
        seen_long_names[long_name] = wide_name
        columns.append((wide_name, long_name, variable_id, col))

    #
    # metadata.json + readme.md
    #
    title = collection.title.get("title")
    metadata_columns = {}
    readme_sections = []
    attributions = set()
    for _wide_name, long_name, variable_id, col in columns:
        attributions.add(get_attribution(col))
        metadata_columns[long_name] = metadata_column_entry(
            col,
            variable_id,
            f"{config.DATA_API_URL}/{variable_id}.metadata.json",
            build_date,
        )
        readme_sections.append("\n".join(column_readme_text(col, build_date)))

    metadata_json = dumps_like_json_stringify(
        {
            "chart": {
                "title": title,
                "citation": "; ".join(sorted(attributions)),
                "originalChartUrl": page_url,
                "selection": collection.default_selection or [],
            },
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
    for wide_name, long_name, _variable_id, _col in columns:
        final[long_name] = _format_numeric_series(wide[wide_name])

    csv_path = dest_dir / f"{page_slug}.csv"
    final.to_csv(csv_path, index=False)

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

    s3_url, public_url = _download_package_location(zip_name)
    # downloadable=True sets Content-Disposition, so clicking the R2 link saves
    # the file under its real name instead of opening it. content_type matters
    # for Cloudflare's edge -- without it R2 serves the object with no
    # Content-Type at all.
    s3_utils.upload(s3_url, zip_path, public=True, downloadable=True, content_type="application/zip")

    log.info(
        "download_package.published",
        rows=len(wide),
        indicators=len(columns),
        csv_bytes=csv_path.stat().st_size,
        zip_bytes=size_bytes,
        url=public_url,
    )

    return DownloadPackageResult(
        url=public_url,
        row_count=len(wide),
        indicator_count=len(columns),
        size_bytes=size_bytes,
    )
