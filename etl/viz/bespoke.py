"""Framework support for `viz://bespoke` steps.

A bespoke step turns garden tables into the JSON feed that one of owid-grapher's
`bespoke/projects/` bundles fetches in the browser. The step itself only writes files into
its own output folder (`data/viz/bespoke/<ns>/<version>/<name>/`); this module ships them,
and derives the provenance block that used to be hand-typed in each step.

Two things it gives a feed that a per-step `s3_utils.upload()` call could not:

* **One environment-dependent location.** The feed goes to `v1/bespoke/<ns>/<version>/<name>/`
  under `s3://owid-api` on production and under `s3://owid-api-staging/<env>` everywhere else --
  the same split `BAKED_VARIABLES_PATH` / `DATA_API_URL` (`etl/config.py`) already apply to the
  baked indicator JSONs and `download_package.py` applies to MDIM download packages. A staging
  server therefore builds its own feed and an article preview can show a data change before it
  is merged, instead of every run overwriting the one production copy.

  Nothing has to seed a new staging environment: the `owid-api-staging` worker falls back to
  the production bucket for any key missing under the environment's prefix (see
  `workers/owid-api-staging/src/index.ts` in owid/cloudflare-workers), so a staging server that
  never ran the step serves production's feed.

* **Metadata derived from the garden data** (`build_feed_metadata`), so the source line a
  reader sees under a bespoke viz is the dataset's own origins rather than a string typed into
  the step that goes stale the next time the dataset is updated.
"""

from __future__ import annotations

import concurrent.futures
import mimetypes
from dataclasses import dataclass
from datetime import date, timezone
from pathlib import Path

import humps
import pandas as pd
from owid.catalog import Variable, s3_utils
from structlog import get_logger

from etl import config
from etl.viz.chart.download_package_format import (
    IndicatorColumn,
    dumps_like_json_stringify,
    format_attributions,
    get_attribution,
    metadata_column_entry,
)

log = get_logger()

# The dataset index that `ExportStep.run` writes next to the feed. It records the step's
# checksum for etl's own change detection and means nothing to a browser.
INDEX_FILE = "index.json"

# The file `build_feed_metadata` is written to, and the name the bundles fetch.
METADATA_FILENAME = "metadata.json"


@dataclass(frozen=True)
class FeedLocation:
    """Where a feed's files live once published."""

    s3_folder: str
    public_url: str


def feed_location(step_path: str) -> FeedLocation:
    """Where the feed of `viz://bespoke/<ns>/<version>/<name>` is published.

    `step_path` is the step's path without its scheme, i.e. `bespoke/<ns>/<version>/<name>`;
    the `bespoke/` channel segment is dropped because the root already names it.
    """
    feed_path = step_path.lstrip("/").removeprefix("bespoke/")
    if config.DATA_API_ENV == "production":
        return FeedLocation(
            s3_folder=f"s3://owid-api/v1/bespoke/{feed_path}",
            public_url=f"https://api.ourworldindata.org/v1/bespoke/{feed_path}",
        )
    return FeedLocation(
        s3_folder=f"s3://owid-api-staging/{config.DATA_API_ENV}/v1/bespoke/{feed_path}",
        public_url=f"https://api-staging.owid.io/{config.DATA_API_ENV}/v1/bespoke/{feed_path}",
    )


def sync_feed(step_path: str, local_dir: Path, max_workers: int = 20) -> FeedLocation:
    """Upload every file in `local_dir` to the step's feed folder, and delete what it no longer has.

    Stale objects are deleted because a feed is a set of files, not a list: a run that drops an
    entity (`causes-of-death.<entityId>.json`) or renumbers one has to take the old file with it,
    or the bundle keeps fetching data that nothing produces any more.
    """
    location = feed_location(step_path)
    files = sorted(p for p in local_dir.glob("**/*") if p.is_file() and p.name != INDEX_FILE)
    # A step that wrote nothing is a bug in the step; deleting the published feed on the strength
    # of it would turn that bug into an outage.
    assert files, f"{step_path} produced no files to publish in {local_dir}"

    client = s3_utils.connect_r2()
    bucket, prefix = s3_utils.s3_bucket_key(location.s3_folder)
    keys = {f"{prefix}/{path.relative_to(local_dir).as_posix()}": path for path in files}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                s3_utils.upload,
                f"s3://{bucket}/{key}",
                path,
                public=True,
                quiet=True,
                content_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
            )
            for key, path in keys.items()
        ]
        for future in concurrent.futures.as_completed(futures):
            # Raise the first failure rather than reporting a feed as published when part of it isn't.
            future.result()

    stale = [key for key in s3_utils.list_s3_objects(f"{location.s3_folder}/", client=client) if key not in keys]
    if stale:
        # delete_objects takes at most 1000 keys per call.
        for i in range(0, len(stale), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in stale[i : i + 1000]]})

    log.info(
        "bespoke.published",
        step=step_path,
        files=len(keys),
        deleted=len(stale),
        url=location.public_url,
    )
    return location


#
# Derived metadata
#
# The feed's provenance, built from the garden columns it is made of, in the shape the MDIM
# download package already publishes (`download_package.py`): a top-level block with a title and
# a citation, and one entry per column. The formatting is grapher's own, via the port in
# `download_package_format.py`, so a source line under a bespoke viz reads like a source line
# under a chart.
#


def build_feed_metadata(
    title: str,
    columns: dict[str, Variable],
    update_period_days: int | None = None,
    build_date: date | None = None,
) -> dict:
    """Build the feed's `metadata.json` content from the columns it is built from.

    `columns` maps the name a reader sees to the garden column carrying the metadata, e.g.
    `{"Deaths": tb["value"]}` -- the same role the long display name plays in an MDIM download
    package. Most bespoke feeds are built from a single long table, so that is usually one entry;
    the mapping exists for the feeds that combine several. The key also stands in as the title
    when the column's own title is a Jinja template a garden table can't render.

    `update_period_days` comes from the garden dataset (`ds.metadata.update_period_days`) and is
    only used to derive `nextUpdate`.
    """
    build_date = build_date or pd.Timestamp.now(tz=timezone.utc).date()

    entries = {}
    attributions = []
    for name, variable in columns.items():
        col = IndicatorColumn(
            variable_meta_to_api_dict(variable, update_period_days=update_period_days, default_title=name)
        )
        attributions.append(get_attribution(col))
        # No variable id and no `fullMetadata` URL: a bespoke feed is built from garden tables,
        # whose columns have no variable in the grapher DB to point at. Both keys drop out.
        entries[name] = metadata_column_entry(col, None, None, build_date)

    return {
        "feed": {
            "title": title,
            # The "Source: ..." line, as grapher composes it for a chart built on these columns.
            "citation": format_attributions(_uniq(attributions)),
        },
        "columns": entries,
        "dateGenerated": build_date.isoformat(),
    }


def write_feed_metadata(dest_dir: Path, metadata: dict, filename: str = METADATA_FILENAME) -> Path:
    """Write `build_feed_metadata`'s output into the step's output folder."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / filename
    path.write_text(dumps_like_json_stringify(metadata))
    return path


# Markers of a metadata value that is still a Jinja template. The templates in a `.meta.yml` are
# rendered per dimension combination when a grapher step builds its wide tables, so the single
# long column a bespoke feed reads still carries the template text (`gbd_treemap`'s title is
# `<% if metric == ... %>`). Shipping that as an indicator title would be worse than shipping
# nothing, so those fields are dropped -- with a warning, since a feed that wanted the title has
# to get it from somewhere else.
JINJA_MARKERS = ("<%", "<<")

# Presentation fields that exist only inside grapher and have no place in a feed's metadata.
PRESENTATION_FIELDS_TO_DROP = ("topic_tags", "faqs", "grapher_config")


def variable_meta_to_api_dict(
    variable: Variable, update_period_days: int | None = None, default_title: str | None = None
) -> dict:
    """A garden column's `VariableMeta` in the shape of a published indicator's `<id>.metadata.json`.

    That shape is what `download_package_format.py` is a port against -- it reads camelCased keys
    off the JSON the Data API serves -- so a column coming straight out of a garden table has to
    be converted before the same formatting code can run on it.

    `default_title` is used when the column has no usable title of its own, which is the normal
    case for a long garden table: an empty title would otherwise reach a reader as `"" [dataset]`
    in the middle of the full citation.
    """
    meta = variable.metadata.to_dict()
    for field in PRESENTATION_FIELDS_TO_DROP:
        (meta.get("presentation") or {}).pop(field, None)

    api = humps.camelize(meta)
    # The DB column, and therefore the API field, for `VariableMeta.title` is `name`.
    api["name"] = api.pop("title", None)
    api["shortName"] = variable.name
    if update_period_days is not None:
        api["updatePeriodDays"] = update_period_days
    if api.get("type") is None:
        # Garden columns rarely set `type` -- a grapher step infers it at upsert time, from the
        # stringified values it writes to the DB. Same call here, so a feed says what the DB would.
        # Imported here to keep the framework's publish path off the grapher/sqlalchemy import chain.
        from etl.grapher.model import Variable as GrapherVariable

        api["type"] = GrapherVariable.infer_type(variable.dropna().astype(str))

    dropped, api = _drop_unrendered_templates(api)
    if api.get("name") is None and default_title is not None:
        api["name"] = default_title
    if dropped:
        # A warning, not an error: origins, unit and license -- everything the citation is made
        # of -- are never templated, so the feed's provenance is complete even when its titles
        # are unusable.
        log.warning("bespoke.metadata_template_not_rendered", column=variable.name, fields=sorted(dropped))
    return api


def _drop_unrendered_templates(value: dict, path: str = "") -> tuple[list[str], dict]:
    """Drop every string field that is still a Jinja template, and report which ones went."""
    dropped = []
    kept = {}
    for key, item in value.items():
        name = f"{path}{key}"
        if isinstance(item, str) and any(marker in item for marker in JINJA_MARKERS):
            dropped.append(name)
        elif isinstance(item, dict):
            nested_dropped, nested = _drop_unrendered_templates(item, path=f"{name}.")
            dropped += nested_dropped
            kept[key] = nested
        elif item is not None:
            kept[key] = item
    return dropped, kept


def _uniq(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


__all__ = [
    "FeedLocation",
    "build_feed_metadata",
    "feed_location",
    "sync_feed",
    "variable_meta_to_api_dict",
    "write_feed_metadata",
]
