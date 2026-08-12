"""Rewrite `descriptionKey` in published R2 indicator metadata from a list to a markdown string.

`descriptionKey` became a free-form markdown string in #6438, and the grapher DB was migrated
with it. The per-indicator metadata JSON on R2 was not: those files are only rewritten when a
variable is upserted, so every dataset that has not been re-run since the migration still serves
a legacy array. owid-grapher tolerates both shapes by normalizing at each ingress point
(`normalizeDescriptionKey`), and those shims can only be removed once no published file holds an
array.

This command closes that gap without re-running any ETL step. For each variable it reads the
published metadata file and, if `descriptionKey` is a list, rewrites that one field with
`description_key_to_string` — the same conversion owid-grapher applies at runtime today, so the
rendered text is unchanged by construction. Every other field is left byte-identical, including
`metadataChecksum` and `updatedAt`, so the file stays exactly what the last ETL run produced
apart from the shape of this one field.

Dry run by default; pass --execute to write.
"""

import concurrent.futures
import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any

import click
import structlog
from owid.catalog import s3_utils
from owid.catalog.core.meta import description_key_to_string

from etl import config
from etl.http import session

log = structlog.get_logger()


def _metadata_url(variable_id: int) -> str:
    return f"{config.DATA_API_URL}/{variable_id}.metadata.json"


def _s3_path(variable_id: int) -> str:
    return f"{config.BAKED_VARIABLES_PATH}/{variable_id}.metadata.json"


def _candidate_ids(cutoff: str | None, limit: int | None) -> list[int]:
    """Variable ids to inspect, newest dataset first so a partial run covers the visible ones.

    The cutoff is only a way to skip variables whose file is already known to be fresh — the
    file itself is always what decides whether a rewrite happens.
    """
    where = "v.descriptionKey IS NOT NULL AND v.descriptionKey != ''"
    if cutoff:
        where += f" AND d.dataEditedAt < '{cutoff}'"
    sql = f"""
        SELECT v.id FROM variables v
        JOIN datasets d ON d.id = v.datasetId
        WHERE {where}
        ORDER BY d.dataEditedAt DESC
    """
    if limit:
        sql += f" LIMIT {limit}"
    return config.OWID_ENV.read_sql(sql)["id"].tolist()


def _rewrite_one(variable_id: int, execute: bool) -> str:
    """Return the outcome for one variable: rewritten, already-string, missing, or failed."""
    try:
        response = session.get(_metadata_url(variable_id), timeout=60)
        if response.status_code == 404:
            return "missing"
        response.raise_for_status()
        metadata: dict[str, Any] = response.json()
    except Exception as e:
        log.error("republish.fetch_failed", variable_id=variable_id, error=str(e))
        return "failed"

    description_key = metadata.get("descriptionKey")
    if not isinstance(description_key, list):
        return "already-string"

    converted = description_key_to_string(description_key)
    if converted is None:
        # An array of empty strings carries no content; drop the field rather than write null,
        # which is what `description_key_to_string` means by None.
        metadata.pop("descriptionKey")
    else:
        metadata["descriptionKey"] = converted

    if not execute:
        return "would-rewrite"

    try:
        body = gzip.compress(json.dumps(metadata, default=str).encode())
        bucket, key = s3_utils.s3_bucket_key(_s3_path(variable_id))
        s3_utils.connect_r2_cached().put_object(  # ty: ignore[unresolved-attribute]
            Bucket=bucket,
            Body=body,
            Key=key,
            ContentEncoding="gzip",
            ContentType="application/json",
        )
    except Exception as e:
        log.error("republish.upload_failed", variable_id=variable_id, error=str(e))
        return "failed"

    return "rewritten"


@click.command(name="republish-description-key", help=__doc__)
@click.option("--execute", is_flag=True, help="Write to R2. Without this the command only reports.")
@click.option(
    "--cutoff",
    default=None,
    help="Only inspect variables whose dataset was last edited before this date (e.g. 2026-07-21). "
    "Skips files already known to be fresh; omit to inspect every variable.",
)
@click.option("--limit", type=int, default=None, help="Only inspect this many variables.")
@click.option("--workers", type=int, default=16, show_default=True, help="Concurrent requests.")
@click.option(
    "--ids-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Read variable ids from a file (one per line) instead of querying the DB.",
)
@click.option(
    "--state-file",
    type=click.Path(path_type=Path),
    default=None,
    help="Append processed ids here and skip them on a later run, so an interrupted run resumes.",
)
def cli(
    execute: bool,
    cutoff: str | None,
    limit: int | None,
    workers: int,
    ids_file: Path | None,
    state_file: Path | None,
) -> None:
    target = "PRODUCTION" if config.DATA_API_ENV == "production" else config.DATA_API_ENV
    log.info("republish.start", target=target, path=config.BAKED_VARIABLES_PATH, execute=execute)

    if ids_file:
        variable_ids = [int(line) for line in ids_file.read_text().split() if line.strip()]
    else:
        variable_ids = _candidate_ids(cutoff, limit)

    done: set[int] = set()
    if state_file and state_file.exists():
        done = {int(line) for line in state_file.read_text().split() if line.strip()}
        variable_ids = [i for i in variable_ids if i not in done]
        log.info("republish.resuming", already_done=len(done))

    log.info("republish.candidates", count=len(variable_ids))
    if not variable_ids:
        return

    outcomes: Counter[str] = Counter()
    handle = state_file.open("a") if (state_file and execute) else None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_rewrite_one, i, execute): i for i in variable_ids}
            for n, future in enumerate(concurrent.futures.as_completed(futures), 1):
                variable_id = futures[future]
                outcome = future.result()
                outcomes[outcome] += 1
                if handle and outcome in ("rewritten", "already-string", "missing"):
                    handle.write(f"{variable_id}\n")
                if n % 500 == 0:
                    handle and handle.flush()
                    log.info("republish.progress", done=n, total=len(variable_ids), **outcomes)
    finally:
        if handle:
            handle.close()

    log.info("republish.done", **outcomes)
    if not execute and outcomes["would-rewrite"]:
        log.info("republish.dry_run", message=f"pass --execute to rewrite {outcomes['would-rewrite']} files")
    if outcomes["failed"]:
        raise click.ClickException(f"{outcomes['failed']} variables failed — rerun to retry them")


if __name__ == "__main__":
    cli()
