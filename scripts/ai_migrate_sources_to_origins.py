#!/usr/bin/env python3
"""AI-assisted Source -> Origin migration for snapshot DVC files.

For each snapshot DVC referenced from `dag/migrated.yml`, we ask Claude to
derive a clean `origin:` block from the legacy `meta.source` block
(recovered from `origin/master` when available), following OWID's
documented Origin style guidance. The naive mapping that PR #5978
introduced is replaced with the AI output.

Smoke test on a single file:

    .venv/bin/python scripts/ai_migrate_sources_to_origins.py \\
      -p snapshots/trade/2018-03-19/current_gdp__british_pounds__fouquin_and_hugot__cepii_2016.feather.dvc \\
      --verbose

Apply on all migrated snapshots:

    .venv/bin/python scripts/ai_migrate_sources_to_origins.py --apply
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anthropic
import click
from owid.catalog.core.meta import License, Origin
from rich.console import Console
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import LiteralScalarString

from etl.config import ANTHROPIC_API_KEY
from etl.files import ruamel_dump, ruamel_load
from etl.snapshot import SnapshotMeta

console = Console()

DAG_PATH = Path("dag/migrated.yml")
SNAPSHOTS_DIR = Path("snapshots")
DEFAULT_REPORT_PATH = Path("ai/source_origin_ai_migration.tsv")
DEFAULT_MODEL = "claude-sonnet-4-6"

# USD per 1M tokens. Best-effort table; we fall back to Sonnet pricing.
MODEL_PRICING_PER_MTOK: dict[str, tuple[float, float]] = {
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-sonnet-4-5-20250929": (3.0, 15.0),
    "claude-opus-4-7": (15.0, 75.0),
    "claude-opus-4": (15.0, 75.0),
    "claude-opus-4-20250514": (15.0, 75.0),
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-haiku-4-5-20251001": (1.0, 5.0),
}


def estimate_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    in_rate, out_rate = MODEL_PRICING_PER_MTOK.get(model, (3.0, 15.0))
    return input_tokens / 1_000_000 * in_rate + output_tokens / 1_000_000 * out_rate

SNAPSHOT_URI_RE = re.compile(r"(?m)^\s*(?:-\s*)?(snapshot://[^\s:#]+)")
YEAR_RE = re.compile(r"\b(1[5-9]\d{2}|20\d{2})\b")
DATE_PUBLISHED_RE = re.compile(r"^(\d{4}|\d{4}-\d{2}-\d{2}|latest)$")

# Grapher MySQL column lengths.
DB_LIMITS = {
    "producer": 255,
    "title": 512,
    "attribution_short": 512,
    "version_producer": 255,
}

# Field order for the rewritten origin block (omits null fields).
ORIGIN_FIELD_ORDER = [
    "producer",
    "title",
    "description",
    "title_snapshot",
    "description_snapshot",
    "citation_full",
    "attribution",
    "attribution_short",
    "version_producer",
    "url_main",
    "url_download",
    "date_accessed",
    "date_published",
    "license",
]

SYSTEM_PROMPT = """You convert legacy OWID Source metadata into modern Origin metadata.
Always emit the result by calling the `emit_origin` tool.

# STEP 1: Does the data product coincide with this snapshot?

This is the central decision. Most snapshots coincide with their data product —
that means `title_snapshot` and `description_snapshot` are NULL.

THEY COINCIDE in all of these cases (the snapshot IS the data product):
- The source is a single paper, journal article, working paper, book, or thesis.
- The source is a one-off study with one accompanying dataset (no follow-up
  release series).
- The source is an OWID compilation of multiple raw sources combined into one
  snapshot — the "data product" IS the compilation.

ABSOLUTE RULE for these cases: `title_snapshot` is NULL and
`description_snapshot` is NULL. Every meaningful sentence of the legacy
`source.description` (whether descriptive of the data or describing OWID's
processing) goes into `description`. The fact that the legacy `meta.name`
phrases the snapshot differently from the producer's title does NOT mean
they differ — `meta.name` is just an internal OWID label, not a
producer-defined slice.

THEY DIFFER (snapshot is a slice of a larger product) ONLY when BOTH:
- The producer publishes a named multi-product database or report series
  (Maddison Project DB, V-Dem, Penn World Table, FAO databases, OECD reports,
  Statistics Canada tables, Correlates of War, World Bank WDI, etc.), AND
- This snapshot picks one specific producer-defined named slice (e.g. one
  table, one indicator group, one named topic). The slicing is the producer's,
  not OWID's.

When they differ:
- `title` = the data product name (e.g. `Penn World Table`).
- `title_snapshot` = `<data product> - <slice>` (e.g. `Penn World Table - National Accounts`).
- `description` = the data product (the producer's whole database).
- `description_snapshot` = OWID's processing notes for this slice, when present.

If you cannot point to a producer-published name for the broader data product
and a producer-defined name for the slice, default to "they coincide".

# STEP 2: Field rules

## producer (required, ≤255 chars)
Institution or author(s).
- One author: `Williams`. Two: `Williams and Jones`. Three or more: `Williams et al.`
- Prefer well-known acronyms (`NASA`, `FAO`); else the full institution name.
- For OWID compilations of several distinct raw sources, use `Various sources`.
- Must NOT contain: years, semicolons, `&` (use `and`), `OWID` / `Our World in Data`,
  trailing period (except when value ends `et al.`).
- Strip OWID-derivation prefixes: `Our World in Data based on X` → `X`.

## title (required, ≤512 chars)
Sentence-case start, no trailing period, no semicolons, no producer/version unless
they're part of the canonical name (`Education at a Glance 2017`).

## title_snapshot (default NULL)
Set ONLY when the data product and snapshot differ (Step 1). Format:
`<Data product> - <Slice>`. No year, no version, no producer, no period.

## description (default NULL when the legacy source has no data-product description)
Describes the producer's data. Sentence-case, end with period, 1–3 short paragraphs.
- DO NOT FABRICATE: a paper/book title alone is not a description. If the legacy
  has no descriptive content about the data, leave `description` null.
- Sentences belonging here: what the data is, who collected it, scope, producer
  methodology, pointers to the producer's own materials ("See the authors' data
  appendix").
- Preserve paragraph structure from the legacy `source.description` — emit real
  newlines (`\n\n`) between logical groups, not a single collapsed paragraph.

## description_snapshot (default NULL)
Set ONLY when the data product and snapshot DIFFER (Step 1).
If they coincide, `description_snapshot` is ALWAYS null even if the legacy
description contains OWID processing notes — fold those into `description`.

## citation_full (required)
Producer's preferred citation; long is OK. Start capital, end period, include the
publication year. Where legacy `source.published_by` is a full citation, it goes here.

## attribution_short (default NULL, ≤512 chars)
Set ONLY when there is a well-known acronym or short brand strictly shorter and more
recognizable than `producer` (e.g. `FAO`, `WHO`, `V-Dem`). If `producer` is already
short (`Fouquin and Hugot`, `World Bank`, `NASA`), leave null. No year, no period.

## version_producer (default NULL, ≤255 chars)
Set ONLY when the producer issues a series of releases AND uses a release identifier
that the legacy source mentions (e.g. `v14`, `Version 3`, `4.0.1.0`, `25.1`, or for
truly annual releases the year as identifier — Maddison Project DB, Total Economy DB).
Never set for a one-off paper/study. Working-paper numbers like `2016-14` are paper
IDs, not versions — never use them here.

## date_published (required)
`YYYY-MM-DD` or `YYYY` or `latest`. The CURRENT version's publication date. Never
pick a year that is part of a coverage range (`1827–2014`) or a projection
(`2030–2050`).

## url_main (default NULL when absent from legacy source)
Landing-page URL. Must appear VERBATIM in the legacy source — never invent one,
never swap a file extension or domain. Never use placeholders like `<UNKNOWN>`,
`N/A`, `TBD`, etc. If no URL exists in the legacy source, leave null.

## url_download (default NULL)
Direct-download URL. Must appear verbatim in the legacy source (typically
`source.source_data_url` or `source.url`). Otherwise null.

## date_accessed (required)
`YYYY-MM-DD`, copied from the legacy source.

## license (default NULL)
`{name, url}`. Leave null if not present in the legacy source.

# Anti-fabrication
- Never invent URLs (no extension swaps, no domain guesses).
- Never invent dates not implied by the legacy.
- Never invent a description from your own knowledge of the source.
- When uncertain about an optional field, set it to null.

# OWID writing style (for description, description_snapshot, citation_full, attribution)
American English. Sentence-case titles. "Data" is singular. Oxford comma. En dashes
for year ranges (`1990–2020`); em dashes with spaces for asides (` — like this — `).
Double quotes. Spell out 1–10 in prose. `US`, `UK`, `UN` without periods. Brand
spelling: `Our World in Data`, `OWID`. Author surnames only in citations.

The `notes` field is your free-form reasoning, not written to the DVC."""


ORIGIN_TOOL_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "producer": {
            "type": "string",
            "description": "Institution or author(s). No year, no semicolon, no &, no OWID. <=255 chars.",
        },
        "title": {"type": "string", "description": "Data product title. No trailing period. <=512 chars."},
        "title_snapshot": {
            "type": ["string", "null"],
            "description": "Snapshot subset title; null when the snapshot is the full product.",
        },
        "description": {"type": ["string", "null"], "description": "1-3 paragraph data product description."},
        "description_snapshot": {"type": ["string", "null"]},
        "citation_full": {"type": "string", "description": "Full academic citation; ends with a period."},
        "attribution": {"type": ["string", "null"]},
        "attribution_short": {"type": ["string", "null"], "description": "<=512 chars, no year, no trailing period."},
        "version_producer": {"type": ["string", "null"], "description": "<=255 chars."},
        "url_main": {
            "type": ["string", "null"],
            "description": "Full http(s) URL to landing page. Null if none in the legacy source.",
        },
        "url_download": {"type": ["string", "null"]},
        "date_accessed": {"type": "string", "description": "YYYY-MM-DD."},
        "date_published": {"type": "string", "description": "YYYY-MM-DD or YYYY or 'latest'."},
        "license": {
            "type": ["object", "null"],
            "properties": {
                "name": {"type": ["string", "null"]},
                "url": {"type": ["string", "null"]},
            },
            "additionalProperties": False,
        },
        "notes": {"type": ["string", "null"], "description": "Free-form reasoning, not written to DVC."},
    },
    "required": ["producer", "title", "citation_full", "date_accessed", "date_published"],
    "additionalProperties": False,
}


@dataclass
class MigrationResult:
    path: Path
    status: str
    origin: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    error: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0


def _get_client() -> anthropic.Anthropic:
    api_key = (
        os.environ.get("ETL_SOURCE_MIGRATION_API_KEY")
        or os.environ.get("ANTHROPIC_API_KEY")
        or ANTHROPIC_API_KEY
    )
    if not api_key:
        raise click.ClickException(
            "No Anthropic API key found. Set ETL_SOURCE_MIGRATION_API_KEY (preferred) "
            "or ANTHROPIC_API_KEY in your shell or .env."
        )
    return anthropic.Anthropic(api_key=api_key)


def snapshot_paths_from_migrated_dag() -> list[Path]:
    if not DAG_PATH.exists():
        raise click.ClickException(f"{DAG_PATH} not found. Run from the repo root.")
    uris = sorted({m.group(1).removeprefix("snapshot://") for m in SNAPSHOT_URI_RE.finditer(DAG_PATH.read_text())})
    return [SNAPSHOTS_DIR / f"{uri}.dvc" for uri in uris if (SNAPSHOTS_DIR / f"{uri}.dvc").exists()]


def load_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return ruamel_load(f) or CommentedMap()


def load_master_yaml(path: Path) -> dict[str, Any] | None:
    """Return the YAML on `origin/master`, or None if the file is new on this branch."""
    try:
        text = subprocess.check_output(
            ["git", "show", f"origin/master:{path.as_posix()}"],
            text=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError:
        return None
    from io import StringIO

    return ruamel_load(StringIO(text)) or CommentedMap()


def legacy_source_from_meta(meta: dict[str, Any]) -> dict[str, Any]:
    """Extract a flat legacy source dict from various legacy shapes."""
    if "source" in meta and meta.get("source"):
        return dict(meta["source"])
    return {
        "name": meta.get("source_name"),
        "description": meta.get("description"),
        "published_by": meta.get("source_published_by"),
        "source_data_url": meta.get("source_data_url"),
        "url": meta.get("url"),
        "date_accessed": meta.get("date_accessed"),
        "publication_date": meta.get("publication_date"),
        "publication_year": meta.get("publication_year"),
    }


def collect_legacy_input(snapshot_path: Path) -> dict[str, Any]:
    """Build the legacy-source payload to send to the LLM.

    Prefer origin/master version (clean legacy source). Fall back to the current branch
    if the file is new on this branch.
    """
    master_yml = load_master_yaml(snapshot_path)
    yml = master_yml if master_yml is not None else load_yaml(snapshot_path)
    meta = yml.get("meta") or {}
    source = legacy_source_from_meta(meta)
    return {
        "snapshot_path": snapshot_path.as_posix(),
        "snapshot_version": snapshot_path.parent.name,
        "snapshot_namespace": snapshot_path.parents[1].name,
        "source_block_origin": "origin/master" if master_yml is not None else "current branch",
        "legacy_source": _strip_nones(source),
        "legacy_top_level": _strip_nones(
            {
                "name": meta.get("name"),
                "description": meta.get("description"),
                "license": meta.get("license"),
            }
        ),
    }


def _strip_nones(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v not in (None, "", [])}


def call_emit_origin(
    client: anthropic.Anthropic,
    model: str,
    payload: dict[str, Any],
    *,
    correction: str | None = None,
    previous_output: dict[str, Any] | None = None,
    verbose: bool = False,
) -> tuple[dict[str, Any], int, int]:
    """Single API call constrained to the emit_origin tool.

    Returns (origin_dict, input_tokens, output_tokens).
    """
    user_text = (
        "Convert the following legacy snapshot metadata into a clean Origin object by calling "
        "the `emit_origin` tool. Follow every rule in the system prompt strictly.\n\n"
        f"```json\n{json.dumps(payload, indent=2, ensure_ascii=False, default=str)}\n```"
    )
    if correction and previous_output is not None:
        user_text += (
            "\n\nYour previous output was:\n"
            f"```json\n{json.dumps(previous_output, indent=2, ensure_ascii=False)}\n```\n"
            f"It violated the following rules: {correction}\n"
            "Please re-emit a corrected origin via `emit_origin`."
        )
    if verbose:
        console.rule(f"[yellow]Prompt to {model}")
        console.print(user_text)
    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        tools=[
            {
                "name": "emit_origin",
                "description": "Emit the migrated Origin object.",
                "input_schema": ORIGIN_TOOL_SCHEMA,
            }
        ],
        tool_choice={"type": "tool", "name": "emit_origin"},
        messages=[{"role": "user", "content": user_text}],
    )
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    for block in response.content:
        if isinstance(block, anthropic.types.ToolUseBlock) and block.name == "emit_origin":
            return dict(block.input), input_tokens, output_tokens  # type: ignore[arg-type]
    raise RuntimeError(f"Model did not call emit_origin: {response.content!r}")


def lint_origin(origin: dict[str, Any], legacy_text: str = "") -> list[str]:
    """Return a list of style/length violations. Empty list means OK.

    If `legacy_text` is provided (the concatenated string form of the legacy source),
    we additionally verify that URLs appear verbatim in it — to catch fabrications
    like swapping a file extension on url_main to invent url_download.
    """
    issues: list[str] = []

    producer = origin.get("producer") or ""
    if YEAR_RE.search(producer):
        issues.append("`producer` contains a year.")
    if ";" in producer:
        issues.append("`producer` contains a semicolon.")
    if "&" in producer:
        issues.append("`producer` contains '&'; use 'and' instead.")
    if re.search(r"\b(OWID|Our World in Data)\b", producer, re.IGNORECASE):
        issues.append("`producer` mentions OWID / Our World in Data.")
    if producer.endswith(".") and not producer.endswith("et al."):
        issues.append("`producer` ends with a period (and is not `et al.`).")

    for field_name, max_len in DB_LIMITS.items():
        val = origin.get(field_name) or ""
        if isinstance(val, str) and len(val) > max_len:
            issues.append(f"`{field_name}` exceeds {max_len} chars (got {len(val)}).")

    title = origin.get("title") or ""
    if title.endswith("."):
        issues.append("`title` ends with a period.")

    date_published = origin.get("date_published") or ""
    if not DATE_PUBLISHED_RE.match(date_published):
        issues.append(f"`date_published` is not YYYY/YYYY-MM-DD/'latest' (got {date_published!r}).")

    citation_full = origin.get("citation_full") or ""
    if citation_full and not citation_full.endswith("."):
        issues.append("`citation_full` does not end with a period.")

    version_producer = origin.get("version_producer") or ""
    if isinstance(version_producer, str) and re.match(r"^\d{4}-\d{1,3}$", version_producer.strip()):
        issues.append(
            f"`version_producer` value {version_producer!r} matches a working-paper "
            "number pattern (YYYY-N). Working-paper numbers are not dataset versions. Set null."
        )

    attribution_short = origin.get("attribution_short") or ""
    if attribution_short and attribution_short.strip() == producer.strip():
        issues.append(
            "`attribution_short` is identical to `producer` and so adds no information. Set null."
        )
    if attribution_short and YEAR_RE.search(attribution_short):
        issues.append(
            f"`attribution_short` contains a year ({attribution_short!r}); years live in `date_published`."
        )
    if attribution_short and attribution_short.endswith("."):
        issues.append("`attribution_short` ends with a period.")

    url_main = origin.get("url_main") or ""
    url_download = origin.get("url_download") or ""
    if url_download and url_download.strip() == url_main.strip():
        issues.append(
            "`url_download` is identical to `url_main`. The download URL must be a direct "
            "download link distinct from the landing page; if none exists, set null."
        )

    title_snapshot = origin.get("title_snapshot") or ""
    if title_snapshot and YEAR_RE.search(title_snapshot):
        issues.append(
            f"`title_snapshot` contains a year ({title_snapshot!r}); years belong in "
            "`date_published`, not in titles."
        )
    if title_snapshot and producer and producer.lower() in title_snapshot.lower():
        # Canonical-name exception: if the producer name also appears in `title`, the
        # producer is part of the data product's well-known name (e.g.
        # "Education at a Glance 2017: OECD Indicators") — title_snapshot inherits it.
        if not (title and producer.lower() in title.lower()):
            issues.append(
                f"`title_snapshot` includes the producer name ({producer!r}); titles must "
                "not mention the producer. Either drop the producer from title_snapshot or set null."
            )

    if legacy_text:
        for url_field in ("url_main", "url_download"):
            url = origin.get(url_field) or ""
            if isinstance(url, str) and url and url not in legacy_text:
                issues.append(
                    f"`{url_field}` value {url!r} does not appear verbatim in the legacy source. "
                    "URLs must not be fabricated; if no matching URL exists in the legacy source, set null."
                )

    return issues


def to_origin_dataclass(origin: dict[str, Any]) -> Origin:
    """Validate by constructing the catalog Origin dataclass. Raises on bad date_published."""
    license_data = origin.get("license") or None
    license_obj = License(**license_data) if license_data else None
    payload = {k: v for k, v in origin.items() if k != "notes" and k != "license"}
    return Origin(license=license_obj, **payload)


def build_origin_block(origin: dict[str, Any]) -> CommentedMap:
    """Render origin dict to a CommentedMap with stable field ordering and no nulls."""
    block = CommentedMap()
    for key in ORIGIN_FIELD_ORDER:
        if key not in origin:
            continue
        value = origin[key]
        if value is None or value == "":
            continue
        if key == "license":
            lic = CommentedMap()
            for lk in ("name", "url"):
                lv = value.get(lk) if isinstance(value, dict) else getattr(value, lk, None)
                if lv:
                    lic[lk] = lv
            if lic:
                block[key] = lic
            continue
        if isinstance(value, str):
            # Some model outputs include literal "\n" sequences instead of real newlines;
            # normalize them so we get a proper YAML literal block.
            if "\\n" in value and "\n" not in value:
                value = value.replace("\\n", "\n")
            if "\n" in value:
                block[key] = LiteralScalarString(value)
                continue
        block[key] = value
    return block


def rewrite_dvc(snapshot_path: Path, origin_block: CommentedMap) -> None:
    yml = load_yaml(snapshot_path)
    meta = yml.get("meta") or CommentedMap()

    legacy_keys = {
        "source",
        "source_name",
        "source_published_by",
        "source_data_url",
        "url",
        "date_accessed",
        "publication_date",
        "publication_year",
        "name",
        "description",
        # Drop any pre-existing origin written by the naive script.
        "origin",
    }

    new_meta = CommentedMap()
    new_meta["origin"] = origin_block
    for key, value in meta.items():
        if key not in legacy_keys:
            new_meta[key] = value

    yml["meta"] = new_meta
    snapshot_path.write_text(ruamel_dump(yml))


def migrate_one(
    snapshot_path: Path,
    *,
    client: anthropic.Anthropic,
    model: str,
    apply: bool,
    verbose: bool,
) -> MigrationResult:
    payload = collect_legacy_input(snapshot_path)
    if not payload["legacy_source"]:
        return MigrationResult(path=snapshot_path, status="skip-no-legacy-source")

    # Concatenate legacy text for URL fabrication checks.
    legacy_text = json.dumps(payload, ensure_ascii=False, default=str)

    total_in = 0
    total_out = 0

    origin, in_tok, out_tok = call_emit_origin(client, model, payload, verbose=verbose)
    total_in += in_tok
    total_out += out_tok
    issues = lint_origin(origin, legacy_text=legacy_text)

    if issues:
        if verbose:
            console.print(f"[yellow]Lint issues, retrying once: {issues}")
        origin, in_tok, out_tok = call_emit_origin(
            client,
            model,
            payload,
            correction="; ".join(issues),
            previous_output=origin,
            verbose=verbose,
        )
        total_in += in_tok
        total_out += out_tok
        issues = lint_origin(origin, legacy_text=legacy_text)

    cost = estimate_cost_usd(model, total_in, total_out)

    try:
        to_origin_dataclass(origin)
    except (TypeError, ValueError) as exc:
        return MigrationResult(
            path=snapshot_path,
            status="needs-review",
            origin=origin,
            notes=origin.get("notes") or "",
            error=f"Origin() validation failed: {exc}",
            input_tokens=total_in,
            output_tokens=total_out,
            cost_usd=cost,
        )

    if issues:
        return MigrationResult(
            path=snapshot_path,
            status="needs-review",
            origin=origin,
            notes=origin.get("notes") or "",
            error="; ".join(issues),
            input_tokens=total_in,
            output_tokens=total_out,
            cost_usd=cost,
        )

    block = build_origin_block(origin)
    if apply:
        rewrite_dvc(snapshot_path, block)
        # Sanity check: parse it back.
        SnapshotMeta.load_from_yaml(snapshot_path)

    return MigrationResult(
        path=snapshot_path,
        status="applied" if apply else "would-apply",
        origin=origin,
        notes=origin.get("notes") or "",
        input_tokens=total_in,
        output_tokens=total_out,
        cost_usd=cost,
    )


def render_origin_yaml(origin: dict[str, Any]) -> str:
    block = CommentedMap()
    block["meta"] = CommentedMap()
    block["meta"]["origin"] = build_origin_block(origin)
    return ruamel_dump(block)


def write_report(report_path: Path, results: list[MigrationResult]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "status",
        "path",
        "producer",
        "title",
        "date_published",
        "citation_full",
        "error",
        "notes",
    ]

    def _row(r: MigrationResult) -> list[str]:
        o = r.origin
        return [
            r.status,
            r.path.as_posix(),
            (o.get("producer") or "").replace("\t", " ").replace("\n", " "),
            (o.get("title") or "").replace("\t", " ").replace("\n", " "),
            o.get("date_published") or "",
            (o.get("citation_full") or "").replace("\t", " ").replace("\n", " "),
            r.error.replace("\t", " ").replace("\n", " "),
            r.notes.replace("\t", " ").replace("\n", " "),
        ]

    lines = ["\t".join(columns)] + ["\t".join(_row(r)) for r in results]
    report_path.write_text("\n".join(lines) + "\n")


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-p",
    "--path",
    "paths",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Migrate a single DVC file. Repeatable. Skips dag/migrated.yml scan when used.",
)
@click.option("--limit", type=int, default=None, help="Process at most N snapshots.")
@click.option("--offset", type=int, default=0, help="Skip the first N snapshots before applying --limit.")
@click.option("--apply/--dry-run", default=False, help="Write back to the DVC files. Default is dry-run.")
@click.option("--model", default=DEFAULT_MODEL, show_default=True, help="Anthropic model id.")
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=DEFAULT_REPORT_PATH,
    show_default=True,
    help="TSV report destination.",
)
@click.option(
    "--concurrency",
    type=int,
    default=1,
    show_default=True,
    help="Number of parallel API calls. >1 disables --verbose interleaving and orders output by completion.",
)
@click.option("-v", "--verbose", is_flag=True, help="Print prompt and proposed origin per file.")
def main(
    paths: tuple[Path, ...],
    limit: int | None,
    offset: int,
    apply: bool,
    model: str,
    report_path: Path,
    concurrency: int,
    verbose: bool,
) -> None:
    """Re-migrate Source -> Origin via Claude for snapshot DVC files."""
    if paths:
        snapshot_paths = list(paths)
    else:
        snapshot_paths = snapshot_paths_from_migrated_dag()
    if offset:
        snapshot_paths = snapshot_paths[offset:]
    if limit is not None:
        snapshot_paths = snapshot_paths[:limit]

    if not snapshot_paths:
        raise click.ClickException("No snapshot DVC files to process.")

    client = _get_client()
    results: list[MigrationResult] = []
    print_lock = threading.Lock()

    def _emit(result: MigrationResult) -> None:
        with print_lock:
            console.rule(f"[cyan]{result.path}")
            console.print(f"[bold]status:[/bold] {result.status}")
            if result.input_tokens or result.output_tokens:
                console.print(
                    f"[dim]tokens: {result.input_tokens} in / {result.output_tokens} out  "
                    f"cost: ${result.cost_usd:.4f}"
                )
            if result.error:
                console.print(f"[red]error:[/red] {result.error}")
            if result.origin:
                console.print(render_origin_yaml(result.origin))
            if result.notes:
                console.print(f"[dim]notes: {result.notes}")

    def _run(sp: Path) -> MigrationResult:
        # Verbose only makes sense in single-threaded mode; parallel runs would interleave prompts.
        v = verbose and concurrency == 1
        try:
            return migrate_one(sp, client=client, model=model, apply=apply, verbose=v)
        except Exception as exc:  # noqa: BLE001 - surface unexpected errors per-file, keep going
            return MigrationResult(path=sp, status="error", error=f"{type(exc).__name__}: {exc}")

    if concurrency <= 1:
        for sp in snapshot_paths:
            r = _run(sp)
            results.append(r)
            _emit(r)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = {ex.submit(_run, sp): sp for sp in snapshot_paths}
            for fut in as_completed(futures):
                r = fut.result()
                results.append(r)
                _emit(r)

    write_report(report_path, results)
    by_status: dict[str, int] = {}
    total_in = 0
    total_out = 0
    total_cost = 0.0
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1
        total_in += r.input_tokens
        total_out += r.output_tokens
        total_cost += r.cost_usd
    console.rule("[bold]Summary")
    for status, count in sorted(by_status.items()):
        console.print(f"  {status}: {count}")
    console.print(
        f"\n  tokens: {total_in:,} in / {total_out:,} out  total cost: ${total_cost:.4f} "
        f"(model {model})"
    )
    console.print(f"\nReport written to {report_path}")
    if any(r.status in {"error", "needs-review"} for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
