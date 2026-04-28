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

SYSTEM_PROMPT = """You convert legacy OWID "Source" metadata into modern OWID "Origin" metadata.
You will receive the legacy `source` block (and any related top-level fields) from a
snapshot DVC file, and you must emit a clean `origin` object that follows OWID's
documented Origin style guidance.

Always emit the result by calling the `emit_origin` tool. Never write origin fields
in plain text.

# Origin field rules

## producer (required, <=255 chars)
- Name of the institution or author(s) that produced the data product.
- Must NOT include a date or year.
- Must NOT mention "Our World in Data" or "OWID".
- Must NOT contain a semicolon `;`.
- Must NOT use `&` (write "and" instead).
- Must start with a capital letter (exception: lowercase author names like `van Haasteren`).
- Must NOT end with a period, except when the value ends with `et al.`.
- Authors:
    - 1 author: `Williams`
    - 2 authors: `Williams and Jones`
    - 3 or more authors: `Williams et al.`
- Acronyms: prefer the well-known acronym if it is more recognizable (e.g. `NASA`, `FAO`).
  Otherwise use the full institution name.
- If the producer is OWID-derived ("Our World in Data based on X"), use just `X`.
- Examples (good): `NASA`, `World Bank`, `Williams et al.`, `Fouquin and Hugot`.
- Examples (bad): `NASA (2023)`, `Williams & Jones`, `Our World in Data based on NASA`,
  `Michel Fouquin & Jules Hugot, 2016. "Two Centuries..."`.

## title (required, <=512 chars)
- Title of the original data product, NOT the snapshot subset.
- Must start with a capital letter, must NOT end with a period.
- Must NOT mention `producer` or `version_producer` (unless those are part of the well-known data product name).
- If the producer's data product has a well-known name, use that name exactly (typo fixes ok).

## title_snapshot (optional, default null)
- DEFAULT TO NULL. Most snapshots ARE the data product, not a subset.
- HARD RULE for papers/books/articles: when the source is a single paper, working
  paper, journal article, or book (signals: legacy `published_by` is an academic
  citation; producer is author surnames like `Williams`, `Smith and Jones`,
  `Williams et al.`), `title_snapshot` MUST be null. The snapshot IS the paper's
  data — there is no "named slice".
- Set `title_snapshot` only when ALL of the following are true:
  1. The producer's data product is a named database, table series, or report
     (NOT a single paper or book).
  2. That data product has several distinct named sub-products that the producer
     (not OWID) distinguishes.
  3. The text after the dash adds NEW information — a sub-table name, a topic,
     or a part — and is not just a year, version, producer name, or restatement
     of `title`.
- DO (real, good — drawn from existing OWID origins):
  - `Penn World Table - National Accounts`
  - `Maddison Project Database - GDP per capita growth in the UK`
  - `Luxembourg Income Study (LIS) - Percentile data`
  - `Global Carbon Budget - Fossil fuels`
  - `War data - Inter-State Wars`
  - `Statistics Canada low-income statistics - All persons, after tax`
- DO NOT (anti-patterns):
  - Appending a year that already lives in `date_published`
    (BAD: `Number of farmed decapod crustaceans (2016)`)
  - Appending a version that already lives in `version_producer`
    (BAD: `Child mortality rate under age five v7`, `War data v4.0`)
  - Appending the producer's name + year (BAD: `... (Geyer et al., 2017)`)
  - Restating `title` with no new information.
- Format: `Data product - Specific slice`. No trailing period, no semicolon.

## description (recommended, default null when not implied by the input)
- Description of the data product ITSELF (the producer's data), not OWID's snapshot.
- Start with a capital letter, end with a period.
- 1-3 paragraphs, succinct. Use the producer's wording where reasonable.
- Don't mention `producer` or `version_producer`.
- HARD RULE — DO NOT FABRICATE: if the legacy source has no description of the data
  product, leave `description` NULL. Do NOT invent one from the paper title or your
  own knowledge of the source. A book/paper title alone is not a description.

## description_snapshot (recommended when there is OWID-specific snapshot detail)
- This is where OWID-specific aggregations, calculations, weightings, exclusions,
  region groupings, or any "we computed X by Y" notes from the legacy description go.
- Examples of content that belongs in `description_snapshot`:
  - "The 'World' time series is the sum of country exports / imports."
  - "Regional aggregates use the World Bank's income groupings; series start in 1970."
  - "Germany combines West Germany and Germany; East Germany excluded."
  - "We weighted gender-specific incidences by the male:female population ratio."
  - "Russia's series combines Russia and the USSR."
  - "Estimates for windows of years are reported at the middle year of each window."
- Style: capital letter at the start, period at the end. Multi-paragraph allowed.

## INFORMATION PRESERVATION (critical)
- The legacy `source.description` (and any legacy top-level `meta.description`) is the
  authoritative content. Every meaningful sentence in it MUST appear, in some form, in
  either `description` (data-product-level) or `description_snapshot` (OWID/snapshot
  level). Do not silently drop calculation notes, regional definitions, scope
  exclusions, or methodological caveats.
- If you cannot tell whether a sentence is data-product-level or snapshot-level, put
  it in `description_snapshot`. Don't drop it.

## citation_full (required)
- Full academic citation per the producer's preferred format. Long is OK.
- Start with a capital letter, end with a period.
- Must include the publication year.
- This is where any long source.published_by string belongs.

## attribution_short (optional, default null, <=512 chars)
- DEFAULT TO NULL. Only set if there is a well-known acronym or short brand name that is
  shorter and clearly more recognizable than `producer`.
- If `producer` is already short (e.g. `Fouquin and Hugot`, `Smith et al.`, `World Bank`),
  leave `attribution_short` null — it would just duplicate `producer`.
- No year, no trailing period.
- Examples (good): `FAO`, `WHO`, `World Bank`, `V-Dem`. Examples (bad): `UN FAO`, `FAO (2023)`.

## version_producer (optional, default null, <=255 chars)
- STRONG DEFAULT: NULL. Only set this when ALL of the following are true:
  1. The dataset is part of a series of releases by the same producer (i.e. there have
     been or will be multiple versions over time — e.g. v1, v2, v3, or annual releases
     in 2019, 2020, 2021, ...).
  2. The producer uses a specific label to distinguish their releases.
  3. The legacy source contains evidence of that label (in `name`, `published_by`, etc.).
- A single paper accompanied by a single one-off dataset has NO `version_producer`.
  Examples that are one-off and therefore null: most working papers; conference-paper
  data; thesis-data appendices; a study that was published once and never revised.
- Real positive examples (verbatim from existing OWID origins):
  - `v14`, `v15`, `v16` (V-Dem — explicitly versions its release)
  - `Version 1`, `Version 3` (textual release labels)
  - `4.0.1.0`, `HadCRUT.5.0.2.0`, `25.1` (semver-like release strings)
  - `'3.0'` (COLDAT — note: even though it is cited via a working paper,
    `version_producer` points at the dataset release `3.0`, NOT the paper number)
  - `'2013'`, `'2019'`, `'2024'` — ONLY for datasets the producer re-releases yearly
    using just the year as the release identifier (Maddison Project DB, Total Economy
    DB). Year-as-version is NEVER appropriate for a one-off publication where the
    year merely matches `date_published`.
- NEGATIVE examples (do NOT set as version_producer):
  - A working-paper number such as `2016-14` (paper id, not dataset version).
  - The publication year of an associated paper that has no follow-up release
    (`'2016'`, `'2018'` — even though these match positive examples elsewhere, the
    distinguishing factor is whether the dataset has a release series).
  - Producer + year combos: `CEPII 2016`, `Smith et al. 2020`.
  - Citation fragments.
- If unsure whether a dataset has a release series, leave null.
- HARD RULE: any value matching the pattern `YYYY-N` or `YYYY-NN` (e.g. `2016-14`,
  `2020-3`) is REJECTED automatically — that pattern is a working-paper number,
  never a dataset version. Do not output it.
- HARD RULE: if your reasoning ever expresses doubt about whether the field should be
  set ("arguably", "may warrant reconsideration", "could go either way", "this is
  borderline"), the answer is NULL. Do not include the field.

## date_published (required)
- Format: `YYYY-MM-DD` or `YYYY` or `latest`.
- Must reflect when the CURRENT version was published, not first release or projection year.
- Never select a year that is part of a data-coverage range (`1827-2014`) or a projection (`2030-2050`).

## url_main (required)
- Full URL to the dataset's main/landing page (must start with http/https).

## url_download (optional)
- Direct download URL, no UI required. If none exists, leave null.

## date_accessed (required)
- `YYYY-MM-DD`. Use the value from the legacy source.

## license (optional)
- Object with `name` and `url`. Leave null if unknown.

# Notes
- The legacy `meta.description` field, if any, is part of the data-product description.
- If the legacy `source.published_by` is a full citation, put it in `citation_full`,
  not in `producer`.
- HARD RULE: do not fabricate URLs. `url_main` and `url_download` must each appear
  VERBATIM somewhere in the legacy source (typically in `source.url`,
  `source.source_data_url`, `source.published_by`, or `source.description`). Never
  swap a file extension (e.g. `.htm` → `.txt`), never swap a domain, never invent a
  download URL by mutating `url_main`. If no direct download link exists in the
  legacy source, set `url_download` to null.
- Do not invent dates that are not implied by the input.
- The `notes` field is free-form reasoning that will not be written to the DVC.

# OWID Writing and Style Guide (applies to all string fields)

These rules come from OWID's Writing and Style Guide. Apply them in `description`,
`description_snapshot`, `citation_full`, `attribution`, and any other prose:

- Write in American English (e.g. "analyzed", "program"; not "analysed", "programme").
- Sentence-case for titles (capitalize only the first word and proper nouns); no trailing period in titles.
- "Data" is singular: "the data is", not "the data are".
- Use the Oxford comma in lists.
- Use en dashes (–), not hyphens (-), for year ranges: `1990–2020`, not `1990-2020`.
- Use em dashes (—) with spaces on both sides for asides: ` — like this — `.
- Use double quotes (`"..."`), not single quotes, for quotations and titles.
- Spell out numbers one to ten in prose; use digits for 11+.
- Spell out acronyms on first use, followed by the acronym in parentheses
  (exceptions: `US`, `UK`, `UN`). For `US` and `UK`, no periods.
- Brand spelling: write `Our World in Data` (lowercase "in") and `OWID` (all caps);
  never `Our World In Data` or `OWiD`. Note: producer/attribution_short still must NOT
  contain these — this rule is for descriptions if OWID's own role is mentioned.
- For author citations on charts: use surnames only.
  - One author: `Williams`. Two: `Williams and Jones`. Three or more: `Williams et al.`.

These rules are advisory for description text — if the producer's preferred citation
in `citation_full` uses different conventions, follow the producer."""


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
        "url_main": {"type": "string", "description": "Full http(s) URL to landing page."},
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
    "required": ["producer", "title", "citation_full", "url_main", "date_accessed", "date_published"],
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
        issues.append(
            f"`title_snapshot` includes the producer name ({producer!r}); titles must not "
            "mention the producer. Either drop the producer from title_snapshot or set null."
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
        if isinstance(value, str) and "\n" in value:
            block[key] = LiteralScalarString(value)
        else:
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
