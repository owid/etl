"""data-diff service for owidbot.

Runs `etl diff` against the remote catalog, uploads the full self-contained HTML report to R2
(publicly served at https://catalog.ourworldindata.org/) and posts a compact, signal-first
summary to the PR: only changed datasets with a few sample rows, everything deeper lives in
the linked report.
"""

import os
import re
import shlex
import subprocess
import tempfile
import time
from pathlib import Path

import structlog
from botocore.exceptions import BotoCoreError
from owid.catalog import s3_utils

from etl.datadiff_report import ColumnDiffResult, DatasetDiffResult, DiffReport, dataset_watch_key
from etl.paths import BASE_DIR

log = structlog.get_logger()

EXCLUDE_DATASETS = "excess_mortality|covid|fluid|flunet|country_profile|garden/ihme_gbd/2019/gbd_risk"

# The owid-catalog R2 bucket is publicly served at https://catalog.ourworldindata.org/.
REPORT_S3_PATH = "s3://owid-catalog/diffs/{branch}/data-diff.html"
REPORT_URL = "https://catalog.ourworldindata.org/diffs/{branch}/data-diff.html"

# Keep the comment body comfortably under GitHub's 65,536-char limit.
MAX_DIFF_CHARS = 50000
# Sample rows shown per value diff in the PR comment (the HTML report holds up to SAMPLE_LIMIT).
MAX_SAMPLE_ROWS = 3
# Cap on a single sample line; long text values (e.g. descriptions) get truncated.
MAX_LINE_CHARS = 160

_SYMBOLS = {"new": "+", "removed": "-", "changed": "~", "error": "!"}
_ICONS = {"clean": "✅", "changed": "❌", "error": "⚠️"}


def run(include: str, branch: str | None = None) -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        json_path = Path(tmp_dir) / "data-diff.json"
        html_path = Path(tmp_dir) / "data-diff.html"

        call_etl_diff(include, output_json=json_path, output_html=html_path)

        report = DiffReport.from_json(json_path.read_text())
        # A report that just says "no differences" isn't worth hosting — upload only when
        # there's something to show.
        report_url = upload_html_report(html_path, branch) if branch and report.status != "clean" else None

    return format_comment(report, report_url)


def upload_html_report(html_path: Path, branch: str) -> str | None:
    """Upload the full HTML report to R2 and return its public URL, or None if the upload fails."""
    # Branch names can contain characters that don't belong in an URL path (e.g. slashes).
    safe_branch = re.sub(r"[^A-Za-z0-9._-]", "-", branch)
    try:
        s3_utils.upload(
            REPORT_S3_PATH.format(branch=safe_branch),
            html_path,
            public=True,
            content_type="text/html; charset=utf-8",
            # The object is overwritten on every push; keep CDN caching short.
            cache_control="max-age=60",
        )
    except (s3_utils.UploadError, s3_utils.MissingCredentialsError, BotoCoreError) as e:
        log.warning("Failed to upload data-diff HTML report", error=str(e))
        return None
    # Cloudflare caches the object at the edge for a few minutes; a fresh query param on every
    # run makes the PR comment always link to the latest report.
    return REPORT_URL.format(branch=safe_branch) + f"?v={int(time.time())}"


def format_comment(report: DiffReport, report_url: str | None) -> str:
    summary = _format_summary(report, report_url)

    # Triage order, same as the HTML report's watch list: tier first, data loss ahead of
    # bigger-but-benign changes — so truncation (MAX_DIFF_CHARS) never drops a lossy dataset.
    changed = sorted((ds for ds in report.datasets if ds.change_kind != "identical"), key=dataset_watch_key)
    diff = "\n".join(line for ds in changed for line in _format_dataset(ds))
    diff = _truncate_diff(diff)

    diff_block = f"\n```diff\n{diff}\n```\n" if diff else "\nNo differences found.\n"

    tail_bits = []
    if report.n_identical:
        tail_bits.append(f"{report.n_identical} compared dataset(s) turned out identical")
    if report.skipped_cascade:
        tail_bits.append(f"{report.skipped_cascade} skipped (data files unchanged, only the source checksum cascaded)")
    tail_note = (
        "= " + " · ".join(tail_bits) + ("; details in the full report" if report_url else "") if tail_bits else ""
    )

    footer = (
        f"Automatically updated datasets matching _{EXCLUDE_DATASETS}_ are not included. "
        "Run locally with `etl diff REMOTE data/ --include <dataset> --verbose`."
    )

    sections = [
        # <summary> must directly follow <details> for GitHub to render the block
        f"<details>\n<summary><b>data-diff</b>: {summary}</summary>",
        diff_block.strip(),
        *([tail_note] if tail_note else []),
        footer,
        "</details>",
    ]
    return "\n\n".join(sections)


def _format_summary(report: DiffReport, report_url: str | None) -> str:
    parts = []
    for label, n in [
        ("changed", report.n_changed),
        ("new", report.n_new),
        ("removed", report.n_removed),
        ("error(s)", report.n_errors),
    ]:
        if n:
            parts.append(f"{n} {label}")
    if not parts:
        parts.append("no differences")
    if report.n_identical:
        parts.append(f"{report.n_identical} identical")
    if report.skipped_cascade:
        parts.append(f"{report.skipped_cascade} skipped")

    summary = f"{_ICONS[report.status]} " + " · ".join(parts)
    if report_url:
        summary += f' — <a href="{report_url}"><b>full report</b></a>'
    return summary


def _format_dataset(ds: DatasetDiffResult) -> list[str]:
    kind = ds.change_kind
    if kind == "error":
        return [f"! {ds.path} — error: {_truncate(ds.error or '', 300)}"]
    if kind == "new":
        n_tables = len(ds.tables)
        n_cols = sum(len(t.columns) for t in ds.tables)
        return [f"+ {ds.path} (new dataset: {n_tables} table(s), {n_cols} column(s))"]
    if kind == "removed":
        return [f"- {ds.path} (removed dataset)"]

    suffix = " (new version)" if ds.is_new_version else ""
    lines = [f"~ {ds.path}{suffix}"]
    if ds.meta_diff:
        lines.append("    ~ dataset metadata changed")
    for t in ds.tables:
        if t.kind == "changed":
            lines.append(f"    ~ {t.name} (changed table metadata)")
        elif t.kind in ("new", "removed"):
            lines.append(f"    {_SYMBOLS[t.kind]} {t.name} ({t.kind} table, {len(t.columns)} column(s))")
            continue
        for c in t.columns:
            if c.kind == "identical":
                continue
            lines += _format_column(t.name, c)
    return lines


def _format_column(table_name: str, c: ColumnDiffResult) -> list[str]:
    name = f"{table_name}.{c.name}"
    if c.kind in ("new", "removed"):
        return [f"    {_SYMBOLS[c.kind]} {name} ({c.kind} column)"]

    label = ", ".join(c.changes) if c.changes else "changed"
    dim = " dim" if c.is_dim else ""
    lines = [f"    ~ {name}{dim} ({label})"]
    for v in c.value_diffs:
        head = {"new": "New", "removed": "Removed", "changed": "Changed"}[v.kind]
        lines.append(f"        {v.symbol} {head} values: {v.count:,} / {v.total:,} ({v.pct:.2f}%)")
        for row in v.sample[:MAX_SAMPLE_ROWS]:
            lines.append("            " + _truncate(_format_row(row, c.name), MAX_LINE_CHARS))
        if v.count > MAX_SAMPLE_ROWS:
            lines.append(f"            (+{v.count - min(len(v.sample), MAX_SAMPLE_ROWS):,} more)")
    return lines


def _format_row(row: dict[str, str], col: str) -> str:
    """Format a sample record as `dim1 dim2: value` or `dim1 dim2: old → new`."""
    old = row.get(f"{col} -")
    new = row.get(f"{col} +")
    val = row.get(col)
    # "anomaly score" is a display column of the HTML report, not a dim.
    dims = " ".join(v for k, v in row.items() if k not in (col, f"{col} -", f"{col} +", "anomaly score"))
    if old is not None or new is not None:
        return f"{dims}: {old} → {new}" if dims else f"{old} → {new}"
    if val is not None:
        return f"{dims}: {val}" if dims else val
    return dims


def _truncate(s: str, limit: int) -> str:
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _truncate_diff(diff: str) -> str:
    if len(diff) <= MAX_DIFF_CHARS:
        return diff
    cut = diff.rfind("\n", 0, MAX_DIFF_CHARS)
    return diff[:cut] + "\n\n... diff too long, truncated — see the full report ..."


def _tail_output(output: str, max_lines: int = 80) -> str:
    """Return the last non-empty lines from command output for error messages."""
    lines = output.strip().splitlines()
    if len(lines) > max_lines:
        lines = [f"... ({len(lines) - max_lines} lines omitted)", *lines[-max_lines:]]
    return "\n".join(lines)


def call_etl_diff(include: str, output_json: Path, output_html: Path) -> None:
    cmd = [
        "uv",
        "run",
        "etl",
        "diff",
        "REMOTE",
        "data/",
        "--changed",
        "--include",
        include,
        "--exclude",
        EXCLUDE_DATASETS,
        "--verbose",
        "--workers",
        "3",
        "--output-json",
        str(output_json),
        "--output-html",
        str(output_html),
    ]

    cmd_str = shlex.join(cmd)
    print(cmd_str)

    env = os.environ.copy()
    env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env["PATH"]

    result = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True)
    stdout, stderr = result.communicate()

    # Remove all warnings from stderr
    stderr = re.sub(r"^.*WARNING.*", "", stderr, flags=re.MULTILINE).strip()

    if result.returncode == 1 and "Found differences" in stdout:
        log.info("etl diff found differences", returncode=result.returncode)
    elif result.returncode != 0:
        details = [f"etl diff failed (exit {result.returncode})", f"Command: {cmd_str}"]
        if stderr:
            details.append(f"stderr (tail):\n{_tail_output(stderr)}")
        if stdout:
            details.append(f"stdout (tail):\n{_tail_output(stdout)}")
        if not stdout and not stderr:
            details.append("No stdout or stderr was captured.")
        raise RuntimeError("\n\n".join(details))
    if stderr:
        log.warning("etl diff produced stderr output", stderr=stderr)
