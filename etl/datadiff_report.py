"""Structured results for `etl diff` and an HTML renderer on top of them.

`etl.datadiff.DatasetDiff` collects one `DatasetDiffResult` per compared dataset while it
prints its usual rich-text summary. The results aggregate into a `DiffReport`, which can be
serialized to JSON (consumed by owidbot to build the PR comment) or rendered as a
self-contained HTML report (uploaded to R2 and linked from the PR comment).

This module must not import from `etl.datadiff` to avoid a circular dependency.
"""

import datetime as dt
import html
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ChangeKind = Literal["new", "removed", "changed", "identical", "error"]

# Number of sample rows stored per value diff (the text output shows only 5; the HTML report
# can afford more).
SAMPLE_LIMIT = 100

# Severity tiers (thresholds on the BARD-based anomaly score, stored in [0, 1] and displayed
# as a percentage): they tell a reviewer where to stop reading. large = score ≥ 15%, moderate
# ≥ 1%, small = anything non-zero below that (usually rounding-level noise).
Tier = Literal["large", "moderate", "small", "none"]
TIER_LARGE = 0.15
TIER_MODERATE = 0.01
TIER_ICONS = {"large": "🔴", "moderate": "🟡", "small": "🟢"}

# Entries shown in the "Top changes" watch list at the head of the report before the
# "show more" toggle, and the hard cap on how many are rendered (hidden) behind it.
TOP_CHANGES_LIMIT = 15
TOP_DATASETS_LIMIT = 10
TOP_CHANGES_MAX = 100
TOP_DATASETS_MAX = 50
# The triage aids (Top changes section, tier strip) only appear when there's something to
# triage: a report with a couple of changed datasets is already scannable, and on the common
# single-dataset PR they'd be redundant noise. Per-row tier chips render at any size.
TRIAGE_MIN_DATASETS = 3


def _tier(severity: float) -> Tier:
    if severity >= TIER_LARGE:
        return "large"
    if severity >= TIER_MODERATE:
        return "moderate"
    if severity > 0:
        return "small"
    return "none"


def format_score(score: float) -> str:
    """Display format for an anomaly score: a percentage in [0, 100], like Anomalist shows it.

    Integer above 10%, more precision below so the smaller tiers don't collapse to 0%. As a rule
    of thumb for same-sign values, a score of s% corresponds to a relative change of roughly
    2s/(100-s): 1% ≈ 2%, 15% ≈ 35%, 50% ≈ 200%.
    """
    pct = 100 * score
    if pct >= 10:
        return f"{pct:.0f}%"
    return f"{pct:.1f}%" if pct >= 1 else f"{pct:.2f}%"


@dataclass
class ValueDiff:
    """New, removed or changed values of a single column."""

    kind: Literal["new", "removed", "changed"]
    count: int
    total: int
    # Display-ready records; rows of a "changed" diff have "<col> -" (old) and "<col> +" (new) keys.
    sample: list[dict[str, str]] = field(default_factory=list)
    # True when the sample of a numeric "changed" diff is sorted by anomaly score (largest first)
    # and carries an "anomaly score" display column; non-numeric samples stay random and unsorted.
    sorted_by_score: bool = False
    # Median BARD (|a-b| / (|a|+|b|), see `etl.data_helpers.misc.bard`) across ALL changed rows
    # of a numeric column — the typical size of the change, bounded in [0, 1]. None when the
    # column is non-numeric (or the diff is not of kind "changed").
    median_bard: float | None = None

    @property
    def pct(self) -> float:
        return self.count / self.total * 100 if self.total else 0.0

    @property
    def symbol(self) -> str:
        return {"new": "+", "removed": "-", "changed": "~"}[self.kind]

    @property
    def severity(self) -> float:
        """How big the differences are, in [0, 1] — the report's sort key.

        Numeric changed diffs use their median BARD; non-numeric changes count as a full change
        (a category flip has no meaningful "size"); removed values scale with the share of rows
        lost (coverage loss is additionally surfaced by the dataset's coverage chip, which forces
        the 🔴 tier); purely new values sort last — they're additions, not differences.
        """
        if self.kind == "new":
            return 0.0
        if self.kind == "removed":
            return self.pct / 100
        if self.median_bard is not None:
            return self.median_bard
        return 1.0


@dataclass
class ColumnDiffResult:
    name: str
    kind: ChangeKind
    is_dim: bool = False
    # Subset of ["changed metadata", "new data", "changed data"].
    changes: list[str] = field(default_factory=list)
    meta_diff: str = ""
    value_diffs: list[ValueDiff] = field(default_factory=list)

    @property
    def severity(self) -> float:
        """Biggest change among the column's value diffs; removed columns are maximal, new ones
        minimal. Metadata-only changes score 0 — they are not anomalies, so they get no tier and
        no score chip (they surface as "metadata-only" instead)."""
        if self.kind == "removed":
            return 1.0
        if self.kind == "new":
            return 0.0
        if self.value_diffs:
            return max(v.severity for v in self.value_diffs)
        return 0.0

    @property
    def is_metadata_only(self) -> bool:
        """A changed column whose diff carries no value changes at all — only metadata edits."""
        return self.kind == "changed" and not self.value_diffs


@dataclass
class TableDiffResult:
    name: str
    # "changed" means the table-level metadata changed; column changes live in `columns`.
    kind: ChangeKind
    meta_diff: str = ""
    columns: list[ColumnDiffResult] = field(default_factory=list)

    @property
    def changed_columns(self) -> list[ColumnDiffResult]:
        return [c for c in self.columns if c.kind != "identical"]

    @property
    def any_change(self) -> bool:
        return self.kind != "identical" or bool(self.changed_columns)

    @property
    def severity(self) -> float:
        """A table is as critical as its most-changed column (metadata-only changes score 0)."""
        return max((c.severity for c in self.columns), default=0.0)

    @property
    def removed_row_count(self) -> int:
        """Rows present before and gone after (index-level removals; coverage loss)."""
        # All dims of a table share the same removed index rows — the first dim suffices.
        for c in self.columns:
            if c.is_dim:
                for v in c.value_diffs:
                    if v.kind == "removed":
                        return v.count
        return 0

    @property
    def removed_labels(self) -> list[str]:
        """Entity/dim labels of removed rows (from the stored sample, so possibly a subset)."""
        for c in self.columns:
            if c.is_dim:
                for v in c.value_diffs:
                    if v.kind == "removed":
                        seen: list[str] = []
                        for row in v.sample:
                            label = row.get(c.name, "")
                            if label and label not in seen:
                                seen.append(label)
                        return seen
        return []


@dataclass
class DatasetDiffResult:
    path: str
    # "changed" means the dataset-level metadata changed; table changes live in `tables`.
    kind: ChangeKind
    is_new_version: bool = False
    meta_diff: str = ""
    tables: list[TableDiffResult] = field(default_factory=list)
    error: str | None = None

    @property
    def any_change(self) -> bool:
        return self.kind != "identical" or any(t.any_change for t in self.tables)

    @property
    def change_kind(self) -> ChangeKind:
        """Overall classification of the dataset for summary counts."""
        if self.error:
            return "error"
        if self.kind in ("new", "removed"):
            return self.kind
        return "changed" if self.any_change else "identical"

    @property
    def severity(self) -> float:
        """A dataset is as critical as its most-changed table.

        Comparison errors rank above everything (they hide unknown changes), removed datasets
        above any value change (data loss), and brand-new datasets low (additions, self-evident
        in a version bump).
        """
        if self.change_kind == "error":
            return 3.0
        if self.change_kind == "removed":
            return 2.0
        if self.change_kind == "new":
            return 0.005
        return max((t.severity for t in self.tables), default=0.0)

    @property
    def removed_row_count(self) -> int:
        return sum(t.removed_row_count for t in self.tables)

    @property
    def removed_labels(self) -> list[str]:
        seen: list[str] = []
        for t in self.tables:
            for label in t.removed_labels:
                if label not in seen:
                    seen.append(label)
        return seen

    @property
    def has_coverage_loss(self) -> bool:
        """Rows/entities, columns or whole tables that existed before and are gone after."""
        return (
            self.removed_row_count > 0
            or any(t.kind == "removed" for t in self.tables)
            or any(c.kind == "removed" for t in self.tables for c in t.columns)
        )

    @property
    def tier(self) -> Tier:
        """Reviewer triage tier. Coverage loss always makes a dataset 🔴 — a disappearing
        entity/series is the classic silent breakage of a dependency update, regardless of how
        small it is relative to the dataset."""
        if self.change_kind in ("error", "removed"):
            return "large"
        if self.change_kind == "identical":
            return "none"
        if self.has_coverage_loss:
            return "large"
        if self.change_kind == "new":
            return "small"
        return _tier(self.severity)

    @property
    def is_metadata_only(self) -> bool:
        """A changed dataset whose diff is purely metadata edits: no value changes anywhere and
        no structural changes (added/removed tables or columns) either.

        Stricter than `tier == "none"`: a dataset whose only change is *added* values also scores
        0 (additions aren't anomalies) but is not metadata-only. Structural changes matter too —
        a removed table/column is coverage loss (🔴), and classifying it "meta" would let the
        tier filter hide it.
        """
        if self.change_kind != "changed":
            return False
        for t in self.tables:
            if t.kind in ("new", "removed"):
                return False
            for c in t.columns:
                if c.kind in ("new", "removed") or c.value_diffs:
                    return False
        return True


@dataclass
class DiffReport:
    datasets: list[DatasetDiffResult] = field(default_factory=list)
    # Datasets skipped because their data files are byte-identical (source_checksum cascade).
    skipped_cascade: int = 0

    def _count(self, kind: ChangeKind) -> int:
        return sum(1 for ds in self.datasets if ds.change_kind == kind)

    @property
    def n_changed(self) -> int:
        return self._count("changed")

    @property
    def n_new(self) -> int:
        return self._count("new")

    @property
    def n_removed(self) -> int:
        return self._count("removed")

    @property
    def n_identical(self) -> int:
        return self._count("identical")

    @property
    def n_errors(self) -> int:
        return self._count("error")

    @property
    def status(self) -> Literal["clean", "changed", "error"]:
        if self.n_errors:
            return "error"
        if self.n_changed or self.n_new or self.n_removed:
            return "changed"
        return "clean"

    def to_json(self) -> str:
        return json.dumps(
            {"skipped_cascade": self.skipped_cascade, "datasets": [asdict(ds) for ds in self.datasets]},
            indent=1,
            default=str,
        )

    @classmethod
    def from_json(cls, s: str) -> "DiffReport":
        d = json.loads(s)
        return cls(
            datasets=[_dataset_from_dict(ds) for ds in d["datasets"]],
            skipped_cascade=d["skipped_cascade"],
        )


def _dataset_from_dict(d: dict[str, Any]) -> DatasetDiffResult:
    return DatasetDiffResult(
        path=d["path"],
        kind=d["kind"],
        is_new_version=d["is_new_version"],
        meta_diff=d["meta_diff"],
        error=d["error"],
        tables=[
            TableDiffResult(
                name=t["name"],
                kind=t["kind"],
                meta_diff=t["meta_diff"],
                columns=[
                    ColumnDiffResult(
                        name=c["name"],
                        kind=c["kind"],
                        is_dim=c["is_dim"],
                        changes=c["changes"],
                        meta_diff=c["meta_diff"],
                        value_diffs=[ValueDiff(**v) for v in c["value_diffs"]],
                    )
                    for c in t["columns"]
                ],
            )
            for t in d["tables"]
        ],
    )


########################################################################################################################
# HTML report
########################################################################################################################

_SYMBOLS = {"new": "+", "removed": "-", "changed": "~", "identical": "=", "error": "⚠"}


def _headline(report: "DiffReport") -> str:
    """Status headline. All counts in this report are about datasets — say so explicitly."""
    n = len(report.datasets)
    total = f"{n:,} compared dataset{'s' if n != 1 else ''}"
    if report.status == "error":
        return f"⚠ {report.n_errors:,} of {total} failed to compare"
    if report.status == "changed":
        n_diff = report.n_changed + report.n_new + report.n_removed
        return f"❌ Found differences in {n_diff:,} of {total}"
    return f"✅ No differences found across {total}"


def _e(s: Any) -> str:
    return html.escape(str(s))


def _anchor(ds_path: str, table: str, col: str) -> str:
    """Stable element id for a column's detail block, so the Top changes list can link to it."""
    return "c-" + re.sub(r"[^a-z0-9]+", "-", f"{ds_path}-{table}-{col}".lower()).strip("-")


def _ds_anchor(ds_path: str) -> str:
    """Stable element id for a dataset's detail block, so the Datasets watch list can link to it."""
    return "d-" + re.sub(r"[^a-z0-9]+", "-", ds_path.lower()).strip("-")


def dataset_watch_key(ds: DatasetDiffResult) -> tuple:
    """Triage sort key for dataset summaries (watch list, owidbot PR comment): tier first, and
    within a tier data loss (or a failed/removed dataset) outranks a bigger-but-benign change;
    then change size. Plain severity ordering would let a truncated summary drop a small-but-lossy
    dataset below big value changes — defeating the "data loss is unmissable" signal."""
    tier_rank = {"large": 0, "moderate": 1, "small": 2, "none": 3}
    lossy_first = 0 if (ds.change_kind in ("error", "removed") or ds.has_coverage_loss) else 1
    return (tier_rank[ds.tier], lossy_first, -ds.severity, ds.path)


def _tier_chip(severity: float, kind: ChangeKind = "changed", tier: Tier | None = None) -> str:
    """Colored triage chip: tier icon + the score it corresponds to.

    Pass `tier` to override the severity-derived tier — a dataset with coverage loss is forced
    🔴 by `DatasetDiffResult.tier`, and its chip must agree with the tier strip and filters.
    """
    tier = tier or _tier(severity)
    if tier == "none":
        return ""
    label = {"removed": "removed", "new": "new"}.get(kind) or f"median anomaly score {format_score(severity)}"
    return f'<span class="chip tier {tier}">{TIER_ICONS[tier]} {_e(label)}</span>'


def _coverage_chip(ds: DatasetDiffResult) -> str:
    """Coverage-loss chip: entities/rows that existed before and are gone after."""
    if not ds.has_coverage_loss:
        return ""
    bits = []
    if ds.removed_row_count:
        labels = ds.removed_labels[:4]
        shown = ", ".join(labels)
        more = "…" if len(ds.removed_labels) > len(labels) else ""
        bits.append(f"{ds.removed_row_count:,} row(s) removed" + (f": {shown}{more}" if shown else ""))
    n_removed_cols = sum(1 for t in ds.tables for c in t.columns if c.kind == "removed")
    if n_removed_cols:
        bits.append(f"{n_removed_cols} column(s) removed")
    n_removed_tables = sum(1 for t in ds.tables if t.kind == "removed")
    if n_removed_tables:
        bits.append(f"{n_removed_tables} table(s) removed")
    return f'<span class="chip cov">− {_e(" · ".join(bits))}</span>'


def _top_changes(
    report: "DiffReport", limit: int = TOP_CHANGES_LIMIT
) -> tuple[list[tuple[int, list[str], str, str, str | None]], list[tuple[float, float, str, str, str]]]:
    """The indicators to watch, as (losses, changes).

    Losses — (n_rows_removed, labels, ds_path, table, dim_or_None) — are data points that existed
    before and are gone after, the classic silent breakage of a dependency update. They always
    lead the watch list, regardless of how small they are relative to the dataset.

    Changes — (severity, pct_rows, ds_path, table, column) — are the biggest value changes.
    """
    losses = []
    changes = []
    for ds in report.datasets:
        if ds.change_kind != "changed":
            continue
        for t in ds.tables:
            if t.removed_row_count:
                # Link to the dim column whose detail block holds the removed-rows sample.
                dim = next(
                    (c.name for c in t.columns if c.is_dim and any(v.kind == "removed" for v in c.value_diffs)),
                    None,
                )
                losses.append((t.removed_row_count, t.removed_labels, ds.path, t.name, dim))
            for c in t.columns:
                if c.is_dim or c.kind == "identical" or c.severity <= 0:
                    continue
                pct = max((v.pct for v in c.value_diffs if v.kind != "new"), default=0.0)
                changes.append((c.severity, pct, ds.path, t.name, c.name))
    losses.sort(key=lambda r: (-r[0], r[2]))
    changes.sort(key=lambda r: (-r[0], -r[1], r[2]))
    losses = losses[:limit]
    return losses, changes[: max(0, limit - len(losses))]


def _expandable_list(items: list[str], visible: int) -> str:
    """An <ol> of rendered <li> strings; entries beyond `visible` hide behind a show-more toggle."""
    if len(items) <= visible:
        return f"<ol>{''.join(items)}</ol>"

    def _mark_extra(li: str) -> str:
        if li.startswith('<li class="'):
            return li.replace('<li class="', '<li class="extra ', 1)
        return li.replace("<li>", '<li class="extra">', 1)

    lis = "".join(items[:visible]) + "".join(_mark_extra(li) for li in items[visible:])
    n_extra = len(items) - visible
    return f'<ol>{lis}</ol><button class="show-more" data-more="{n_extra}">show {n_extra} more</button>'


def _render_meta_diff(meta_diff: str, label: str) -> str:
    """Render an ndiff metadata diff as a collapsible block with +/- lines colored."""
    if not meta_diff:
        return ""
    lines = []
    for line in meta_diff.splitlines():
        cls = "add" if line.startswith("+") else "del" if line.startswith("-") else ""
        lines.append(f'<span class="{cls}">{_e(line)}</span>')
    return f'<details class="meta"><summary>{_e(label)}</summary><pre>{chr(10).join(lines)}</pre></details>'


def _render_value_diff(v: ValueDiff) -> str:
    label = {"new": "New values", "removed": "Removed values", "changed": "Changed values"}[v.kind]
    # Say up front that the table is a sample — a note below a 100-row scrollable table is only
    # discovered after scrolling past rows the reader didn't know were truncated.
    head_note = ""
    if v.count > len(v.sample):
        what = (
            f"the {len(v.sample):,} most anomalous rows"
            if v.sorted_by_score
            else f"a random sample of {len(v.sample):,} rows"
        )
        head_note = f' <span class="head-note">— showing {what}</span>'
    head = (
        f'<div class="vd-head {v.kind}">{_e(v.symbol)} {label}: '
        f"{v.count:,} / {v.total:,} ({v.pct:.2f}%){head_note}</div>"
    )
    if not v.sample:
        return f'<div class="vd">{head}</div>'

    cols = list(v.sample[0].keys())
    ths = "".join(
        f'<th class="{"old" if c.endswith(" -") else "new" if c.endswith(" +") else ""}">{_e(c)}</th>' for c in cols
    )
    trs = []
    for row in v.sample:
        tds = "".join(
            f'<td class="{"old" if c.endswith(" -") else "new" if c.endswith(" +") else ""}">{_e(row.get(c, ""))}</td>'
            for c in cols
        )
        trs.append(f"<tr>{tds}</tr>")
    return (
        f'<div class="vd">{head}<div class="table-wrap"><table><thead><tr>{ths}</tr></thead>'
        f"<tbody>{''.join(trs)}</tbody></table></div></div>"
    )


def _render_column(table_name: str, c: ColumnDiffResult, ds_path: str = "") -> str:
    chips = "".join(f'<span class="chip">{_e(ch)}</span>' for ch in c.changes)
    dim = '<span class="chip dim">dim</span>' if c.is_dim else ""
    tier = _tier_chip(c.severity, c.kind) if not c.is_dim else ""
    anchor = f' id="{_anchor(ds_path, table_name, c.name)}"' if ds_path else ""
    # Dims are navigation context, not indicators — the indicator tier filter skips them.
    # Metadata-only columns get their own filterable category ("meta") instead of a tier.
    tier_attr = "" if c.is_dim else f' data-tier="{"meta" if c.is_metadata_only else _tier(c.severity)}"'
    parts = [
        f'<div class="col {c.kind}"{anchor}{tier_attr}>'
        f'<div class="col-head"><span class="sym {c.kind}">{_SYMBOLS[c.kind]}</span> '
        f"<code>{_e(table_name)}.{_e(c.name)}</code> {dim}{tier}{chips}</div>"
    ]
    if c.meta_diff:
        parts.append(_render_meta_diff(c.meta_diff, "metadata diff"))
    for v in c.value_diffs:
        parts.append(_render_value_diff(v))
    parts.append("</div>")
    return "".join(parts)


def _render_table(t: TableDiffResult, ds_path: str = "") -> str:
    parts = [
        f'<div class="tbl"><div class="tbl-head"><span class="sym {t.kind}">{_SYMBOLS[t.kind]}</span> '
        f"Table <b>{_e(t.name)}</b>{_tier_chip(t.severity, t.kind)}</div>"
    ]
    if t.meta_diff:
        parts.append(_render_meta_diff(t.meta_diff, "table metadata diff"))
    # Most-changed columns first (stable: ties keep collection order).
    for c in sorted(t.columns, key=lambda c: -c.severity):
        if c.kind == "identical":
            continue
        parts.append(_render_column(t.name, c, ds_path))
    parts.append("</div>")
    return "".join(parts)


def _dataset_summary_note(ds: DatasetDiffResult) -> str:
    """Short human summary shown next to the dataset path."""
    if ds.error:
        return "error"
    if ds.kind in ("new", "removed"):
        return f"{ds.kind} dataset"
    notes = []
    if ds.meta_diff:
        notes.append("dataset metadata")
    n_cols = sum(len(t.changed_columns) for t in ds.tables)
    n_tables = sum(1 for t in ds.tables if t.any_change)
    if n_cols:
        notes.append(f"{n_cols} column{'s' if n_cols != 1 else ''} in {n_tables} table{'s' if n_tables != 1 else ''}")
    elif n_tables:
        notes.append(f"{n_tables} table{'s' if n_tables != 1 else ''} metadata")
    return ", ".join(notes) if notes else "identical"


def _render_dataset(ds: DatasetDiffResult) -> str:
    kind = ds.change_kind
    open_attr = " open" if kind in ("changed", "error") else ""
    new_version = ' <span class="chip">new version</span>' if ds.is_new_version else ""
    tier = _tier_chip(ds.severity, kind, tier=ds.tier) if kind == "changed" else ""
    # What the filter box matches against: names only (path, tables, changed columns) — matching
    # the full text would make queries hit sample values and scores, which is pure noise.
    search = " ".join(
        [ds.path] + [t.name for t in ds.tables] + [c.name for t in ds.tables for c in t.changed_columns]
    ).lower()
    parts = [
        f'<details class="ds {kind}" id="{_ds_anchor(ds.path)}" data-tier="{"meta" if ds.is_metadata_only else ds.tier}" data-search="{_e(search)}"{open_attr}>'
        f'<summary><span class="sym {kind}">{_SYMBOLS[kind]}</span> '
        f'<code class="path">{_e(ds.path)}</code>{new_version}{tier}{_coverage_chip(ds)}'
        f'<span class="ds-note">{_e(_dataset_summary_note(ds))}</span></summary>'
        f'<div class="ds-body">'
    ]
    if ds.error:
        parts.append(f'<div class="error-msg">⚠ {_e(ds.error)}</div>')
    if ds.meta_diff:
        parts.append(_render_meta_diff(ds.meta_diff, "dataset metadata diff"))
    # Most-changed tables first (stable: ties keep collection order).
    for t in sorted(ds.tables, key=lambda t: -t.severity):
        if t.any_change or ds.kind in ("new", "removed"):
            parts.append(_render_table(t, ds.path))
    parts.append("</div></details>")
    return "".join(parts)


def render_html(report: DiffReport) -> str:
    """Render the report as a single self-contained HTML page."""
    generated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    cards = []
    for label, count, kind in [
        ("changed", report.n_changed, "changed"),
        ("new", report.n_new, "new"),
        ("removed", report.n_removed, "removed"),
        ("errors", report.n_errors, "error"),
        ("identical", report.n_identical, "identical"),
        ("skipped", report.skipped_cascade, "skipped"),
    ]:
        if count or kind in ("changed", "identical", "skipped"):
            cards.append(
                f'<div class="card {kind}"><div class="count">{count:,}</div><div class="label">{label}</div></div>'
            )

    # Biggest differences first: comparison errors, then removed datasets (data loss), then
    # changed datasets ranked by the size of their changes (median BARD of the most-changed
    # column), then new datasets; identical last. Path as tie-break for determinism.
    datasets = sorted(report.datasets, key=lambda ds: (-ds.severity, ds.path))
    sections = "".join(_render_dataset(ds) for ds in datasets)

    tier_counts = {"large": 0, "moderate": 0, "small": 0}
    for ds in report.datasets:
        if ds.change_kind != "identical" and ds.tier != "none":
            tier_counts[ds.tier] += 1

    # The tier dropdown offers only tiers that exist in the report (with their counts), and is
    # dropped entirely when there are fewer than two to tell apart — a filter with one choice
    # is dead weight.
    n_meta_only_datasets = sum(1 for ds in report.datasets if ds.is_metadata_only)
    available_tiers = [t for t in ("large", "moderate", "small") if tier_counts[t]]
    if len(available_tiers) + (1 if n_meta_only_datasets else 0) >= 2:
        opts = "".join(f'<option value="{t}">{TIER_ICONS[t]} {t} ({tier_counts[t]})</option>' for t in available_tiers)
        if n_meta_only_datasets:
            opts += f'<option value="meta">📝 metadata-only ({n_meta_only_datasets})</option>'
        tier_select = f'<select id="tier-filter"><option value="all">all dataset tiers</option>{opts}</select>'
    else:
        tier_select = ""

    # Same for indicators: a dropdown filtering the column blocks by their tier (dims excluded).
    col_tier_counts = {"large": 0, "moderate": 0, "small": 0}
    n_meta_only_cols = 0
    for ds in report.datasets:
        if ds.change_kind != "changed":
            continue
        for t in ds.tables:
            for c in t.columns:
                if c.is_dim or c.kind == "identical":
                    continue
                if c.is_metadata_only:
                    n_meta_only_cols += 1
                elif _tier(c.severity) != "none":
                    col_tier_counts[_tier(c.severity)] += 1
    available_col_tiers = [t for t in ("large", "moderate", "small") if col_tier_counts[t]]
    if len(available_col_tiers) + (1 if n_meta_only_cols else 0) >= 2:
        opts = "".join(
            f'<option value="{t}">{TIER_ICONS[t]} {t} ({col_tier_counts[t]})</option>' for t in available_col_tiers
        )
        if n_meta_only_cols:
            opts += f'<option value="meta">📝 metadata-only ({n_meta_only_cols})</option>'
        ind_tier_select = (
            f'<select id="ind-tier-filter"><option value="all">all indicator tiers</option>{opts}</select>'
        )
    else:
        ind_tier_select = ""

    # Triage aids — only when the report is big enough to need them.
    tier_strip = ""
    top_block = ""
    if report.n_changed >= TRIAGE_MIN_DATASETS:
        strip_bits = [f"{TIER_ICONS[t]} {n} {t}" for t, n in tier_counts.items() if n]
        # Datasets whose only differences are metadata edits carry no anomaly tier — list them
        # separately so the strip total still matches the headline's differing-dataset count.
        # (A changed dataset can also score 0 from purely *added* values; that gets its own bucket.)
        n_meta_only = sum(1 for ds in report.datasets if ds.is_metadata_only)
        if n_meta_only:
            strip_bits.append(f"📝 {n_meta_only} metadata-only")
        n_new_only = sum(
            1 for ds in report.datasets if ds.change_kind == "changed" and ds.tier == "none" and not ds.is_metadata_only
        )
        if n_new_only:
            strip_bits.append(f"➕ {n_new_only} new-data-only")
        n_diff = sum(tier_counts.values()) + n_meta_only + n_new_only
        if strip_bits and n_diff:
            tier_strip = (
                f'<div class="tier-strip">Of the {n_diff:,} dataset{"s" if n_diff != 1 else ""} with '
                f"differences, the changes are: {' · '.join(strip_bits)} "
                f'<span class="tier-hint">(tiered by median anomaly score; coverage loss ⇒ 🔴)</span></div>'
            )

        # Datasets to watch, in triage order (see dataset_watch_key).
        watch = sorted((d for d in datasets if d.change_kind in ("changed", "error", "removed")), key=dataset_watch_key)
        ds_items = []
        for d in watch[:TOP_DATASETS_MAX]:
            # Red is reserved for the alarming part only: the data-loss fragment (or a dataset
            # that failed to compare / was removed) — not the whole meta line.
            if d.change_kind == "error":
                meta_html = '<span class="top-meta loss">failed to compare</span>'
            elif d.change_kind == "removed":
                meta_html = '<span class="top-meta loss">removed dataset</span>'
            elif d.is_metadata_only:
                meta_html = '<span class="top-meta">metadata-only changes</span>'
            elif d.severity <= 0:
                meta_html = '<span class="top-meta">new data only</span>'
            else:
                n_cols = sum(len(t.changed_columns) for t in d.tables)
                n_tables = sum(1 for t in d.tables if t.any_change)
                meta_html = f'<span class="top-meta">median anomaly score {_e(format_score(d.severity))}</span>'
                if d.removed_row_count:
                    meta_html += f'<span class="top-meta loss">− lost {d.removed_row_count:,} data point(s)</span>'
                meta_html += f'<span class="top-meta">{n_cols} column(s) in {n_tables} table(s)</span>'
            icon = TIER_ICONS.get(d.tier) or ("📝" if d.is_metadata_only else "")
            ds_items.append(
                f'<li><span class="ti">{icon}</span> '
                f'<a href="#{_ds_anchor(d.path)}"><code>{_e(d.path)}</code></a>{meta_html}</li>'
            )

        losses, changes = _top_changes(report, limit=TOP_CHANGES_MAX)
        items = []
        # Data-point losses lead the indicators list — make it unmistakable that rows disappeared.
        for n_removed, labels, ds_path, table, dim in losses:
            shown = ", ".join(labels[:4]) + ("…" if len(labels) > 4 else "")
            link_open = f'<a href="#{_anchor(ds_path, table, dim)}">' if dim else ""
            link_close = "</a>" if dim else ""
            items.append(
                f'<li class="loss"><span class="ti">🔴</span> '
                f"{link_open}<code>{_e(ds_path)}</code> · <code>{_e(table)}</code>{link_close}"
                f'<span class="top-meta loss">− lost {n_removed:,} data point(s)'
                + (f": {_e(shown)}" if shown else "")
                + "</span></li>"
            )
        for severity, pct, ds_path, table, col in changes:
            tier = _tier(severity)
            items.append(
                f'<li><span class="ti">{TIER_ICONS.get(tier, "")}</span> '
                f'<a href="#{_anchor(ds_path, table, col)}"><code>{_e(ds_path)}</code> · <code>{_e(table)}.{_e(col)}</code></a>'
                f'<span class="top-meta">median anomaly score {_e(format_score(severity))} · {pct:.0f}% of rows</span></li>'
            )
        if ds_items or items:
            parts = ['<details class="top-changes" open><summary><b>Top changes — what to watch</b></summary>']
            if ds_items:
                parts.append(f"<div class='tc-h'>Datasets</div>{_expandable_list(ds_items, TOP_DATASETS_LIMIT)}")
            if items:
                parts.append(f"<div class='tc-h'>Indicators</div>{_expandable_list(items, TOP_CHANGES_LIMIT)}")
            parts.append("</details>")
            top_block = "".join(parts)

    skipped_note = (
        f'<div class="skipped-note">= {report.skipped_cascade:,} more dataset(s) skipped: '
        "their data files are byte-identical, the source checksum changed only because an upstream "
        "metadata change cascaded.</div>"
        if report.skipped_cascade
        else ""
    )

    # Identical datasets render at the bottom (severity 0) and are hidden by default; carry the
    # count in the label so the toggle's effect is discoverable, and drop it when there's nothing
    # to show.
    identical_toggle = (
        f'<label><input type="checkbox" id="show-identical"> '
        f"show {report.n_identical:,} identical dataset{'s' if report.n_identical != 1 else ''}</label>"
        if report.n_identical
        else ""
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>data-diff</title>
<style>
  :root {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  body {{ margin: 0; padding: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  .sub {{ color: #777; font-size: .85rem; margin-bottom: 1.5rem; }}
  .cards {{ display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
  .card {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: .75rem 1.25rem; text-align: center; min-width: 90px; }}
  .card .count {{ font-size: 1.6rem; font-weight: 700; }}
  .card.changed .count {{ color: #b58900; }}
  .card.new .count {{ color: #2e7d32; }}
  .card.removed .count {{ color: #c62828; }}
  .card.error .count {{ color: #c62828; }}
  .card.identical .count, .card.skipped .count {{ color: #999; }}
  .card .label {{ font-size: .75rem; color: #777; text-transform: uppercase; letter-spacing: .04em; }}
  .controls {{ display: flex; gap: 1.5rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; }}
  input[type="search"] {{ width: 100%; max-width: 420px; padding: .5rem .75rem; border: 1px solid #ccc; border-radius: 6px; font-size: .9rem; }}
  .controls label {{ font-size: .85rem; color: #444; user-select: none; }}
  details.ds {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; margin-bottom: .75rem; }}
  details.ds > summary {{ padding: .7rem 1rem; cursor: pointer; display: flex; align-items: center; gap: .6rem; flex-wrap: wrap; }}
  details.ds > summary:hover {{ background: #fcfcff; }}
  details.ds.identical {{ display: none; }}
  body.show-identical details.ds.identical {{ display: block; }}
  details.ds.identical.match-visible {{ display: block; }}
  details.ds.hidden {{ display: none !important; }}
  div.col.hidden {{ display: none; }}
  select {{ padding: .45rem .5rem; border: 1px solid #ccc; border-radius: 6px; font-size: .85rem; background: #fff; }}
  .match-count {{ color: #999; font-size: .8rem; margin-left: auto; }}
  .ds-body {{ padding: .25rem 1rem 1rem; border-top: 1px solid #f0f0f0; }}
  .ds-note {{ margin-left: auto; color: #999; font-size: .8rem; }}
  code {{ font-size: .82rem; background: #f3f3f3; padding: .1rem .3rem; border-radius: 3px; }}
  code.path {{ font-weight: 600; }}
  .sym {{ font-family: ui-monospace, Menlo, monospace; font-weight: 700; width: 1rem; display: inline-block; text-align: center; }}
  .sym.changed {{ color: #b58900; }}
  .sym.new {{ color: #2e7d32; }}
  .sym.removed {{ color: #c62828; }}
  .sym.identical {{ color: #bbb; }}
  .sym.error {{ color: #c62828; }}
  .chip {{ font-size: .7rem; background: #fdf3d7; color: #8a6d00; border-radius: 10px; padding: .1rem .5rem; margin-left: .3rem; white-space: nowrap; }}
  .chip.dim {{ background: #e8eaf6; color: #3949ab; }}
  .chip.tier.large {{ background: #fdeaea; color: #b71c1c; }}
  .chip.tier.moderate {{ background: #fdf3d7; color: #8a6d00; }}
  .chip.tier.small {{ background: #e9f6ec; color: #1b5e20; }}
  .chip.cov {{ background: #fdeaea; color: #b71c1c; font-weight: 600; }}
  .tier-strip {{ font-size: .9rem; margin: -0.75rem 0 1rem; }}
  .tier-strip .tier-hint {{ color: #999; font-size: .78rem; }}
  details.top-changes {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: .6rem 1rem; margin-bottom: 1rem; }}
  details.top-changes > summary {{ cursor: pointer; font-size: .95rem; }}
  details.top-changes ol {{ margin: .5rem 0 .25rem; padding-left: 1.5rem; }}
  details.top-changes li {{ font-size: .85rem; margin: .25rem 0; }}
  details.top-changes a {{ text-decoration: none; color: inherit; }}
  details.top-changes a:hover code {{ background: #eef; }}
  .top-meta {{ color: #888; font-size: .78rem; margin-left: .5rem; }}
  .top-meta.loss {{ color: #b71c1c; font-weight: 600; }}
  .ti {{ margin-right: .15rem; }}
  .tc-h {{ font-size: .72rem; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; color: #999; margin: .6rem 0 .1rem; }}
  .top-changes li.extra {{ display: none; }}
  .top-changes ol.expanded li.extra {{ display: list-item; }}
  .show-more {{ background: none; border: none; color: #3949ab; font-size: .8rem; cursor: pointer; padding: 0 0 .2rem 1.5rem; }}
  .show-more:hover {{ text-decoration: underline; }}
  .cards-caption {{ color: #aaa; font-size: .72rem; margin: -1.25rem 0 1.25rem; }}
  .tbl {{ margin: .75rem 0 0; }}
  .tbl-head {{ font-size: .9rem; margin-bottom: .25rem; }}
  .col {{ margin: .5rem 0 .5rem 1.5rem; }}
  .col-head {{ font-size: .85rem; margin-bottom: .25rem; }}
  details.meta {{ margin: .3rem 0 .3rem 1.5rem; }}
  details.meta > summary {{ font-size: .78rem; color: #777; cursor: pointer; }}
  details.meta pre {{ background: #fbfbfd; border: 1px solid #f0f0f0; border-radius: 6px; padding: .6rem .8rem; font-size: .75rem; overflow-x: auto; }}
  details.meta pre .add {{ color: #2e7d32; }}
  details.meta pre .del {{ color: #c62828; }}
  .vd {{ margin: .4rem 0 .6rem 1.5rem; }}
  .vd-head {{ font-size: .8rem; font-weight: 600; margin-bottom: .3rem; }}
  .vd-head.new {{ color: #2e7d32; }}
  .vd-head.removed {{ color: #c62828; }}
  .vd-head.changed {{ color: #8a6d00; }}
  .vd-head .head-note {{ color: #888; font-weight: 400; font-size: .75rem; }}
  .table-wrap {{ overflow-x: auto; max-height: 420px; overflow-y: auto; border: 1px solid #eee; border-radius: 6px; display: inline-block; max-width: 100%; }}
  .vd table {{ border-collapse: collapse; font-size: .78rem; }}
  .vd th, .vd td {{ padding: .3rem .6rem; border-bottom: 1px solid #f0f0f0; text-align: left; white-space: nowrap; }}
  .vd th {{ position: sticky; top: 0; background: #f7f7f7; font-weight: 600; }}
  .vd th.old, .vd td.old {{ background: #fdeaea; }}
  .vd th.new, .vd td.new {{ background: #e9f6ec; }}
  .note {{ color: #888; font-size: .74rem; margin-top: .25rem; }}
  .error-msg {{ color: #c62828; font-size: .85rem; margin: .5rem 0; }}
  .skipped-note {{ color: #999; font-size: .82rem; margin-top: 1rem; }}
  .headline {{ font-size: 1rem; font-weight: 600; margin-bottom: 1rem; }}
</style>
</head>
<body>
  <h1>data-diff</h1>
  <div class="sub">generated {generated_at} · <code>etl diff</code> report</div>
  <div class="headline">{_headline(report)}</div>
  <div class="cards">{"".join(cards)}</div>
  <div class="cards-caption">counts are datasets</div>
  {tier_strip}
  {top_block}
  <div class="controls">
    <input type="search" id="filter" placeholder="Filter datasets (path, table, column…)">
    {tier_select}
    {ind_tier_select}
    {identical_toggle}
    <span class="match-count" id="match-count"></span>
  </div>
  {sections}
  {skipped_note}
<script>
  const dsBlocks = Array.from(document.querySelectorAll('details.ds'));
  const filterInput = document.getElementById('filter');
  const tierSelect = document.getElementById('tier-filter');
  const indTierSelect = document.getElementById('ind-tier-filter');
  const colBlocks = Array.from(document.querySelectorAll('div.col[data-tier]'));
  const showIdentical = document.getElementById('show-identical');
  const matchCount = document.getElementById('match-count');

  function applyFilters() {{
    // Multi-term AND over names only (path + table + changed-column names, via data-search).
    const terms = filterInput.value.toLowerCase().split(/\\s+/).filter(Boolean);
    const tier = tierSelect ? tierSelect.value : 'all';
    const filtering = terms.length > 0 || tier !== 'all';
    let shown = 0;
    dsBlocks.forEach(d => {{
      const hay = d.dataset.search || '';
      const match = terms.every(t => hay.includes(t)) && (tier === 'all' || d.dataset.tier === tier);
      d.classList.toggle('hidden', !match);
      // An active filter reveals matching identical datasets even when the toggle is off —
      // searching for a dataset you know was compared should always find it.
      d.classList.toggle('match-visible', match && filtering);
      const visible = match && (!d.classList.contains('identical') || (showIdentical && showIdentical.checked) || filtering);
      if (visible) shown++;
    }});
    matchCount.textContent = `${{shown}} of ${{dsBlocks.length}} datasets shown`;
  }}

  if (showIdentical) {{
    showIdentical.addEventListener('change', (e) => {{
      document.body.classList.toggle('show-identical', e.target.checked);
      applyFilters();
    }});
  }}
  filterInput.addEventListener('input', applyFilters);
  if (tierSelect) tierSelect.addEventListener('change', applyFilters);
  if (indTierSelect) {{
    indTierSelect.addEventListener('change', () => {{
      const t = indTierSelect.value;
      colBlocks.forEach(c => c.classList.toggle('hidden', t !== 'all' && c.dataset.tier !== t));
    }});
  }}
  document.querySelectorAll('.show-more').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const ol = btn.previousElementSibling;
      const expanded = ol.classList.toggle('expanded');
      btn.textContent = expanded ? 'show fewer' : `show ${{btn.dataset.more}} more`;
    }});
  }});
  applyFilters();
</script>
</body>
</html>
"""
