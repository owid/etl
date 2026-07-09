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
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ChangeKind = Literal["new", "removed", "changed", "identical", "error"]

# Number of sample rows stored per value diff (the text output shows only 5; the HTML report
# can afford more).
SAMPLE_LIMIT = 100


@dataclass
class ValueDiff:
    """New, removed or changed values of a single column."""

    kind: Literal["new", "removed", "changed"]
    count: int
    total: int
    # Display-ready records; rows of a "changed" diff have "<col> -" (old) and "<col> +" (new) keys.
    sample: list[dict[str, str]] = field(default_factory=list)

    @property
    def pct(self) -> float:
        return self.count / self.total * 100 if self.total else 0.0

    @property
    def symbol(self) -> str:
        return {"new": "+", "removed": "-", "changed": "~"}[self.kind]


@dataclass
class ColumnDiffResult:
    name: str
    kind: ChangeKind
    is_dim: bool = False
    # Subset of ["changed metadata", "new data", "changed data"].
    changes: list[str] = field(default_factory=list)
    meta_diff: str = ""
    value_diffs: list[ValueDiff] = field(default_factory=list)


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

_STATUS_HEADLINE = {
    "clean": "✅ No differences found",
    "changed": "❌ Found differences",
    "error": "⚠ Found errors",
}


def _e(s: Any) -> str:
    return html.escape(str(s))


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
    head = f'<div class="vd-head {v.kind}">{_e(v.symbol)} {label}: {v.count:,} / {v.total:,} ({v.pct:.2f}%)</div>'
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
    note = f'<div class="note">showing {len(v.sample):,} of {v.count:,} rows</div>' if v.count > len(v.sample) else ""
    return (
        f'<div class="vd">{head}<div class="table-wrap"><table><thead><tr>{ths}</tr></thead>'
        f"<tbody>{''.join(trs)}</tbody></table></div>{note}</div>"
    )


def _render_column(table_name: str, c: ColumnDiffResult) -> str:
    chips = "".join(f'<span class="chip">{_e(ch)}</span>' for ch in c.changes)
    dim = '<span class="chip dim">dim</span>' if c.is_dim else ""
    parts = [
        f'<div class="col {c.kind}">'
        f'<div class="col-head"><span class="sym {c.kind}">{_SYMBOLS[c.kind]}</span> '
        f"<code>{_e(table_name)}.{_e(c.name)}</code> {dim}{chips}</div>"
    ]
    if c.meta_diff:
        parts.append(_render_meta_diff(c.meta_diff, "metadata diff"))
    for v in c.value_diffs:
        parts.append(_render_value_diff(v))
    parts.append("</div>")
    return "".join(parts)


def _render_table(t: TableDiffResult) -> str:
    parts = [
        f'<div class="tbl"><div class="tbl-head"><span class="sym {t.kind}">{_SYMBOLS[t.kind]}</span> '
        f"Table <b>{_e(t.name)}</b></div>"
    ]
    if t.meta_diff:
        parts.append(_render_meta_diff(t.meta_diff, "table metadata diff"))
    for c in t.columns:
        if c.kind == "identical":
            continue
        parts.append(_render_column(t.name, c))
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
    parts = [
        f'<details class="ds {kind}"{open_attr}>'
        f'<summary><span class="sym {kind}">{_SYMBOLS[kind]}</span> '
        f'<code class="path">{_e(ds.path)}</code>{new_version}'
        f'<span class="ds-note">{_e(_dataset_summary_note(ds))}</span></summary>'
        f'<div class="ds-body">'
    ]
    if ds.error:
        parts.append(f'<div class="error-msg">⚠ {_e(ds.error)}</div>')
    if ds.meta_diff:
        parts.append(_render_meta_diff(ds.meta_diff, "dataset metadata diff"))
    for t in ds.tables:
        if t.any_change or ds.kind in ("new", "removed"):
            parts.append(_render_table(t))
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

    # Changed/new/removed/errored datasets first, identical last.
    order = {"error": 0, "changed": 1, "new": 2, "removed": 3, "identical": 4}
    datasets = sorted(report.datasets, key=lambda ds: (order[ds.change_kind], ds.path))
    sections = "".join(_render_dataset(ds) for ds in datasets)

    skipped_note = (
        f'<div class="skipped-note">= {report.skipped_cascade:,} more dataset(s) skipped: '
        "their data files are byte-identical, the source checksum changed only because an upstream "
        "metadata change cascaded.</div>"
        if report.skipped_cascade
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
  details.ds.hidden {{ display: none !important; }}
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
  <div class="headline">{_STATUS_HEADLINE[report.status]}</div>
  <div class="cards">{"".join(cards)}</div>
  <div class="controls">
    <input type="search" id="filter" placeholder="Filter datasets (path, table, column…)">
    <label><input type="checkbox" id="show-identical"> show identical datasets</label>
  </div>
  {sections}
  {skipped_note}
<script>
  const dsBlocks = Array.from(document.querySelectorAll('details.ds'));
  document.getElementById('show-identical').addEventListener('change', (e) => {{
    document.body.classList.toggle('show-identical', e.target.checked);
  }});
  document.getElementById('filter').addEventListener('input', (e) => {{
    const q = e.target.value.toLowerCase();
    dsBlocks.forEach(d => d.classList.toggle('hidden', !d.textContent.toLowerCase().includes(q)));
  }});
</script>
</body>
</html>
"""
