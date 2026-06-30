"""CLI to inventory and visualise all `.corrections.yml` files in the repo.

Scans every step for a `.corrections.yml` sidecar, validates the entries (so this also acts as a
repo-wide lint), and renders a self-contained HTML dashboard grouped by provider and status. This is
the cross-dataset view of known upstream data errors and what we've reported to whom.

    etl corrections -o /tmp/c.html            # write the dashboard and open it
    etl corrections -o /tmp/c.html --charts   # also embed per-correction time-series charts
    etl corrections -o /tmp/c.html --no-open  # generate only, don't open a browser
"""

import datetime as dt
import html
import webbrowser
from pathlib import Path
from typing import Any

import rich_click as click
from owid.catalog import Dataset
from rich.console import Console
from rich.table import Table as RichTable

from etl.data_corrections import load_corrections
from etl.paths import BASE_DIR, STEP_DIR

console = Console()

# Colours for each status, used for the HTML badges.
STATUS_COLORS = {
    "open": "#d9534f",  # red — unreported error we're carrying
    "reported": "#f0ad4e",  # amber — told the provider, awaiting fix
    "acknowledged": "#5bc0de",  # blue — provider confirmed
    "fixed_upstream": "#5cb85c",  # green — fixed at source (correction can likely be removed)
}
STATUS_ORDER = ["open", "reported", "acknowledged", "fixed_upstream"]


def _locator(correction: dict[str, Any]) -> str:
    """A human-readable description of which rows a correction targets."""
    if "match" in correction:
        return "match " + ", ".join(f"{k}={v}" for k, v in correction["match"].items())
    return f"{correction.get('entity')} · {correction.get('years')}"


def _action_detail(correction: dict[str, Any]) -> str:
    """The action plus its parameter (value / factor), and the expect guard if present."""
    action = correction.get("action", "")
    if action == "override":
        action += f" → {correction.get('value')!r}"
    elif action == "scale":
        action += f" × {correction.get('factor')}"
    if "expect" in correction:
        action += " (guarded)"
    return action


def collect_corrections(step_dir: Path) -> list[dict[str, Any]]:
    """Find, load and validate every `.corrections.yml` under `step_dir`."""
    rows = []
    for path in sorted(step_dir.rglob("*.corrections.yml")):
        # load_corrections validates each entry and raises on a malformed file.
        for correction in load_corrections(path):
            rows.append({"path": path, "correction": correction})
    return rows


def _dataset_dir(corrections_path: Path) -> Path:
    """The built-dataset directory that corresponds to a `.corrections.yml` file.

    `etl/steps/data/garden/gcp/.../x.corrections.yml` → `data/garden/gcp/.../x`.
    """
    rel = corrections_path.relative_to(STEP_DIR)  # e.g. data/garden/.../x.corrections.yml
    return BASE_DIR / rel.parent / rel.name.replace(".corrections.yml", "")


def _load_series(corrections_path: Path, indicator: str, entity: str, country_col: str = "country") -> list[tuple]:
    """Return the published `(year, value)` series for `entity`/`indicator` from the built dataset.

    Raises on any problem (dataset not built, indicator renamed/absent) — the caller turns that into a
    "chart unavailable" note rather than failing the whole report.
    """
    import pandas as pd

    ds = Dataset(_dataset_dir(corrections_path))
    for table_name in ds.table_names:
        tb = ds[table_name].reset_index()
        if indicator in tb.columns and country_col in tb.columns and "year" in tb.columns:
            if not pd.api.types.is_numeric_dtype(tb[indicator]):
                raise ValueError(f"indicator '{indicator}' is categorical — no time series")
            sub = tb[tb[country_col] == entity][["year", indicator]].dropna()
            series = sorted((int(y), float(v)) for y, v in zip(sub["year"], sub[indicator]))
            if series:
                return series
    raise ValueError(f"indicator '{indicator}' not found for '{entity}' in the built dataset")


def _affected_years(correction: dict[str, Any], series_years: list[int]) -> set[int]:
    """Years the correction targets, intersected with the years present on the chart's axis."""
    years = correction.get("years")
    axis = set(series_years)
    if years == "all":
        return axis
    if years == "latest":
        return {max(series_years)} if series_years else set()
    if isinstance(years, list):
        return {int(y) for y in years}
    if isinstance(years, dict):
        lo = max(years.get("from", years.get("after", -(10**9)) + 1), min(axis, default=-(10**9)))
        hi = min(years.get("to", years.get("before", 10**9) - 1), max(axis, default=10**9))
        return {y for y in range(lo, hi + 1)}
    return set()


def _sparkline_svg(series: list[tuple], affected: set[int], action: str) -> str:
    """A small self-contained SVG line chart of the series, annotating the affected years."""
    w, h, pad = 480, 140, 32
    years = [y for y, _ in series]
    vals = [v for _, v in series]
    ymin, ymax = min(years), max(years)
    vmin, vmax = min(vals), max(vals)
    xspan = max(ymax - ymin, 1)
    vspan = max(vmax - vmin, 1e-9)

    def px(year: int) -> float:
        return pad + (year - ymin) / xspan * (w - 2 * pad)

    def py(val: float) -> float:
        return h - pad - (val - vmin) / vspan * (h - 2 * pad)

    points = " ".join(f"{px(y):.1f},{py(v):.1f}" for y, v in series)
    dots = "".join(f'<circle cx="{px(y):.1f}" cy="{py(v):.1f}" r="2" fill="#3b6"/>' for y, v in series)

    # Annotate the affected years.
    marks = []
    for ay in sorted(affected):
        x = px(ay)
        if action == "drop":
            # The dropped points are gone from the series — mark the position with a red axis tick.
            marks.append(
                f'<line x1="{x:.1f}" y1="{h - pad:.1f}" x2="{x:.1f}" y2="{h - pad + 6:.1f}" stroke="#d9534f" stroke-width="2"/>'
            )
        else:
            # scale/override keep the (corrected) point — highlight it.
            match = [v for yy, v in series if yy == ay]
            if match:
                marks.append(
                    f'<circle cx="{x:.1f}" cy="{py(match[0]):.1f}" r="4" fill="none" stroke="#d9534f" stroke-width="2"/>'
                )
    polyline = f'<polyline points="{points}" fill="none" stroke="#3b6" stroke-width="1.5"/>' if len(series) > 1 else ""
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">'
        f'<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" stroke="#ddd"/>'
        f"{polyline}{dots}{''.join(marks)}"
        f'<text x="{pad}" y="{h - pad + 18}" font-size="10" fill="#999">{ymin}</text>'
        f'<text x="{w - pad}" y="{h - pad + 18}" font-size="10" fill="#999" text-anchor="end">{ymax}</text>'
        f'<text x="{pad - 4}" y="{py(vmax):.1f}" font-size="10" fill="#999" text-anchor="end">{vmax:g}</text>'
        f'<text x="{pad - 4}" y="{py(vmin):.1f}" font-size="10" fill="#999" text-anchor="end">{vmin:g}</text>'
        "</svg>"
    )


def _chart_cell(r: dict[str, Any]) -> str:
    """The expandable chart HTML for a correction row, or a note explaining why it's unavailable."""
    c = r["correction"]
    if "match" in c or "entity" not in c:
        return '<span class="note">Chart not supported for match-based corrections.</span>'
    try:
        series = _load_series(r["path"], c["indicator"], c["entity"])
    except (FileNotFoundError, ValueError, KeyError) as e:
        return f'<span class="note">Chart unavailable: {html.escape(str(e))}. Build the step (or the indicator was renamed after the correction).</span>'
    affected = _affected_years(c, [y for y, _ in series])
    legend = "red tick = removed year" if c.get("action") == "drop" else "red ring = corrected value"
    return (
        f'<div class="note">Published (post-correction) series for {html.escape(c["entity"])} · {legend}</div>'
        + _sparkline_svg(series, affected, c.get("action", ""))
    )


def _render_html(rows: list[dict[str, Any]], generated_at: str, charts: bool) -> str:
    """Render the inventory as a single self-contained HTML page."""
    # Summary counts by status.
    by_status = {s: sum(1 for r in rows if r["correction"].get("status") == s) for s in STATUS_ORDER}
    summary_cards = "".join(
        f'<div class="card"><div class="count" style="color:{STATUS_COLORS[s]}">{by_status[s]}</div>'
        f'<div class="label">{s}</div></div>'
        for s in STATUS_ORDER
    )

    # Sort rows by provider, then status order, then dataset path.
    rows = sorted(
        rows,
        key=lambda r: (
            str(r["correction"].get("provider", "")).lower(),
            STATUS_ORDER.index(r["correction"]["status"]) if r["correction"].get("status") in STATUS_ORDER else 99,
            str(r["path"]),
        ),
    )

    n_cols = 9 if charts else 8
    body_rows = []
    for r in rows:
        c = r["correction"]
        step = r["path"].relative_to(BASE_DIR)
        # The step directory (drop the .corrections.yml filename) — the clickable dataset path.
        dataset = str(step.parent.relative_to("etl/steps")) + "/" + r["path"].name.replace(".corrections.yml", "")
        status = c.get("status", "")
        color = STATUS_COLORS.get(status, "#999")
        reported = c.get("reported", "")
        # With charts on, the row is expandable and a leading caret toggles a detail row below it.
        expand_cell = '<td class="caret">▸</td>' if charts else ""
        row_class = "main expandable" if charts else "main"
        body_rows.append(
            f'<tr class="{row_class}">'
            f"{expand_cell}"
            f"<td>{html.escape(str(c.get('provider', '')))}</td>"
            f"<td><code>{html.escape(dataset)}</code></td>"
            f"<td><code>{html.escape(str(c.get('indicator', '')))}</code></td>"
            f"<td>{html.escape(_locator(c))}</td>"
            f"<td>{html.escape(_action_detail(c))}</td>"
            f'<td><span class="badge" style="background:{color}">{html.escape(status)}</span></td>'
            f"<td>{html.escape(str(reported))}</td>"
            f'<td class="reason">{html.escape(str(c.get("reason", "")))}</td>'
            "</tr>"
        )
        if charts:
            body_rows.append(f'<tr class="detail"><td colspan="{n_cols}">{_chart_cell(r)}</td></tr>')

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data corrections inventory</title>
<style>
  :root {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  body {{ margin: 0; padding: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  .sub {{ color: #777; font-size: .85rem; margin-bottom: 1.5rem; }}
  .cards {{ display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
  .card {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: .75rem 1.25rem; text-align: center; min-width: 90px; }}
  .card .count {{ font-size: 1.6rem; font-weight: 700; }}
  .card .label {{ font-size: .75rem; color: #777; text-transform: uppercase; letter-spacing: .04em; }}
  input[type="search"] {{ width: 100%; max-width: 420px; padding: .5rem .75rem; border: 1px solid #ccc; border-radius: 6px; margin-bottom: 1rem; font-size: .9rem; }}
  .table-wrap {{ overflow-x: auto; background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: .82rem; }}
  th, td {{ text-align: left; padding: .55rem .75rem; border-bottom: 1px solid #f0f0f0; vertical-align: top; }}
  th {{ position: sticky; top: 0; background: #f7f7f7; font-weight: 600; cursor: pointer; white-space: nowrap; }}
  code {{ font-size: .78rem; background: #f3f3f3; padding: .1rem .3rem; border-radius: 3px; word-break: break-all; }}
  .badge {{ color: #fff; padding: .15rem .5rem; border-radius: 10px; font-size: .72rem; font-weight: 600; white-space: nowrap; }}
  .reason {{ max-width: 380px; color: #444; }}
  tr.main:hover {{ background: #fcfcff; }}
  tr.expandable {{ cursor: pointer; }}
  .caret {{ color: #aaa; width: 1rem; transition: transform .1s; }}
  tr.open .caret {{ transform: rotate(90deg); }}
  tr.detail {{ display: none; }}
  tr.detail.show {{ display: table-row; }}
  tr.detail td {{ background: #fbfbfd; padding: 1rem; }}
  tr.hidden {{ display: none !important; }}
  .note {{ color: #888; font-size: .78rem; margin-bottom: .4rem; }}
</style>
</head>
<body>
  <h1>Data corrections inventory</h1>
  <div class="sub">{len(rows)} corrections across the repo · generated {generated_at}</div>
  <div class="cards">{summary_cards}</div>
  <input type="search" id="filter" placeholder="Filter (provider, dataset, country, reason…)">
  <div class="table-wrap">
  <table id="t">
    <thead><tr>
      {"<th></th>" if charts else ""}<th>Provider</th><th>Dataset</th><th>Indicator</th><th>Locator</th>
      <th>Action</th><th>Status</th><th>Reported</th><th>Reason</th>
    </tr></thead>
    <tbody>
      {"".join(body_rows)}
    </tbody>
  </table>
  </div>
<script>
  const mainRows = Array.from(document.querySelectorAll('#t tbody tr.main'));
  // Pair each main row with its detail row once, before any DOM moves (sorting breaks sibling links).
  const detail = new Map(mainRows.map(r => {{
    const n = r.nextElementSibling;
    return [r, (n && n.classList.contains('detail')) ? n : null];
  }}));

  // Click an expandable row to toggle its chart detail row.
  mainRows.forEach(row => {{
    if (!row.classList.contains('expandable')) return;
    row.addEventListener('click', () => {{
      row.classList.toggle('open');
      const d = detail.get(row);
      if (d) d.classList.toggle('show');
    }});
  }});

  // Live text filter over main rows (detail rows follow their main row).
  const input = document.getElementById('filter');
  input.addEventListener('input', () => {{
    const q = input.value.toLowerCase();
    mainRows.forEach(row => {{
      const hide = !row.textContent.toLowerCase().includes(q);
      row.classList.toggle('hidden', hide);
      const d = detail.get(row);
      if (d) d.classList.toggle('hidden', hide);
    }});
  }});

  // Click a header to sort by that column (keeps each detail row attached to its main row).
  document.querySelectorAll('#t th').forEach((th, i) => {{
    let asc = true;
    th.addEventListener('click', () => {{
      const body = document.querySelector('#t tbody');
      mainRows
        .sort((a, b) => a.cells[i].textContent.localeCompare(b.cells[i].textContent) * (asc ? 1 : -1))
        .forEach(row => {{ body.appendChild(row); const d = detail.get(row); if (d) body.appendChild(d); }});
      asc = !asc;
    }});
  }});
</script>
</body>
</html>
"""


def _print_terminal_summary(rows: list[dict[str, Any]]) -> None:
    table = RichTable(title=f"{len(rows)} data corrections", show_lines=False)
    table.add_column("Provider")
    table.add_column("Dataset")
    table.add_column("Locator")
    table.add_column("Action")
    table.add_column("Status")
    for r in sorted(rows, key=lambda r: str(r["correction"].get("provider", ""))):
        c = r["correction"]
        dataset = str(r["path"].parent.relative_to(STEP_DIR)) + "/" + r["path"].name.replace(".corrections.yml", "")
        table.add_row(
            str(c.get("provider", "")),
            dataset,
            _locator(c),
            _action_detail(c),
            str(c.get("status", "")),
        )
    console.print(table)


@click.command(name="corrections", cls=click.RichCommand)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Path to write the HTML dashboard (e.g. /tmp/corrections.html).",
)
@click.option("--open/--no-open", "open_browser", default=True, help="Open the dashboard in a browser.")
@click.option(
    "--charts/--no-charts",
    default=False,
    help="Embed an expandable post-correction time-series chart per row (loads each built dataset).",
)
def cli(output: Path, open_browser: bool, charts: bool) -> None:
    """Inventory all `.corrections.yml` files and render an HTML dashboard.

    Scans every step for a `.corrections.yml` sidecar, validates each entry, and writes a
    self-contained HTML page grouped by provider and status. With ``--charts``, each row expands to a
    small time-series of the affected indicator (loaded from the built dataset — run the steps first).
    """
    rows = collect_corrections(STEP_DIR)
    if not rows:
        console.print("[yellow]No .corrections.yml files found.[/yellow]")
        return

    _print_terminal_summary(rows)

    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_render_html(rows, generated_at, charts=charts))
    console.print(f"\n[green]Wrote[/green] {output}")

    if open_browser:
        webbrowser.open(f"file://{output.resolve()}")


if __name__ == "__main__":
    cli()
