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
import json
import webbrowser
from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table as RichTable

from etl.data_corrections import _label, load_corrections, read_audit
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


def _fmt(v: float | None) -> str:
    """Human-readable number with thousands separators (e.g. 56,180,148 · -742,811 · 14.2)."""
    if v is None:
        return "—"
    a = abs(v)
    if a != 0 and (a < 0.01 or a >= 1e15):
        return f"{v:.3g}"  # fall back to scientific only for extreme magnitudes
    return f"{v:,.2f}".rstrip("0").rstrip(".") or "0"


def _affected_table(affected: list, action: str) -> str:
    """A small table of the affected points: year, the (problematic) before value, and the after value."""
    after_label = "After" if action != "drop" else "Result"
    body = "".join(
        f'<tr><td>{int(y)}</td><td class="bad">{_fmt(before)}</td>'
        f"<td>{'removed' if action == 'drop' else _fmt(after)}</td></tr>"
        for y, before, after in affected
    )
    return f'<table class="affected"><thead><tr><th>Year</th><th>Before</th><th>{after_label}</th></tr></thead><tbody>{body}</tbody></table>'


def _plotly_source() -> str:
    """The bundled plotly.min.js shipped with the plotly Python package (inlined for a portable file)."""
    import plotly

    return (Path(plotly.__file__).parent / "package_data" / "plotly.min.js").read_text()


def _chart_traces(ent: dict[str, Any], action: str) -> tuple[list, dict]:
    """Build Plotly traces + layout for one entity: the pre-correction series with bad/corrected points."""
    series = ent["series"]
    affected = ent["affected"]
    before = [(y, b) for y, b, _ in affected if b is not None]
    after = [(y, a) for y, _, a in affected if action != "drop" and a is not None]

    traces: list[dict] = []
    if series:
        traces.append(
            {
                "x": [y for y, _ in series],
                "y": [v for _, v in series],
                "mode": "lines+markers",
                "name": "series",
                "line": {"color": "#9bb7c0"},
                "marker": {"size": 5, "color": "#9bb7c0"},
            }
        )
    if before:
        traces.append(
            {
                "x": [y for y, _ in before],
                "y": [b for _, b in before],
                "mode": "markers",
                "name": "removed (bad)" if action == "drop" else "original (bad)",
                "marker": {"size": 11, "color": "#d9534f", "symbol": "circle"},
            }
        )
    if after:
        traces.append(
            {
                "x": [y for y, _ in after],
                "y": [a for _, a in after],
                "mode": "markers",
                "name": "corrected",
                "marker": {"size": 9, "color": "#5cb85c", "symbol": "diamond"},
            }
        )
    layout = {
        "margin": {"t": 28, "r": 12, "b": 36, "l": 64},
        "height": 300,
        "hovermode": "closest",
        "showlegend": True,
        "legend": {"orientation": "h", "y": -0.2},
        "xaxis": {"title": {"text": "Year"}, "tickformat": "d"},
        "yaxis": {"title": {"text": ""}},
        # A Linear/Log y-axis toggle (log drops non-positive points, so Linear stays the default).
        "updatemenus": [
            {
                "type": "buttons",
                "direction": "right",
                "x": 1,
                "xanchor": "right",
                "y": 1.2,
                "showactive": True,
                "buttons": [
                    {"label": "Linear", "method": "relayout", "args": [{"yaxis.type": "linear"}]},
                    {"label": "Log", "method": "relayout", "args": [{"yaxis.type": "log"}]},
                ],
            }
        ],
    }
    return traces, layout


def _chart_cell(r: dict[str, Any]) -> str:
    """The expandable detail for a correction: a before→after table plus a Plotly time-series."""
    records = read_audit(r["path"])
    if records is None:
        rel = r["path"].parent.relative_to(STEP_DIR)
        return f'<span class="note">No audit captured yet — run the step (<code>etlr {rel}</code>) to generate chart data.</span>'
    rec = next((x for x in records if x["label"] == _label(r["correction"])), None)
    if rec is None:
        return '<span class="note">No audit entry for this correction (re-run the step after editing it).</span>'
    if not rec.get("numeric"):
        return '<span class="note">Indicator is non-numeric (categorical) — no time series.</span>'

    action = rec["action"]
    legend = "red = removed values" if action == "drop" else "red = original (bad) value · green = corrected value"
    blocks = []
    for ent in rec["entities"]:
        if not ent["affected"]:
            continue
        traces, layout = _chart_traces(ent, action)
        # Plotly is initialised lazily (on row expand) from these data-* attributes — the detail row is
        # display:none until then, and Plotly can't size a hidden container.
        traces_attr = html.escape(json.dumps(traces), quote=True)
        layout_attr = html.escape(json.dumps(layout), quote=True)
        blocks.append(
            f'<div class="entity-block">'
            f'<div class="note"><b>{html.escape(ent["entity"])}</b> · pre-correction series · {legend}</div>'
            f'<div class="chart-row">{_affected_table(ent["affected"], action)}'
            f'<div class="plot" data-traces="{traces_attr}" data-layout="{layout_attr}"></div>'
            f"</div></div>"
        )
    return "".join(blocks) or '<span class="note">No affected points captured.</span>'


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

    # Inline the Plotly bundle only when charts are requested (keeps the metadata-only report small).
    plotly_script = f"<script>{_plotly_source()}</script>" if charts else ""

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
  #t {{ border-collapse: collapse; width: 100%; font-size: .82rem; }}
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
  .entity-block {{ margin-bottom: 1.5rem; }}
  .chart-row {{ display: flex; gap: 1.5rem; align-items: flex-start; flex-wrap: wrap; }}
  .plot {{ flex: 0 1 620px; min-width: 0; height: 320px; }}
  table.affected {{ border-collapse: collapse; font-size: .8rem; width: auto; flex: 0 0 auto; align-self: flex-start; }}
  table.affected th, table.affected td {{ border: 1px solid #eee; padding: .3rem .6rem; text-align: right; }}
  table.affected th {{ background: #f7f7f7; position: static; }}
  table.affected td.bad {{ color: #d9534f; font-weight: 600; }}
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
{plotly_script}
<script>
  const mainRows = Array.from(document.querySelectorAll('#t tbody tr.main'));
  // Pair each main row with its detail row once, before any DOM moves (sorting breaks sibling links).
  const detail = new Map(mainRows.map(r => {{
    const n = r.nextElementSibling;
    return [r, (n && n.classList.contains('detail')) ? n : null];
  }}));

  // Plotly can't size a hidden container, so we draw each chart the first time its row is expanded.
  const drawn = new WeakSet();
  function drawPlots(detailRow) {{
    if (typeof Plotly === 'undefined') return;
    detailRow.querySelectorAll('.plot').forEach(div => {{
      if (drawn.has(div)) return;
      drawn.add(div);
      Plotly.newPlot(div, JSON.parse(div.dataset.traces), JSON.parse(div.dataset.layout),
                     {{responsive: true, displayModeBar: false}});
    }});
  }}

  // Click an expandable row to toggle its chart detail row.
  mainRows.forEach(row => {{
    if (!row.classList.contains('expandable')) return;
    row.addEventListener('click', () => {{
      row.classList.toggle('open');
      const d = detail.get(row);
      if (d) {{ d.classList.toggle('show'); if (d.classList.contains('show')) drawPlots(d); }}
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
