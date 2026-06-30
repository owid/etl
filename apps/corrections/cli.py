"""CLI to inventory and visualise all `.corrections.yml` files in the repo.

Scans every step for a `.corrections.yml` sidecar, validates the entries (so this also acts as a
repo-wide lint), and renders a self-contained HTML dashboard grouped by provider and status. This is
the cross-dataset view of known upstream data errors and what we've reported to whom.

    etl corrections                       # write ai/corrections.html and open it
    etl corrections -o /tmp/c.html        # custom output path
    etl corrections --no-open             # don't open a browser
"""

import datetime as dt
import html
import webbrowser
from pathlib import Path
from typing import Any

import rich_click as click
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


def _render_html(rows: list[dict[str, Any]], generated_at: str) -> str:
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

    body_rows = []
    for r in rows:
        c = r["correction"]
        step = r["path"].relative_to(BASE_DIR)
        # The step directory (drop the .corrections.yml filename) — the clickable dataset path.
        dataset = str(step.parent.relative_to("etl/steps")) + "/" + r["path"].name.replace(".corrections.yml", "")
        status = c.get("status", "")
        color = STATUS_COLORS.get(status, "#999")
        reported = c.get("reported", "")
        body_rows.append(
            "<tr>"
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
  tr:hover {{ background: #fcfcff; }}
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
      <th>Provider</th><th>Dataset</th><th>Indicator</th><th>Locator</th>
      <th>Action</th><th>Status</th><th>Reported</th><th>Reason</th>
    </tr></thead>
    <tbody>
      {"".join(body_rows)}
    </tbody>
  </table>
  </div>
<script>
  // Live text filter across all cells.
  const input = document.getElementById('filter');
  const rows = Array.from(document.querySelectorAll('#t tbody tr'));
  input.addEventListener('input', () => {{
    const q = input.value.toLowerCase();
    rows.forEach(r => {{ r.style.display = r.textContent.toLowerCase().includes(q) ? '' : 'none'; }});
  }});
  // Click a header to sort by that column.
  document.querySelectorAll('#t th').forEach((th, i) => {{
    let asc = true;
    th.addEventListener('click', () => {{
      const body = document.querySelector('#t tbody');
      Array.from(body.querySelectorAll('tr'))
        .sort((a, b) => a.cells[i].textContent.localeCompare(b.cells[i].textContent) * (asc ? 1 : -1))
        .forEach(r => body.appendChild(r));
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
    default=BASE_DIR / "ai" / "corrections.html",
    help="Path to write the HTML dashboard.",
)
@click.option("--open/--no-open", "open_browser", default=True, help="Open the dashboard in a browser.")
def cli(output: Path, open_browser: bool) -> None:
    """Inventory all `.corrections.yml` files and render an HTML dashboard.

    Scans every step for a `.corrections.yml` sidecar, validates each entry, and writes a
    self-contained HTML page grouped by provider and status.
    """
    rows = collect_corrections(STEP_DIR)
    if not rows:
        console.print("[yellow]No .corrections.yml files found.[/yellow]")
        return

    _print_terminal_summary(rows)

    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_render_html(rows, generated_at))
    console.print(f"\n[green]Wrote[/green] {output}")

    if open_browser:
        webbrowser.open(f"file://{output.resolve()}")


if __name__ == "__main__":
    cli()
