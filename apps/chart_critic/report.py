"""Render a sweep's results as a self-contained HTML page.

Charts are shown only beside a finding — a gallery of charts with nothing wrong is noise, and
the point of the page is the handful of claims someone has to adjudicate.
"""

from __future__ import annotations

import base64
import datetime as dt
import html
from pathlib import Path
from typing import Any

from apps.chart_critic.bundle import render
from apps.chart_critic.critic import issue_params

CSS = """
:root{--bg:#f7f6f3;--panel:#fff;--ink:#1b1b1b;--soft:#4a4a4a;--muted:#767676;--line:#e2ded6;
  --blue:#2b5f8e;--red:#b13507;--red-soft:#fbeee8;--green:#2e7d5b;--green-soft:#e9f4ee;
  --amber:#97650d;--amber-soft:#fbf3e0}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.55 -apple-system,BlinkMacSystemFont,
  "Segoe UI",Helvetica,Arial,sans-serif}
.wrap{max-width:1000px;margin:0 auto;padding:36px 28px 70px}
h1{font-family:Georgia,serif;font-weight:400;font-size:34px;margin:0 0 6px}
.sub{color:var(--muted);font-size:14px;margin:0 0 26px}
h2{font-family:Georgia,serif;font-weight:400;font-size:23px;margin:44px 0 10px}
code{font-family:ui-monospace,Menlo,monospace;font-size:13px;background:#f0eee9;padding:1.5px 5px;border-radius:3px}
.f{background:var(--panel);border:1px solid var(--line);border-left:4px solid var(--red);border-radius:6px;
  padding:18px 20px;margin:16px 0}
.f.medium{border-left-color:var(--amber)}.f.low{border-left-color:#b9b2a6}
.f h3{margin:0 0 8px;font-size:17px;font-family:Georgia,serif;font-weight:400}
.f h3 a{color:var(--ink);text-decoration:none;border-bottom:1px solid var(--line)}
.tags{display:flex;flex-wrap:wrap;gap:7px;margin:0 0 12px;font-size:11px}
.tag{padding:3px 8px;border-radius:3px;font-weight:700}
.hi{background:var(--red-soft);color:var(--red)}.md{background:var(--amber-soft);color:var(--amber)}
.lo{background:#efeae4;color:#6b5b46}.bl{background:#eaf1f7;color:#1f4a70}
.f p{font-size:14.5px;color:var(--soft);margin:0 0 9px}
.f p b{color:var(--ink)}
.f img{width:100%;max-width:620px;display:block;border:1px solid var(--line);border-radius:4px;
  margin:12px 0 4px;background:#fff}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin:14px 0}
.chip{display:inline-flex;gap:6px;align-items:baseline;background:var(--panel);border:1px solid var(--line);
  border-radius:4px;padding:4px 8px;font-size:11.5px}
.chip b{font-family:ui-monospace,Menlo,monospace;font-weight:400;color:var(--soft)}
.chip i{font-style:normal;color:var(--muted)}
.chip.gone b{color:var(--muted);text-decoration:line-through}
.note{background:var(--panel);border-left:3px solid var(--blue);padding:14px 18px;margin:20px 0;font-size:15px}
.ok{background:var(--green-soft);border-left-color:var(--green)}
footer{margin-top:44px;padding-top:16px;border-top:1px solid var(--line);font-size:12.5px;color:var(--muted)}
"""

SEV_CLASS = {"high": "hi", "medium": "md", "low": "lo"}


def _finding(result: dict[str, Any], issue: dict[str, Any], png: bytes | None, params: str) -> str:
    sev = issue.get("severity", "low")
    base = f"https://ourworldindata.org/grapher/{result['slug']}"
    url = issue.get("url") or (f"{base}?{params}" if params else base)
    parts = [
        f'<div class="f {sev}">',
        '<div class="tags">',
        f'<span class="tag {SEV_CLASS.get(sev, "lo")}">{html.escape(sev)} severity</span>',
        f'<span class="tag {SEV_CLASS.get(issue.get("confidence", "low"), "lo")}">'
        f"{html.escape(issue.get('confidence', '?'))} confidence</span>",
        f'<span class="tag bl">{html.escape(issue.get("kind", "?"))}-level</span>',
        f'<span class="tag lo">found in: {html.escape(issue.get("found_in", "?"))}</span>',
        f'<span class="tag lo">{result["views"]:,} views/yr</span>' if result["views"] else "",
        "</div>",
        f'<h3><a href="{url}">{html.escape(issue.get("claim", ""))}</a></h3>',
        f"<p><b>Evidence:</b> {html.escape(issue.get('evidence', ''))}</p>",
        f"<p><b>A reader would conclude:</b> {html.escape(issue.get('reader_impact', ''))}</p>",
        f'<p><b>Chart:</b> <a href="{url}"><code>{html.escape(result["slug"])}</code></a>'
        f"{' — linked to the view showing it' if issue.get('chart_params') else ''}</p>",
    ]
    if png:
        b64 = base64.b64encode(png).decode()
        parts.append(f'<img src="data:image/png;base64,{b64}" alt="{html.escape(result["slug"])}">')
    parts.append("</div>")
    return "\n".join(parts)


def write(results: list[dict[str, Any]], path: Path, model: str) -> None:
    flagged = [r for r in results if r["issues"]]
    clean = [r for r in results if not r["issues"] and r["status"] == "ok"]
    gone = [r for r in results if r["status"] == "gone"]
    cost = sum(r["cost"] for r in results)
    n_issues = sum(len(r["issues"]) for r in flagged)

    # Render each finding's own view rather than the chart's default, so the image shows what
    # the claim is about.
    cards = []
    for r in flagged:
        for issue in r["issues"]:
            # Both halves of the view: the params the chart was reviewed at (an mdim view) and
            # the ones the model attached. Rendering only the latter shows the mdim default —
            # a different chart than the finding is about.
            params = issue_params(r.get("params", ""), issue)
            try:
                png = render(r["slug"], params)
            except Exception:  # noqa: BLE001 — a missing render must not break the report
                png = None
            cards.append(_finding(r, issue, png, params))

    chips = "\n".join(
        f'<span class="chip"><b>{html.escape(r["slug"])}</b><i>{r["views"]:,}</i></span>'
        if r["views"]
        else f'<span class="chip"><b>{html.escape(r["slug"])}</b></span>'
        for r in clean
    ) + "\n".join(f'<span class="chip gone"><b>{html.escape(r["slug"])}</b><i>gone</i></span>' for r in gone)

    headline = (
        f'<div class="note ok"><b>No issues found</b> in {len(clean)} charts.</div>'
        if not flagged
        else f'<div class="note"><b>{n_issues} issue(s) across {len(flagged)} of {len(results)} charts.</b> '
        "Every one is a claim to check against the data, not a verdict — and an error in an "
        "indicator affects every chart using it, the data page, the API and every download.</div>"
    )

    path.write_text(
        f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chart critic — {len(results)} charts</title><style>{CSS}</style></head><body>
<div class="wrap">
<h1>Chart critic</h1>
<p class="sub">{len(results)} charts reviewed with {html.escape(model)} · {dt.date.today().isoformat()}
 · ${cost:.4f} (${cost / max(len(results), 1):.4f} per chart)</p>
{headline}
{"<h2>Findings</h2>" + chr(10).join(cards) if cards else ""}
<h2>Reviewed with nothing to report</h2>
<div class="chips">{chips}</div>
<footer>Generated by <code>etl chart-critic</code>. Findings are model output verified by nobody yet;
confirm against the data before filing. Statistical anomaly detection is
<code>apps/anomalist</code>'s job — this looks for what a reader would misread.</footer>
</div></body></html>
"""
    )
