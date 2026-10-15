"""Rendering for what the term selection chose, and what it left behind.

A run covers 125 topics, so the terminal gets one line per topic — enough to see
which topics are badly covered, which is the thing worth acting on — and the full
per-term arithmetic goes to an HTML file where it can be read.
"""

from __future__ import annotations

import html
import statistics
from datetime import UTC, datetime

from coverage_model import TopicSelection, WeightedUniverse


def render_topic_table(selection: TopicSelection, universe: WeightedUniverse) -> str:
    """The full per-term arithmetic for one topic, as text."""
    total = selection.total_weight
    resolution = universe.resolution_counts()
    lines = [
        f"{selection.topic_name} — {selection.total_count} records, {total:,.0f} views "
        f"({resolution.get('per-view', 0)} weighted per view, "
        f"{resolution.get('averaged', 0)} averaged, {resolution.get('unmatched', 0)} unmatched)",
        f"  {'#':<3}{'term':<34}{'own':>17}{'adds':>17}{'cumulative':>12}",
    ]
    for index, term in enumerate(selection.selected, 1):
        lines.append(
            f"  {index:<3}{term.term:<34}"
            f"{term.own_share(total):>8.1%} /{term.own_count:>6}"
            f"{term.marginal_share(total):>8.1%} /{term.marginal_count:>6}"
            f"{term.cumulative_share(total):>12.1%}"
        )
    if selection.near_misses:
        misses = ", ".join(
            f"{miss.term!r} +{miss.marginal_share(total):.1%}"
            for miss in selection.near_misses[:3]
        )
        lines.append(f"  not taken: {misses}")
    uncovered_weight = total - selection.covered_weight
    lines.append(
        f"  uncovered: {uncovered_weight / total if total else 0:.1%} of views, "
        f"{len(selection.uncovered)} records"
    )
    if selection.topic_name_share > 0.2:
        lines.append(
            f"  note: the topic's own name alone reaches "
            f"{selection.topic_name_share:.0%} of these views, which no suggestion "
            f"can narrow — the reachable share is smaller than 100%"
        )
    for title, weight in selection.uncovered[:5]:
        lines.append(f"      {weight:>9,.0f}  {title[:64]}")
    return "\n".join(lines)


def render_run_summary(selections: list[TopicSelection]) -> str:
    """One line per topic, worst-covered first — that is what needs attention."""
    if not selections:
        return "no topics selected"
    rows = sorted(
        selections,
        key=lambda s: (s.covered_weight / s.total_weight) if s.total_weight else 0.0,
    )
    lines = [f"  {'topic':<38}{'terms':>6}{'views covered':>15}{'records':>10}"]
    for selection in rows:
        share = (
            selection.covered_weight / selection.total_weight
            if selection.total_weight
            else 0.0
        )
        covered_records = selection.total_count - len(selection.uncovered)
        lines.append(
            f"  {selection.topic_name[:36]:<38}{len(selection.selected):>6}"
            f"{share:>14.1%} {covered_records:>6}/{selection.total_count:<6}"
        )
    shares = [
        (s.covered_weight / s.total_weight) if s.total_weight else 0.0
        for s in selections
    ]
    thin = sum(1 for share in shares if share < 0.35)
    lines.append(
        f"  ── median {statistics.median(shares):.1%} of views covered; "
        f"{thin} topics below 35%; "
        f"{sum(1 for s in selections if len(s.selected) <= 1)} topics with one term or none"
    )
    return "\n".join(lines)


_CSS = """
body{margin:0;background:#fff;color:#1a1a1a;font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
.wrap{max-width:1040px;margin:0 auto;padding:40px 24px 80px}
h1{font-family:Georgia,serif;font-size:30px;color:#002147;margin:0 0 6px}
.meta{color:#6e6e6e;font-size:13.5px;margin-bottom:28px}
details{border:1px solid #e3e3e3;border-radius:6px;margin:10px 0;background:#fff}
details[open]{box-shadow:0 1px 3px rgba(0,0,0,.05)}
summary{cursor:pointer;padding:11px 16px;font-weight:600;color:#002147;display:flex;gap:14px;align-items:baseline}
summary .share{font-family:Georgia,serif;font-size:19px;min-width:74px}
summary .sub{font-weight:400;color:#6e6e6e;font-size:13px}
.body{padding:4px 16px 16px}
table{border-collapse:collapse;width:100%;font-size:14px;margin:8px 0 14px}
th,td{text-align:left;padding:6px 9px;border-bottom:1px solid #eee}
th{background:#f7f8f9;font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;color:#6e6e6e}
td.n{text-align:right;font-variant-numeric:tabular-nums}
.term{font-weight:600}
.bar{background:#e8eef5;height:7px;border-radius:4px;overflow:hidden;min-width:80px}
.bar span{display:block;height:100%;background:#1d3d63}
.note{font-size:13px;color:#4a4a4a;margin:6px 0}
.uncov td{color:#4a4a4a}
.thin summary{background:#fdf6ec}
"""


def render_html_report(
    selections: list[TopicSelection],
    universes: dict[str, WeightedUniverse],
    weighting: str,
    views_column: str,
) -> str:
    """One collapsible section per topic, worst-covered first."""
    rows = sorted(
        selections,
        key=lambda s: (s.covered_weight / s.total_weight) if s.total_weight else 0.0,
    )
    out = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Topic vocabulary coverage</title>",
        f"<style>{_CSS}</style></head><body><div class='wrap'>",
        "<h1>Topic vocabulary coverage</h1>",
        f"<div class='meta'>{len(selections)} topics · weighting <code>{html.escape(weighting)}</code>"
        f" on <code>{html.escape(views_column)}</code> · generated "
        f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}<br>"
        "Each term's <em>own</em> coverage is everything it reveals; <em>adds</em> is what it "
        "reveals that the terms above it did not. Sorted worst-covered first.</div>",
    ]

    for selection in rows:
        total = selection.total_weight
        share = selection.covered_weight / total if total else 0.0
        universe = universes.get(selection.topic_name)
        resolution = universe.resolution_counts() if universe else {}
        thin = " class='thin'" if share < 0.35 else ""
        out.append(f"<details{thin}><summary><span class='share'>{share:.0%}</span>")
        out.append(
            f"<span>{html.escape(selection.topic_name)}</span>"
            f"<span class='sub'>{len(selection.selected)} terms · "
            f"{selection.total_count} records · {total:,.0f} views</span></summary>"
        )
        out.append("<div class='body'>")
        out.append(
            "<table><tr><th>#</th><th>term</th><th class='n'>own</th>"
            "<th class='n'>records</th><th class='n'>adds</th>"
            "<th class='n'>cumulative</th><th></th></tr>"
        )
        for index, term in enumerate(selection.selected, 1):
            out.append(
                f"<tr><td class='n'>{index}</td><td class='term'>{html.escape(term.term)}</td>"
                f"<td class='n'>{term.own_share(total):.1%}</td>"
                f"<td class='n'>{term.own_count}</td>"
                f"<td class='n'>{term.marginal_share(total):.1%}</td>"
                f"<td class='n'>{term.cumulative_share(total):.1%}</td>"
                f"<td><div class='bar'><span style='width:{min(100, term.cumulative_share(total) * 100):.1f}%'></span></div></td></tr>"
            )
        out.append("</table>")

        if selection.near_misses:
            misses = ", ".join(
                f"<code>{html.escape(miss.term)}</code> +{miss.marginal_share(total):.1%}"
                for miss in selection.near_misses[:3]
            )
            out.append(f"<div class='note'>Not taken: {misses}</div>")
        if resolution:
            out.append(
                f"<div class='note'>Weights: {resolution.get('per-view', 0)} records per view, "
                f"{resolution.get('averaged', 0)} averaged over their explorer or multi-dim, "
                f"{resolution.get('unmatched', 0)} with no view data.</div>"
            )
        if selection.topic_name_share > 0.2:
            out.append(
                f"<div class='note'>The topic's own name alone reaches "
                f"<strong>{selection.topic_name_share:.0%}</strong> of these views. A reader here "
                "has already applied it, so that much of the topic is not narrowable by any "
                "suggestion and the coverage above is measured against a ceiling below 100%.</div>"
            )
        out.append(
            f"<div class='note'>Uncovered: {1 - share:.1%} of views across "
            f"{len(selection.uncovered)} records — the most-viewed of them:</div>"
        )
        out.append("<table class='uncov'><tr><th class='n'>views</th><th>chart</th></tr>")
        for title, weight in selection.uncovered[:15]:
            out.append(
                f"<tr><td class='n'>{weight:,.0f}</td><td>{html.escape(title)}</td></tr>"
            )
        out.append("</table></div></details>")

    out.append("</div></body></html>")
    return "\n".join(out)
