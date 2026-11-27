"""Assemble the two lanes of a full fact check into one report.

A full review runs as two lanes from the same dossier (`prepare.py`): the claims lane writes
`lane_claims.result.md`, the values lane `lane_values.result.md`. This script merges them into
`ai/adversarial-review-<short_name>-<date>.md`:

  - findings from both lanes, numbered and ordered 🔴 → 🟡 → 🟢;
  - Part 0 from the claims lane; the cross-check appendix from the values lane;
  - the SKIPPED indicators copied from `ranking.csv`, so a truncated review never reads as complete;
  - when the targeted pass on Anomalist's candidates ran (`anomalist_candidates.py`), its `lane_anomalist.result.md`
    too: findings tagged as that lane, and its own tables in the appendix. Run the script again after that pass;
    it rewrites the report.

It refuses to write a report when a lane result or one of its required sections is missing, and when a lane left
notes for the other lane that the caller has not settled (`--reconciled` says they are settled).

    .venv/bin/python .claude/skills/fact-check-dataset/scripts/assemble_report.py <dossier dir> [--reconciled] \
        [--tldr "One to three sentences."] [--date YYYY-MM-DD]
"""

import argparse
import csv
import datetime as dt
import re
import sys
from pathlib import Path

SEVERITIES = ("🔴", "🟡", "🟢")
REQUIRED = {
    "lane_claims.result.md": ["Part 0", "Findings", "Notes for the other lane"],
    "lane_values.result.md": ["Findings", "Values checked", "Anomalies not checked", "Notes for the other lane"],
}
# The targeted pass on Anomalist's candidates (`anomalist_candidates.py`), run later in an update: optional, but
# once its brief is in the dossier the report waits for its result.
ANOMALIST = "lane_anomalist.result.md"
ANOMALIST_BRIEF = "lane_anomalist.md"
# What a lane writes under a heading when it has nothing to report there.
EMPTY = re.compile(r"^\W*(none|no findings|nothing|n/a|no notes)\b", re.IGNORECASE)
FINDING_START = re.compile(r"^\s*(?:[-*]\s*)?(?:\d+[.)]\s*)?(🔴|🟡|🟢)")


def sections(text: str) -> dict[str, str]:
    """A result's `## ` sections, keyed by heading text."""
    found: dict[str, str] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            found[current] = ""
        elif current is not None:
            found[current] += line + "\n"
    return {heading: body.strip() for heading, body in found.items()}


def section(parts: dict[str, str], name: str) -> str | None:
    """The body of the section whose heading starts with `name`, or None when it is absent."""
    for heading, body in parts.items():
        if heading.lower().startswith(name.lower()):
            return body
    return None


def is_empty(body: str | None) -> bool:
    return body is None or not body.strip() or bool(EMPTY.match(body.strip()))


def findings(body: str) -> list[tuple[str, str]]:
    """(severity, text) of each finding: a block that starts with a severity mark, up to the next one."""
    blocks: list[list[str]] = []
    for line in body.splitlines():
        if FINDING_START.match(line):
            blocks.append([FINDING_START.sub(lambda m: m.group(1), line, count=1)])
        elif blocks:
            blocks[-1].append(line)
    return [(block[0][0], "\n".join(block).rstrip()) for block in blocks]


def skipped_indicators(dossier: Path) -> list[str]:
    path = dossier / "ranking.csv"
    if not path.exists():
        return [
            "- `ranking.csv` is missing from the dossier: the SKIPPED list could not be written. Run `prepare.py` again."
        ]
    with path.open() as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("selected") in ("False", "false", "0")]
    return [f"- `{row['identity']}`: {row['why']}" for row in rows] or ["- none: every indicator was reviewed"]


def assemble(dossier: Path, reconciled: bool, tldr: str | None, date: str) -> tuple[str, list[str], dict[str, int]]:
    """The report's text, the problems that stop it from being written, and the findings by severity."""
    problems: list[str] = []
    lanes: dict[str, dict[str, str]] = {}
    for name, headings in REQUIRED.items():
        path = dossier / name
        if not path.exists():
            problems.append(f"{name} is missing: that lane has not reported")
            continue
        lanes[name] = sections(path.read_text())
        problems += [f"{name} has no `## {h}` section" for h in headings if section(lanes[name], h) is None]
    anomalist: dict[str, str] | None = None
    if (dossier / ANOMALIST).exists():
        anomalist = sections((dossier / ANOMALIST).read_text())
        headings = REQUIRED["lane_values.result.md"]
        problems += [f"{ANOMALIST} has no `## {h}` section" for h in headings if section(anomalist, h) is None]
    elif (dossier / ANOMALIST_BRIEF).exists():
        problems.append(f"{ANOMALIST_BRIEF} is in the dossier but {ANOMALIST} is not: that lane has not reported")
    if problems:
        return "", problems, {}

    claims, values = lanes["lane_claims.result.md"], lanes["lane_values.result.md"]
    notes = {
        "claims lane → values lane": section(claims, "Notes for the other lane"),
        "values lane → claims lane": section(values, "Notes for the other lane"),
    }
    if anomalist is not None:
        notes["anomalist lane → the other lanes"] = section(anomalist, "Notes for the other lane")
    open_notes = {who: body for who, body in notes.items() if not is_empty(body)}
    if open_notes and not reconciled:
        problems.append(
            "a lane left notes for the other lane. Settle each one (does it change a finding in the other result?), "
            "edit the results if it does, then run again with --reconciled:\n"
            + "\n".join(f"  {who}:\n    " + body.replace("\n", "\n    ") for who, body in open_notes.items())
        )
        return "", problems, {}

    merged = [(severity, text, "claims") for severity, text in findings(section(claims, "Findings") or "")]
    merged += [(severity, text, "values") for severity, text in findings(section(values, "Findings") or "")]
    results = [("claims", claims), ("values", values)]
    if anomalist is not None:
        merged += [(severity, text, "anomalist") for severity, text in findings(section(anomalist, "Findings") or "")]
        results.append(("anomalist", anomalist))
    for lane, parts in results:
        body = section(parts, "Findings") or ""
        if not is_empty(body) and not findings(body):
            problems.append(f"the {lane} lane's Findings has text but no line starting with 🔴, 🟡 or 🟢")
    if problems:
        return "", problems, {}
    merged.sort(key=lambda item: SEVERITIES.index(item[0]))
    counts = {severity: sum(1 for item in merged if item[0] == severity) for severity in SEVERITIES}

    step = "/".join(dossier.parts[-3:])
    if not tldr:
        tally = ", ".join(f"{count} {severity}" for severity, count in counts.items())
        first = merged[0][1].splitlines()[0] if merged else ""
        tldr = f"{tally}. " + (f"Fix first: {first}" if merged else "No findings: the review came back clean.")
    out = [f"# Adversarial data review — {step}", "", f"Reviewed {date}.", "", "## TL;DR", "", tldr, ""]
    out += ["## Part 0 — Source verification", "", section(claims, "Part 0") or "", ""]
    out += ["## Part 1 — Findings", ""]
    if not merged:
        out += ["No findings.", ""]
    for number, (_, text, lane) in enumerate(merged, start=1):
        head, *rest = text.splitlines()
        out += [f"{number}. {head} _({lane} lane)_", *rest, ""]
    out += [
        "## Part 2 — Cross-check appendix",
        "",
        "### Values checked",
        "",
        section(values, "Values checked") or "",
        "",
    ]
    if anomalist is not None:
        out += ["### Values checked: Anomalist candidates", "", section(anomalist, "Values checked") or "", ""]
    out += ["### SKIPPED indicators", "", *skipped_indicators(dossier), ""]
    out += ["### Anomalies not checked", "", section(values, "Anomalies not checked") or "", ""]
    if anomalist is not None:
        out += ["### Anomalist candidates not checked", "", section(anomalist, "Anomalies not checked") or "", ""]
        out += ["Every other stored candidate, with why it was left out, is in `anomalist.csv` in the dossier.", ""]
    if open_notes:
        out += ["### Notes between the lanes (settled by the caller)", ""]
        out += [f"- {who}: {' '.join(body.split())}" for who, body in open_notes.items()]
        out += [""]
    return "\n".join(out), [], counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dossier", type=Path, help="the dossier directory `prepare.py` printed")
    parser.add_argument("--reconciled", action="store_true", help="the notes between the lanes are settled")
    parser.add_argument("--tldr", help="the report's TL;DR (default: the counts and the first finding)")
    parser.add_argument("--date", default=dt.date.today().isoformat())
    args = parser.parse_args()

    dossier = args.dossier.resolve()
    text, problems, counts = assemble(dossier, args.reconciled, args.tldr, args.date)
    if problems:
        sys.exit("Report NOT written:\n- " + "\n- ".join(problems))
    # <repo>/ai/fact-check/<namespace>/<version>/<short_name> → <repo>/ai/adversarial-review-<short_name>-<date>.md
    report = dossier.parents[3] / f"adversarial-review-{dossier.name}-{args.date}.md"
    report.write_text(text)
    print(f"Report written: {report}")
    print("Findings: " + ", ".join(f"{count} {severity}" for severity, count in counts.items()))


if __name__ == "__main__":
    main()
