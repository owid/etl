#!/usr/bin/env python3
"""Aggregate datapitfalls scan outputs into counts + a triage list.

Usage:
    dp_aggregate.py OUTDIR [--exclude SLUG ...]

Reads every ``OUTDIR/*.json`` produced by ``dp_scan.sh`` and prints:
  - totals by severity, domain, and rule (deduped across scans);
  - the errors;
  - likely SELF-RETRACTED false positives (the tool raised a rule and then
    walked it back in its own explanation) — these should NOT be counted.

It does not write the report; it gives the numbers and the triage flags the
report is built from. See SKILL.md.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

# Phrases the tool uses when it retracts a finding inside its own explanation.
RETRACTION_MARKERS = (
    "disregard",
    "no actual",
    "on review, there is no",
    "not an issue",
    "no real issue",
    "false alarm",
)


def load_findings(outdir: Path, exclude: set[str]) -> list[dict]:
    findings: list[dict] = []
    for path in sorted(outdir.glob("*.json")):
        if path.stem in exclude:
            continue
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            print(f"  [skip] unreadable JSON: {path.name}")
            continue
        for finding in data.get("findings", []):
            finding["_scan"] = path.stem
            findings.append(finding)
    return findings


def is_retracted(finding: dict) -> bool:
    text = (finding.get("explanation", "") + " " + finding.get("evidence", "")).lower()
    return any(marker in text for marker in RETRACTION_MARKERS)


def histogram(label: str, counter: Counter) -> None:
    print(f"\n=== {label} ===")
    for key, count in counter.most_common():
        print(f"  {count:>3}  {key}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outdir", type=Path)
    parser.add_argument("--exclude", nargs="*", default=[], help="scan slugs to exclude")
    args = parser.parse_args()

    findings = load_findings(args.outdir, set(args.exclude))
    if not findings:
        print(f"No findings found under {args.outdir}")
        return

    retracted = [f for f in findings if is_retracted(f)]
    live = [f for f in findings if not is_retracted(f)]

    print(f"\nScans dir: {args.outdir}")
    if args.exclude:
        print(f"Excluded scans: {', '.join(sorted(args.exclude))}")
    print(f"Total findings: {len(findings)}  (live: {len(live)}, self-retracted: {len(retracted)})")

    histogram("by severity (live)", Counter(f.get("severity", "?") for f in live))
    histogram("by domain (live)", Counter(f.get("domain", "?") for f in live))
    histogram("by rule (live, deduped across scans)", Counter(f.get("ruleId", "?") for f in live))

    errors = [f for f in live if f.get("severity") == "error"]
    if errors:
        print("\n=== ERRORS (live) ===")
        for f in errors:
            print(f"  [{f['_scan']}] {f.get('ruleId')} :: {f.get('domain')}")

    if retracted:
        print("\n=== LIKELY SELF-RETRACTED — do NOT count ===")
        for f in retracted:
            print(f"  [{f['_scan']}] {f.get('ruleId')}: {f.get('explanation', '')[:140]}")


if __name__ == "__main__":
    main()
