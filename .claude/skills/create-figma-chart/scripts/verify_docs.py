#!/usr/bin/env python3
"""Check that this skill's docs are internally consistent, and that a refactor didn't drop instructions.

Two modes, both read-only:

  --structure            every relative link resolves; every reference/ file is reachable from
                         SKILL.md; nothing is orphaned. Run this after any edit.

  --against <git-ref>    every substantive line of the docs at <git-ref> still exists somewhere in
                         the working tree. Run this after moving text between files: splitting a
                         doc, promoting a heading, re-homing a section. Differences that are purely
                         a link path, a heading level, or a pointer rewrite are normalized away, so
                         what it reports is text that actually went missing.

Usage:
    python3 .claude/skills/create-figma-chart/scripts/verify_docs.py --structure
    python3 .claude/skills/create-figma-chart/scripts/verify_docs.py --against HEAD~1

Exits 1 on any finding.
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SKILL_DIR.parents[2]
REL = SKILL_DIR.relative_to(REPO_ROOT)

# Markdown links that point at a file (not http, not a bare anchor, not OWID's #dod: markup).
LINK = re.compile(r"\[([^\]]+)\]\((?!https?:|#)([^)]+)\)")
MIN_LINE = 12  # shorter lines are punctuation/table rules and carry no instruction


def docs() -> list[Path]:
    return sorted(p for p in SKILL_DIR.rglob("*.md") if "__pycache__" not in p.parts)


def normalize(s: str) -> str:
    """Collapse the differences a refactor legitimately introduces."""
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^#+\s*", "", s)  # heading level: ## X and # X are the same instruction
    s = re.sub(r"\((?:\.\./)+", "(", s)  # link depth: ](../../x) == ](x)
    s = re.sub(r"`(?:\.\./)+", "`", s)  # ditto for backticked path mentions
    return s


def check_structure() -> list[str]:
    findings = []
    all_md = docs()

    for f in all_md:
        for label, target in LINK.findall(f.read_text()):
            clean = target.split("#")[0]
            if not clean:
                continue
            if not (f.parent / clean).resolve().exists():
                findings.append(f"broken link: {f.relative_to(SKILL_DIR)} -> {target}  [{label}]")

    # Every doc other than the entry points must be reachable by a link from some other doc.
    entry = {"SKILL.md", "GUIDELINES.md", "SMALL-CHARTS.md", "BESPOKE-SVG.md"}
    linked: set[Path] = set()
    for f in all_md:
        for _, target in LINK.findall(f.read_text()):
            clean = target.split("#")[0]
            if clean.endswith(".md"):
                p = (f.parent / clean).resolve()
                if p.exists():
                    linked.add(p)
    for f in all_md:
        if f.name in entry:
            continue
        if f.resolve() not in linked:
            findings.append(f"orphan: {f.relative_to(SKILL_DIR)} is not linked from any other doc")

    return findings


def check_against(ref: str) -> list[str]:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, "--", str(REL)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    old_md = [p for p in listing if p.endswith(".md")]
    if not old_md:
        return [f"no markdown found under {REL} at {ref}"]

    haystack: dict[str, str] = {}
    for f in docs():
        for line in f.read_text().split("\n"):
            n = normalize(line)
            if len(n) >= MIN_LINE:
                haystack.setdefault(n, str(f.relative_to(SKILL_DIR)))

    findings = []
    keys = list(haystack)
    total = 0
    for path in old_md:
        old = subprocess.run(
            ["git", "show", f"{ref}:{path}"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
        ).stdout
        for line in old.split("\n"):
            n = normalize(line)
            if len(n) < MIN_LINE:
                continue
            total += 1
            if n in haystack:
                continue
            close = difflib.get_close_matches(n, keys, n=1, cutoff=0.60)
            if close:
                # Present but reworded — show what changed so a human can accept or reject it.
                sm = difflib.SequenceMatcher(None, n, close[0])
                edits = "; ".join(
                    f"{tag} {n[a1:a2]!r}->{close[0][b1:b2]!r}"
                    for tag, a1, a2, b1, b2 in sm.get_opcodes()
                    if tag != "equal"
                )
                findings.append(
                    f"REWORDED  {path}\n    was: {line.strip()[:150]}\n    now: {haystack[close[0]]} ({sm.ratio():.2f})\n    {edits[:300]}"
                )
            else:
                findings.append(f"LOST      {path}\n    {line.strip()[:200]}")
    print(f"compared {total} substantive lines from {len(old_md)} file(s) at {ref}")
    return findings


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--structure", action="store_true", help="check links and reachability")
    ap.add_argument("--against", metavar="GIT_REF", help="check no instruction was dropped since GIT_REF")
    args = ap.parse_args()
    if not args.structure and not args.against:
        ap.error("pass --structure and/or --against <git-ref>")

    findings = []
    if args.structure:
        findings += check_structure()
    if args.against:
        findings += check_against(args.against)

    if findings:
        print(f"\n{len(findings)} finding(s):\n")
        for f in findings:
            print(f"  {f}")
        return 1
    print("ok — no findings")
    return 0


if __name__ == "__main__":
    sys.exit(main())
