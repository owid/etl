#!/usr/bin/env python3
"""Check that this skill's docs are internally consistent, and that a refactor didn't drop instructions.

Two modes, both read-only:

  --structure            every relative link resolves; every reference/ file is reachable from
                         SKILL.md; nothing is orphaned; the eagerly-read docs are inside their
                         size budgets. Run this after any edit.

  --against <git-ref>    every substantive line of the docs at <git-ref> still exists somewhere in
                         the working tree. Run this after moving text between files: splitting a
                         doc, promoting a heading, re-homing a section. Differences that are purely
                         a link path, a heading level, or a pointer rewrite are normalized away, so
                         what it reports is text that actually went missing.

Usage (from the repo root, always through the repo virtualenv):
    .venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --structure
    .venv/bin/python .claude/skills/create-figma-chart/scripts/verify_docs.py --against HEAD~1

Exits 1 on any finding.
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from pathlib import Path

# Defaults to this script's own skill; --skill points it at a sibling (e.g. create-static-viz),
# which is why the paths below are resolved at call time rather than at import.
DEFAULT_SKILL_DIR = Path(__file__).resolve().parent.parent
SKILLS_ROOT = DEFAULT_SKILL_DIR.parent

# Markdown links that point at a file (not http, not a bare anchor, not OWID's #dod: markup).
LINK = re.compile(r"\[([^\]]+)\]\((?!https?:|#)([^)]+)\)")
MIN_LINE = 12  # shorter lines are punctuation/table rules and carry no instruction

# Byte budgets for the docs a run reads EAGERLY, keyed by skill directory name. Each doc has its own
# cap, and the group has a combined one: capping only per-file is gameable, since moving a paragraph
# from one eagerly-read file to another satisfies both caps while costing a run exactly as much. Each
# skill's own SKILL.md states these numbers — keep the two in step. Lazily-read files (reference/,
# per-chart-type/) are deliberately unbudgeted: they cost only the run that needs them.
BUDGETS: dict[str, dict[str, int]] = {
    "create-figma-chart": {"SKILL.md": 64_000, "GUIDELINES.md": 80_000, "*": 145_000},
    "create-static-viz": {"SKILL.md": 30_000, "TEMPLATES.md": 25_000},
}


def docs(skill_dir: Path) -> list[Path]:
    return sorted(p for p in skill_dir.rglob("*.md") if "__pycache__" not in p.parts)


def normalize(s: str) -> str:
    """Collapse the differences a refactor legitimately introduces."""
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^#+\s*", "", s)  # heading level: ## X and # X are the same instruction
    s = re.sub(r"\((?:\.\./)+", "(", s)  # link depth: ](../../x) == ](x)
    s = re.sub(r"`(?:\.\./)+", "`", s)  # ditto for backticked path mentions
    # Inline formatting is not content: promoting a lesson tends to bold its lead or backtick a
    # value, and every such character breaks the verbatim run the wrapped-rewording fallback needs.
    s = s.replace("`", "").replace("**", "")
    return s


def check_budgets(skill_dir: Path) -> list[str]:
    """Report any eagerly-read doc, or the group, over its byte budget."""
    budgets = BUDGETS.get(skill_dir.name)
    if not budgets:
        return []  # a skill with no declared budget is not a failure

    findings = []
    per_file = {name: cap for name, cap in budgets.items() if name != "*"}
    total = 0
    for name, cap in per_file.items():
        doc = skill_dir / name
        if not doc.exists():
            findings.append(f"budget: {name} is budgeted but missing from {skill_dir.name}")
            continue
        size = doc.stat().st_size
        total += size
        if size > cap:
            findings.append(f"over budget: {name} is {size:,} bytes, cap {cap:,} (+{size - cap:,})")

    combined = budgets.get("*")
    if combined is not None and len(per_file) > 1 and total > combined:
        names = " + ".join(per_file)
        findings.append(f"over budget: {names} together are {total:,} bytes, cap {combined:,} (+{total - combined:,})")
    return findings


def check_structure(skill_dir: Path) -> list[str]:
    findings = []
    all_md = docs(skill_dir)

    for f in all_md:
        for label, target in LINK.findall(f.read_text()):
            clean = target.split("#")[0]
            if not clean:
                continue
            if not (f.parent / clean).resolve().exists():
                findings.append(f"broken link: {f.relative_to(skill_dir)} -> {target}  [{label}]")

    # Every doc must be REACHABLE from an entry point by following links — not merely linked from
    # somewhere. Two reference files that link only to each other both have an incoming link, and an
    # any-incoming-link check reports no orphan for either; a run starting from SKILL.md still cannot
    # reach them. Build the link graph and traverse it from the entry points instead.
    entry = {"SKILL.md", "GUIDELINES.md", "SMALL-CHARTS.md", "BESPOKE-SVG.md"}
    graph: dict[Path, set[Path]] = {}
    for f in all_md:
        targets: set[Path] = set()
        for _, target in LINK.findall(f.read_text()):
            clean = target.split("#")[0]
            if clean.endswith(".md"):
                p = (f.parent / clean).resolve()
                if p.exists():
                    targets.add(p)
        graph[f.resolve()] = targets
    frontier = [f.resolve() for f in all_md if f.name in entry]
    reachable = set(frontier)
    while frontier:
        new = graph.get(frontier.pop(), set()) - reachable
        reachable |= new
        frontier.extend(new)
    for f in all_md:
        if f.resolve() not in reachable:
            findings.append(f"orphan: {f.relative_to(skill_dir)} is not reachable from an entry point")

    return findings


def check_against(ref: str, skill_dir: Path, repo_root: Path) -> list[str]:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, "--", str(skill_dir.relative_to(repo_root))],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    old_md = [p for p in listing if p.endswith(".md")]
    if not old_md:
        return [f"no markdown found under {skill_dir.relative_to(repo_root)} at {ref}"]

    haystack: dict[str, str] = {}
    # A whole-file corpus per doc, of per-line-normalized text joined with single spaces. Line-level
    # matching alone is defeated by a reflow: a paragraph that wraps differently after a move keeps
    # every word and loses every line boundary, so each old physical line reports LOST while the
    # content sits right there. Substring membership in the joined corpus is what "the content
    # survived" actually means.
    corpora: list[tuple[str, str]] = []
    for f in docs(skill_dir):
        lines = f.read_text().split("\n")
        normed = [normalize(line) for line in lines]
        for n in normed:
            if len(n) >= MIN_LINE:
                haystack.setdefault(n, str(f.relative_to(skill_dir)))
        corpora.append((str(f.relative_to(skill_dir)), " ".join(n for n in normed if n)))

    findings = []
    keys = list(haystack)
    total = 0
    for path in old_md:
        old = subprocess.run(
            ["git", "show", f"{ref}:{path}"], cwd=repo_root, capture_output=True, text=True, check=True
        ).stdout
        for line in old.split("\n"):
            n = normalize(line)
            if len(n) < MIN_LINE:
                continue
            total += 1
            if n in haystack:
                continue
            if any(n in corpus for _, corpus in corpora):
                continue  # reflowed, not lost — the exact content survives across line boundaries
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
                continue
            # Reworded ACROSS a wrap or a restructure: the old doc carried the paragraph as one
            # physical line and its new home hard-wraps or condenses it, so no single new line is
            # close and the line-level match above misses. Measure what fraction of the old line's
            # characters survive, in order, as runs of >= 10 chars in some file's corpus — long
            # enough that common English fragments don't count, short enough that a rewrite's kept
            # phrases do. Calibrated on this skill's own split: restructured-but-retained lines score
            # 0.46-0.94, fabricated never-existed lines 0.22-0.33, so 0.40 separates them with margin
            # on both sides. At or above it the instruction was edited, not dropped — report where,
            # and let a human judge the edit. Below it, LOST means lost.
            best_frac, best_file = 0.0, ""
            for name, corpus in corpora:
                sm = difflib.SequenceMatcher(None, n, corpus, autojunk=False)
                kept = sum(b.size for b in sm.get_matching_blocks() if b.size >= 10)
                if kept / len(n) > best_frac:
                    best_frac, best_file = kept / len(n), name
            if best_frac >= 0.40:
                findings.append(
                    f"REWORDED  {path}\n    was: {line.strip()[:150]}\n    now: {best_file} "
                    f"({best_frac:.0%} of the old line survives in >=10-char runs)"
                )
            else:
                findings.append(f"LOST      {path}\n    {line.strip()[:200]}")
    print(f"compared {total} substantive lines from {len(old_md)} file(s) at {ref}")
    return findings


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--structure", action="store_true", help="check links, reachability and size budgets")
    ap.add_argument("--against", metavar="GIT_REF", help="check no instruction was dropped since GIT_REF")
    ap.add_argument(
        "--skill",
        default=DEFAULT_SKILL_DIR.name,
        help="which skill directory to check (default: this script's own). Accepts a sibling name "
        "like create-static-viz, or a path.",
    )
    args = ap.parse_args()

    skill_dir = Path(args.skill)
    if not skill_dir.is_absolute():
        candidate = SKILLS_ROOT / args.skill
        skill_dir = candidate if candidate.is_dir() else Path(args.skill).resolve()
    if not skill_dir.is_dir():
        ap.error(f"no such skill directory: {skill_dir}")
    repo_root = next(p for p in skill_dir.parents if (p / ".git").exists())
    print(f"checking {skill_dir.relative_to(repo_root)}")
    if not args.structure and not args.against:
        ap.error("pass --structure and/or --against <git-ref>")

    findings = []
    if args.structure:
        findings += check_structure(skill_dir)
        findings += check_budgets(skill_dir)
    if args.against:
        findings += check_against(args.against, skill_dir, repo_root)

    if findings:
        print(f"\n{len(findings)} finding(s):\n")
        for f in findings:
            print(f"  {f}")
        return 1
    print("ok — no findings")
    return 0


if __name__ == "__main__":
    sys.exit(main())
