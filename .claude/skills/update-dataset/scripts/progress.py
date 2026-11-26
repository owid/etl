"""Track the /update-dataset checklist in workbench/<short_name>/progress.md, and report what is still open.

    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py init <short_name>
    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py check <short_name> --before handoff
    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py check <short_name> --before merge
    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py check <short_name>

`init` copies every item of the skill's "Progress checklist" into progress.md (it refuses to overwrite an existing
file, so a resumed update keeps its state). Close an item by ticking it and writing what shows it was done after
" | evidence: ", for example:

    - [x] Run the empty-entity audit ... | evidence: workbench/<short_name>/empty_entities.md
    - [x] Grapher step: run + verify ... | evidence: N/A, the dataset has no grapher step
    - [x] Run `check-hardcoded-years` ... | evidence: skipped at the user's request (listed in the PR's open items)

Checks you tell the user you'll run go in the "Added during this update" section, as new `- [ ]` items.

`check` lists every item due by the given checkpoint that is still open, or ticked without evidence, and exits with 1
when there is any. The checkpoints are `<!-- checkpoint: <name> -->` lines in the skill's checklist: `--before handoff`
covers the skill items above the "handoff" marker, `--before merge` those above the "merge" marker, and no flag covers
them all. Items added during the update are always due. `check` also fails when progress.md lacks an item the skill's
checklist now has (an update resumed from a file written by an older version of the skill).
"""

import argparse
import re
import sys
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1] / "SKILL.md"
# The repository root (scripts/ -> update-dataset/ -> skills/ -> .claude/ -> root), so the script runs from anywhere.
WORKBENCH = Path(__file__).resolve().parents[4] / "workbench"
CHECKLIST_HEADING = "## Progress checklist"
ADDED_HEADING = "## Added during this update"
ITEM = re.compile(r"^- \[(?P<mark>[ xX])\] (?P<text>.+)$")
CHECKPOINT = re.compile(r"^<!-- checkpoint: (?P<name>[a-z]+) -->$")
# Evidence follows the item text after " | evidence: " (item texts contain dashes, so a dash can't mark it).
EVIDENCE_MARKER = " | evidence:"
EVIDENCE = re.compile(r" \| evidence: (?P<evidence>\S.*)$")


def checklist() -> tuple[list[str], dict[str, int]]:
    """Return the skill's checklist items in order, and the number of items above each checkpoint."""
    items, checkpoints, inside = [], {}, False
    for line in SKILL.read_text().splitlines():
        if line.startswith("## "):
            inside = line.startswith(CHECKLIST_HEADING)
            continue
        if not inside:
            continue
        if m := ITEM.match(line):
            items.append(m.group("text"))
        elif m := CHECKPOINT.match(line):
            checkpoints[m.group("name")] = len(items)
    assert items, f"No checklist items found under '{CHECKLIST_HEADING}' in {SKILL}."
    return items, checkpoints


def init(short_name: str) -> None:
    path = WORKBENCH / short_name / "progress.md"
    if path.exists():
        sys.exit(f"{path} already exists; keep it (resumed update), or delete it first for a fresh start.")
    items, _ = checklist()
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"# Progress: {short_name}", "", "## Checklist", ""]
    lines += [f"- [ ] {text}" for text in items]
    lines += ["", ADDED_HEADING, "", "<!-- Checks you told the user you'd run, as `- [ ] ...` items. -->", ""]
    path.write_text("\n".join(lines))
    print(f"Wrote {path} with {len(items)} items.")


def check(short_name: str, before: str | None) -> None:
    path = WORKBENCH / short_name / "progress.md"
    if not path.exists():
        sys.exit(f"{path} doesn't exist. Run `progress.py init {short_name}` first.")
    items, checkpoints = checklist()
    if before is not None and before not in checkpoints:
        sys.exit(f"Unknown checkpoint '{before}'; the skill's checklist defines: {', '.join(checkpoints)}.")
    # Skill items below the checkpoint aren't due yet; items added during the update always are.
    not_due = set(items[checkpoints[before] :]) if before is not None else set()

    # Read progress.md, keyed by item text without its evidence.
    rows = {}
    for line in path.read_text().splitlines():
        if m := ITEM.match(line):
            text = m.group("text")
            rows[text.split(EVIDENCE_MARKER)[0].rstrip()] = (m.group("mark") != " ", bool(EVIDENCE.search(text)))

    missing = [text for text in items if text not in rows]
    open_items = [text for text, (ticked, _) in rows.items() if not ticked and text not in not_due]
    no_evidence = [text for text, (ticked, evidence) in rows.items() if ticked and not evidence]
    for title, found in [
        ("Missing from progress.md (copy them in from the skill's checklist)", missing),
        ("Open", open_items),
        ("Ticked without evidence (add ' | evidence: <artifact, link or reason>')", no_evidence),
    ]:
        if found:
            print(f"{title} ({len(found)}):")
            for text in found:
                print(f"  - {text[:160]}")
    if missing or open_items or no_evidence:
        sys.exit(1)
    pending = sum(1 for text, (ticked, _) in rows.items() if not ticked and text in not_due)
    scope = f"due before '{before}' " if before is not None else ""
    print(f"Every item {scope}is closed with evidence." + (f" {pending} later item(s) still open." if pending else ""))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["init", "check"])
    parser.add_argument("short_name")
    parser.add_argument("--before", help="Only check the skill items above this checkpoint (handoff, merge).")
    args = parser.parse_args()
    if args.command == "init":
        init(args.short_name)
    else:
        check(args.short_name, args.before)


if __name__ == "__main__":
    main()
