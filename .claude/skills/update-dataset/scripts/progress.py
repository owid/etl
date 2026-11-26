"""Track the /update-dataset checklist in workbench/<short_name>/progress.md, and report what is still open.

    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py init <short_name>
    .venv/bin/python .claude/skills/update-dataset/scripts/progress.py check <short_name>

`init` copies every item of the skill's "Progress checklist" into progress.md (it refuses to overwrite an existing
file, so a resumed update keeps its state). Close an item by ticking it and writing what shows it was done after
" | evidence: ", for example:

    - [x] Run the empty-entity audit ... | evidence: workbench/<short_name>/empty_entities.md
    - [x] Grapher step: run + verify ... | evidence: N/A, the dataset has no grapher step
    - [x] Run `check-hardcoded-years` ... | evidence: skipped at the user's request (listed in the PR's open items)

Checks you tell the user you'll run go in the "Added during this update" section, as new `- [ ]` items.

`check` lists every item that is still open, or ticked without evidence, and exits with 1 when there is any.
"""

import argparse
import re
import sys
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1] / "SKILL.md"
WORKBENCH = Path("workbench")
CHECKLIST_HEADING = "## Progress checklist"
ADDED_HEADING = "## Added during this update"
ITEM = re.compile(r"^- \[(?P<mark>[ xX])\] (?P<text>.+)$")
# Evidence follows the item text after " | evidence: " (item texts contain dashes, so a dash can't mark it).
EVIDENCE = re.compile(r" \| evidence: (?P<evidence>\S.*)$")


def checklist_items() -> list[str]:
    """Return the items of the skill's progress checklist, in order."""
    items, inside = [], False
    for line in SKILL.read_text().splitlines():
        if line.startswith("## "):
            inside = line.startswith(CHECKLIST_HEADING)
            continue
        if inside and (m := ITEM.match(line)):
            items.append(m.group("text"))
    assert items, f"No checklist items found under '{CHECKLIST_HEADING}' in {SKILL}."
    return items


def init(short_name: str) -> None:
    path = WORKBENCH / short_name / "progress.md"
    if path.exists():
        sys.exit(f"{path} already exists; keep it (resumed update), or delete it first for a fresh start.")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"# Progress: {short_name}", "", "## Checklist", ""]
    lines += [f"- [ ] {text}" for text in checklist_items()]
    lines += ["", ADDED_HEADING, "", "<!-- Checks you told the user you'd run, as `- [ ] ...` items. -->", ""]
    path.write_text("\n".join(lines))
    print(f"Wrote {path} with {len(checklist_items())} items.")


def check(short_name: str) -> None:
    path = WORKBENCH / short_name / "progress.md"
    if not path.exists():
        sys.exit(f"{path} doesn't exist. Run `progress.py init {short_name}` first.")
    open_items, no_evidence = [], []
    for line in path.read_text().splitlines():
        if not (m := ITEM.match(line)):
            continue
        text = m.group("text")
        if m.group("mark") == " ":
            open_items.append(text)
        elif not EVIDENCE.search(text):
            no_evidence.append(text)
    for title, items in [
        ("Open", open_items),
        ("Ticked without evidence (add ' | evidence: <artifact, link or reason>')", no_evidence),
    ]:
        if items:
            print(f"{title} ({len(items)}):")
            for text in items:
                print(f"  - {text[:160]}")
    if open_items or no_evidence:
        sys.exit(1)
    print("Every item is closed with evidence.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["init", "check"])
    parser.add_argument("short_name")
    args = parser.parse_args()
    {"init": init, "check": check}[args.command](args.short_name)


if __name__ == "__main__":
    main()
