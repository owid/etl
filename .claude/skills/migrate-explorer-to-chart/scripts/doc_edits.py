"""Write the gdoc URL swaps for an explorer migration as a review file, one section per doc.

Reads the `references.csv` that `map-explorer-to-mdim/scripts/audit_references.py` writes and
writes `doc_edits.md` next to it: per doc, each reference with where to find it, the old URL and
the new URL, in copyable code blocks. Read-only.

Usage:
    .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/doc_edits.py \
        --references ai/<slug>-redirects/references.csv
"""

import argparse
from pathlib import Path

import pandas as pd

# Embeds break the moment the redirect exists; everything else keeps working behind the 302.
COMPONENT_LABELS = {
    "chart": "Embedded explorer (breaks when the redirect is created)",
    "prominent-link": "Prominent link (breaks when the redirect is created)",
    "span-link": "Text link (covered by the redirect)",
}


def main(references: Path) -> None:
    refs = pd.read_csv(references).fillna("")
    lines = [
        "# Doc edits",
        "",
        "Replace each old URL with the new one exactly as given (they keep each embed's entities "
        "and view), then republish the doc from the admin. Do this after the multidim is "
        "published on production and before the redirects are created.",
        "",
    ]
    for where, group in refs.groupby("where", sort=False):
        first = group.iloc[0]
        lines += [f"## {where}", ""]
        links = [f"[doc]({first['doc_edit_url']})", f"[page]({first['where_url']})"]
        if first["doc_preview_url"]:
            links.insert(1, f"[preview]({first['doc_preview_url']})")
        lines += [" · ".join(links), ""]
        for i, r in enumerate(group.to_dict("records"), start=1):
            label = COMPONENT_LABELS.get(r["component"], r["component"])
            lines += [f"### {i}. {label}", ""]
            if r["find_in_doc"]:
                lines += [f"Find in doc: `{r['find_in_doc']}`", ""]
            lines += ["Old:", "```", r["old_url"], "```", "New:", "```", r["replacement_url"], "```", ""]
    out = references.with_name("doc_edits.md")
    out.write_text("\n".join(lines))
    print(f"{len(refs)} reference(s) in {refs['where'].nunique()} doc(s) -> {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--references", type=Path, required=True, help="references.csv from audit_references.py")
    main(ap.parse_args().references)
