"""Build the Google Docs suggestions that swap a doc's explorer links for their multidim links.

Reads a doc as `documents.get` JSON (what the Google Docs connector's `read_doc` returns) and the
review's `references_full.json`, and writes the `documents.batchUpdate` requests to send with
`writeControl.writeMode: "SUGGEST"`, so every change lands as a suggestion the doc's owner accepts
or rejects. Nothing here writes to the doc.

Two kinds of occurrence:
- the URL as visible text (chart blocks, prominent links, `url:` lines): the new URL is inserted
  right after the old one, then the old one is deleted, which shows as a single replacement;
- the URL as a link target (span links): the link is restyled to the new URL, keeping its text.

Requests are ordered from the end of the doc backwards, so no request shifts the indices of the
ones after it.

Usage:
    gdoc_suggestions.py --doc <read_doc output> --references <review>/references_full.json --out <requests.json>
"""

import argparse
import json
from pathlib import Path

# Characters that can continue a URL: an occurrence followed by one of them is a longer URL
# (e.g. the bare explorer URL inside a URL with params), not this one.
URL_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~:/?#[]@!$&'*+,;=%")


def text_runs(doc: dict) -> list[dict]:
    """Every text run of every tab, with its index range, text and link target."""
    out: list[dict] = []

    def walk(elems, tab):
        for el in elems or []:
            if "paragraph" in el:
                for pe in el["paragraph"].get("elements", []):
                    tr = pe.get("textRun")
                    if tr:
                        style = tr.get("textStyle") or {}
                        out.append(
                            {
                                "tab": tab,
                                "start": pe["startIndex"],
                                "end": pe["endIndex"],
                                "text": tr["content"],
                                "link": (style.get("link") or {}).get("url"),
                                "suggested": bool(tr.get("suggestedInsertionIds") or tr.get("suggestedDeletionIds")),
                            }
                        )
            if "table" in el:
                for row in el["table"]["tableRows"]:
                    for cell in row["tableCells"]:
                        walk(cell["content"], tab)

    def tabs(ts):
        for t in ts:
            walk(t["documentTab"]["body"]["content"], t["tabProperties"]["tabId"])
            tabs(t.get("childTabs", []))

    tabs(doc["tabs"])
    return out


def text_occurrences(run: dict, url: str) -> list[int]:
    """Offsets of `url` in the run's text where it is the whole URL, not the start of a longer one."""
    found, i = [], run["text"].find(url)
    while i != -1:
        after = run["text"][i + len(url) : i + len(url) + 1]
        if not after or after not in URL_CHARS:
            found.append(i)
        i = run["text"].find(url, i + 1)
    return found


def build_requests(doc: dict, swaps: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """The batchUpdate requests for `swaps` ({old URL: new URL}), and a log of what each one does."""
    edits = []  # (start index, request(s), log entry)
    for run in text_runs(doc):
        if run["suggested"]:
            # Already part of an open suggestion (e.g. from an earlier run): leave it to the owner.
            continue
        for old, new in swaps.items():
            if run["link"] == old:
                rng = {"startIndex": run["start"], "endIndex": run["end"], "tabId": run["tab"]}
                req = {"updateTextStyle": {"range": rng, "textStyle": {"link": {"url": new}}, "fields": "link"}}
                edits.append((run["start"], [req], {"kind": "link", "text": run["text"], "old": old, "new": new}))
            for off in text_occurrences(run, old):
                start = run["start"] + off
                end = start + len(old)
                reqs = [
                    {"insertText": {"text": new, "location": {"index": end, "tabId": run["tab"]}}},
                    {"deleteContentRange": {"range": {"startIndex": start, "endIndex": end, "tabId": run["tab"]}}},
                ]
                edits.append((start, reqs, {"kind": "text", "old": old, "new": new}))
    edits.sort(key=lambda e: e[0], reverse=True)
    return [r for _, reqs, _ in edits for r in reqs], [log for _, _, log in edits]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--doc", required=True, type=Path, help="read_doc output (JSON)")
    ap.add_argument("--references", required=True, type=Path, help="references_full.json from scan_references.py")
    ap.add_argument("--out", required=True, type=Path, help="where to write the requests")
    args = ap.parse_args()

    raw = json.loads(args.doc.read_text())
    doc = raw.get("content", raw)
    refs = json.loads(args.references.read_text())["references"]
    swaps = {
        r["old_url"]: r["new_url"]
        for r in refs
        if r["surface"] == "Google Doc" and r.get("edit_url") and doc["documentId"] in r["edit_url"]
    }
    requests, log = build_requests(doc, swaps)
    args.out.write_text(json.dumps({"documentId": doc["documentId"], "requiredRevisionId": doc["revisionId"], "requests": requests}, indent=1))

    print(f"{doc['title']} ({doc['documentId']})")
    print(f"  {len(swaps)} link(s) in references, {len(log)} occurrence(s) found, {len(requests)} request(s)")
    for entry in log:
        print(f"  [{entry['kind']}] {entry['old']}\n      -> {entry['new']}")
    missing = set(swaps) - {e["old"] for e in log}
    for old in sorted(missing):
        print(f"  NOT FOUND (already swapped, or the doc changed since the scan): {old}")


if __name__ == "__main__":
    main()
