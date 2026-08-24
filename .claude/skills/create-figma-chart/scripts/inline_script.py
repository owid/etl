#!/usr/bin/env python3
"""Emit one of this skill's `.js` helpers ready to paste into `use_figma`'s `code` argument.

Two problems this solves, and the first one is a hard blocker:

  * **`verify_page.js` does not fit.** `use_figma` caps `code` at 50,000 characters and that file is
    ~79,000 — so Step 8c's "runs the mechanical rows in ONE read-only call" is impossible as written.
    Stripped of comments it is ~45,000, which fits but leaves you relaying 90% of the cap verbatim.
  * **Reading a script to paste it costs the model context twice** — once for the Read, once for the
    tool call. Piping this straight into the call skips the Read entirely.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js

`--rows` is the answer to the size, and it is a slice rather than a rewrite: the rows already sit in
bare `{ }` blocks above a shared preamble, so `// #region` markers select whole blocks and every
slice carries the preamble and runs alone. Nothing was moved, so the harness still tests the one
file. `--list-rows` prints the groups with their sizes; `--frame-id` rewrites `CONFIG.frameId` so
the output can go straight into the call.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js \
        --rows series --frame-id 26417:6
    .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js \
        --rows type,series                                      # groups combine

Stripping is context-aware, not a regex. These files contain `https://` inside strings, regex
literals holding `/*`, and template literals spanning lines — a naive `//`-to-end-of-line strip
corrupts all three, and the corruption surfaces as a plugin syntax error with no clue to its cause.
`--check` reports sizes for every script without printing any, and exits 1 if one still overflows.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
CAP = 50_000
# Above this fraction of the cap, a sliceable script refuses to emit whole (see main()).
CROWDED = 0.80
# `// #region name` ... `// #endregion` mark independently-emittable row groups. Kept through
# stripping, because --rows selects on them.
REGION = re.compile(r"^//\s*#(region\s+\S+|endregion)\s*$")


def strip_js(src: str) -> str:
    """Remove comments while respecting string, template-literal and regex context."""
    out: list[str] = []
    i, n = 0, len(src)
    # None | "'" | '"' | '`' | 'regex'
    context: str | None = None

    while i < n:
        ch = src[i]
        nxt = src[i + 1] if i + 1 < n else ""

        if context is None:
            if ch == "/" and nxt == "/":
                line_end = src.find("\n", i)
                line_end = n if line_end < 0 else line_end
                # Region markers are structure, not commentary: --rows needs them to survive.
                if REGION.match(src[i:line_end]):
                    out.append(src[i:line_end])
                i = line_end
                continue
            if ch == "/" and nxt == "*":
                i += 2
                while i + 1 < n and not (src[i] == "*" and src[i + 1] == "/"):
                    i += 1
                i += 2
                continue
            if ch in "'\"`":
                context = ch
            elif ch == "/" and _regex_allowed(out):
                context = "regex"
            out.append(ch)
            i += 1
            continue

        # Inside a string, template or regex: copy verbatim, honouring escapes.
        if ch == "\\":
            out.append(ch)
            if i + 1 < n:
                out.append(src[i + 1])
            i += 2
            continue
        if (context == "regex" and ch == "/") or (context != "regex" and ch == context):
            context = None
        elif context in ("'", '"') and ch == "\n":
            context = None  # unterminated string; bail rather than swallow the file
        out.append(ch)
        i += 1

    lines = [ln.rstrip() for ln in "".join(out).split("\n")]
    return "\n".join(ln for ln in lines if ln.strip())


def select_rows(src: str, wanted: set[str]) -> tuple[str, list[str]]:
    """Keep everything outside regions, plus the regions named. Unmarked files come back whole.

    The preamble (frame resolution, the node walk, the shared boxes and bands) sits outside every
    region, so each slice is self-contained and runs on its own.
    """
    kept, found, skipping = [], [], False
    for line in src.split("\n"):
        m = REGION.match(line.strip())
        if m:
            if m.group(1) == "endregion":
                skipping = False
            else:
                name = m.group(1).split()[1]
                found.append(name)
                skipping = name not in wanted
            continue
        if not skipping:
            kept.append(line)
    return "\n".join(kept), found


def _regex_allowed(out: list[str]) -> bool:
    """A `/` starts a regex only where a value is expected — after an operator or an opener."""
    for ch in reversed(out):
        if ch in " \t\n":
            continue
        return ch in "(,=:[!&|?{};+-*%~^<>"
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("script", nargs="?", help="e.g. verify_page.js")
    ap.add_argument("--check", action="store_true", help="report sizes for every script; print none")
    ap.add_argument(
        "--rows",
        help="comma-separated row groups to keep (verify_page.js: type, series, geometry, "
        "annotations). Omit for the whole file. Use --list-rows to see them with their sizes.",
    )
    ap.add_argument("--list-rows", action="store_true", help="list a script's row groups and exit")
    ap.add_argument(
        "--frame-id",
        help="rewrite CONFIG.frameId in the emitted source, so the output is ready to send as-is",
    )
    ap.add_argument(
        "--whole",
        action="store_true",
        # argparse runs help through %-formatting, so a literal percent sign must be doubled.
        help=f"emit a sliceable script whole even when it is over {int(CROWDED * 100)}%% of the cap",
    )
    args = ap.parse_args()

    if args.check:
        # What matters is the largest thing anyone actually SENDS, which for a script carrying
        # #region markers is its biggest slice, not the whole file. Judging a sliceable script on
        # its total would report a failure for a file that runs perfectly — verify_page.js sits a
        # few hundred characters under the cap whole, and crossing it changes nothing about how it
        # is used, because the whole-file path is already refused above CROWDED.
        failed = False
        print(f"{'script':<30} {'raw':>8} {'stripped':>9} {'sent':>9} {'of cap':>8}")
        for p in sorted(SCRIPTS.glob("*.js")):
            if p.name.startswith("test_"):
                continue
            stripped = strip_js(p.read_text())
            groups = list(dict.fromkeys(select_rows(stripped, set())[1]))
            if groups:
                sent = max(len(select_rows(stripped, {g})[0]) for g in groups)
                how = f"  (largest of {len(groups)} slices)"
            else:
                sent = len(stripped)
                how = ""
            over = sent > CAP
            failed = failed or over
            print(
                f"{p.name:<30} {len(p.read_text()):>8,} {len(stripped):>9,} {sent:>9,} "
                f"{sent / CAP * 100:>7.0f}%{'  OVER CAP' if over else ''}{how}"
            )
        return 1 if failed else 0

    if not args.script:
        ap.error("give a script name, or --check")
    path = SCRIPTS / args.script
    if not path.exists():
        ap.error(f"no such script: {path}")

    stripped = strip_js(path.read_text())

    if args.list_rows:
        _, groups = select_rows(stripped, set())
        if not groups:
            print(f"{args.script} has no #region markers — it is emitted whole ({len(stripped):,} chars)")
            return 0
        base = len(select_rows(stripped, set())[0])
        print(f"{args.script}: preamble {base:,} chars, plus each group:")
        for g in dict.fromkeys(groups):
            size = len(select_rows(stripped, {g})[0])
            print(f"  {g:<14} +{size - base:>6,}  = {size:>6,} chars  ({size / CAP * 100:>3.0f}% of cap)")
        return 0

    if args.rows:
        wanted = {w.strip() for w in args.rows.split(",") if w.strip()}
        stripped, groups = select_rows(stripped, wanted)
        unknown = wanted - set(groups)
        if unknown:
            ap.error(
                f"no such row group(s): {', '.join(sorted(unknown))}. Available: {', '.join(dict.fromkeys(groups))}"
            )

    if args.frame_id:
        stripped, n_sub = re.subn(
            r'(frameId:\s*)"[^"]*"', lambda m: f'{m.group(1)}"{args.frame_id}"', stripped, count=1
        )
        if not n_sub:
            print(f"--frame-id given but {args.script} has no CONFIG.frameId to rewrite", file=sys.stderr)
            return 1

    if len(stripped) > CAP:
        print(
            f"{args.script} is {len(stripped):,} chars stripped, over the {CAP:,} cap — "
            f"split it with --rows (see --list-rows).",
            file=sys.stderr,
        )
        return 1

    # Fitting is not the same as being safe to send. Near the cap the whole file has to be relayed
    # verbatim through the model, and a one-character corruption there yields a WRONG VERDICT rather
    # than an error — which is the failure this helper exists to prevent, not to enable. So refuse
    # the whole-file path once it is this close and point at the slices, which are half the size.
    if not args.rows and not args.whole and len(stripped) > CAP * CROWDED and select_rows(stripped, set())[1]:
        pct = len(stripped) / CAP * 100
        print(
            f"{args.script} is {len(stripped):,} chars stripped — {pct:.0f}% of the {CAP:,} cap.\n"
            "It fits, but relaying that much verbatim risks a silent corruption, and a corrupted\n"
            "check reports a wrong verdict rather than failing. Use --rows instead (--list-rows\n"
            "shows the groups; each is roughly half this). --whole overrides if you really mean it.",
            file=sys.stderr,
        )
        return 1

    print(stripped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
