#!/usr/bin/env python3
"""Emit one of this skill's `.js` helpers ready to paste into `use_figma`'s `code` argument.

Two problems this solves, and the first one is a hard blocker:

  * **`verify_page.js` does not fit.** `use_figma` caps `code` at 50,000 characters and that file is
    ~79,000 — so Step 8c's "runs the mechanical rows in ONE read-only call" is impossible as written.
    Stripped of comments it is ~45,000 and fits.
  * **Reading a script to paste it costs the model context twice** — once for the Read, once for the
    tool call. Piping this straight into the call skips the Read entirely.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/inline_script.py verify_page.js

Stripping is context-aware, not a regex. These files contain `https://` inside strings, regex
literals holding `/*`, and template literals spanning lines — a naive `//`-to-end-of-line strip
corrupts all three, and the corruption surfaces as a plugin syntax error with no clue to its cause.
`--check` reports sizes for every script without printing any, and exits 1 if one still overflows.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
CAP = 50_000


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
                while i < n and src[i] != "\n":
                    i += 1
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
    args = ap.parse_args()

    if args.check:
        worst = 0
        print(f"{'script':<30} {'raw':>8} {'stripped':>9} {'of cap':>8}")
        for p in sorted(SCRIPTS.glob("*.js")):
            if p.name.startswith("test_"):
                continue
            raw = p.read_text()
            stripped = strip_js(raw)
            pct = len(stripped) / CAP * 100
            flag = "  OVER CAP" if len(stripped) > CAP else ""
            worst = max(worst, len(stripped))
            print(f"{p.name:<30} {len(raw):>8,} {len(stripped):>9,} {pct:>7.0f}%{flag}")
        return 1 if worst > CAP else 0

    if not args.script:
        ap.error("give a script name, or --check")
    path = SCRIPTS / args.script
    if not path.exists():
        ap.error(f"no such script: {path}")

    stripped = strip_js(path.read_text())
    if len(stripped) > CAP:
        print(
            f"{args.script} is {len(stripped):,} chars stripped, over the {CAP:,} cap — "
            "it cannot be inlined and must be split.",
            file=sys.stderr,
        )
        return 1
    print(stripped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
