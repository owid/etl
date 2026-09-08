#!/usr/bin/env python3
"""Emit one of this skill's `.js` helpers ready to paste into `use_figma`'s `code` argument.

Two problems this solves, and the first one is a hard blocker:

  * **`verify_page.js` does not fit.** `use_figma` caps `code` at 50,000 characters and that file is
    ~134,000 — so Step 8c's "runs the mechanical rows in ONE read-only call" is impossible as written.
    Stripped of comments it is ~68,000, which does not fit either, so the pass runs as the three
    `--rows` slices CHECKS.md prescribes; the largest of them relays 90% of the cap verbatim.
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
        --rows type,geometry                                    # groups combine

Stripping is context-aware, not a regex. These files contain `https://` inside strings, regex
literals holding `/*`, and template literals spanning lines — a naive `//`-to-end-of-line strip
corrupts all three, and the corruption surfaces as a plugin syntax error with no clue to its cause.
`--check` reports sizes for every script without printing any, and exits 1 if one still overflows.
"""

from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
CAP = 50_000
# Above this fraction of the cap, a sliceable script refuses to emit whole (see main()).
CROWDED = 0.80
# `// #region name` ... `// #endregion` mark independently-emittable row groups. Kept through
# stripping, because --rows selects on them.
REGION = re.compile(r"^//\s*#(region\s+\S+|endregion)\s*$")

# The calls reference/CHECKS.md tells an operator to send, per script. `--check` measures THESE.
# Keep the two in step: this list is the workflow, and the doc is where a human reads it.
DOCUMENTED_CALLS: dict[str, list[tuple[str, ...]]] = {
    # Rebalanced when the declared-gap rows moved into their own `skipped` region. Those rows are
    # pure prose and were being re-sent by all three calls, which is what held the largest slice at
    # 90% of the cap: the shared floor was 26,899 characters, of which 13,217 was text nobody needed
    # three copies of. Splitting it out took the floor to ~15,700 and the worst call from 90% to 70%.
    "verify_page.js": [("annotations",), ("type", "geometry"), ("series", "skipped")],
}


def strip_js(src: str) -> str:
    """Remove comments while respecting string, template-literal and regex context."""
    out: list[str] = []
    i, n = 0, len(src)
    # A STACK, not a single context, because a template literal can hold a `${}` expression that
    # holds another template. That nesting is ordinary JS, and a flat context could not see it: the
    # INNER opening backtick looked like the outer one closing, so from there the parser was
    # one level out of step and stripped template text as if it were code. Balanced nesting left the
    # final context at None, so the end-of-file guard passed it — the emitted script differed from
    # the file on disk, silently, which is the one failure this stripper exists to prevent.
    # "code" is the base level; "expr" is the inside of a `${}`, which is code too — a comment there
    # IS a comment — and it ends at the brace that matches its own, so its depth is tracked.
    stack: list[str] = ["code"]  # "code" | "expr" | "'" | '"' | "`" | "regex"
    depth: list[int] = []  # brace depth within each "expr" entry, innermost last

    while i < n:
        ch = src[i]
        nxt = src[i + 1] if i + 1 < n else ""

        if stack[-1] in ("code", "expr"):
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
                stack.append(ch)
            elif ch == "/" and _regex_allowed(out):
                stack.append("regex")
            elif stack[-1] == "expr" and ch == "{":
                depth[-1] += 1
            elif stack[-1] == "expr" and ch == "}":
                if depth[-1]:
                    depth[-1] -= 1
                else:
                    stack.pop()
                    depth.pop()
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
        # `${` opens an expression INSIDE the template: code again, one level down.
        if stack[-1] == "`" and ch == "$" and nxt == "{":
            stack.append("expr")
            depth.append(0)
            out.append(ch)
            out.append(nxt)
            i += 2
            continue
        if (stack[-1] == "regex" and ch == "/") or (stack[-1] != "regex" and ch == stack[-1]):
            stack.pop()
        elif stack[-1] in ("'", '"') and ch == "\n":
            stack.pop()  # unterminated string; bail rather than swallow the file
        out.append(ch)
        i += 1

    lines = [ln.rstrip() for ln in "".join(out).split("\n")]
    stripped = "\n".join(ln for ln in lines if ln.strip())

    # Ending inside a literal means the parse desynchronized and the rest of the file was copied
    # verbatim — the failure this stripper exists to avoid, and it is SILENT: the output is still
    # valid JS, just far larger, so it gets sent and the size guard is what eventually notices.
    # It happened once already: `--check` jumped 75% -> 96% of cap on a 1.8KB edit, which is the only
    # reason it was caught.
    #
    # The signature is the OPEN CONTEXT, not a surviving comment. An earlier version of this guard
    # flagged any `//` line that came through, which is wrong: a template literal may legitimately
    # contain one, the stripper handles that correctly, and the guard then called correct behaviour
    # corruption. Well-formed JS always closes its literals, so this has no false positive to trade
    # away — but it is only a backstop, and a BALANCED nested template closes everything it opens.
    # That case is parsed rather than caught, by the stack above; the guard is what is left for
    # genuinely unterminated source.
    if len(stack) > 1:
        kind = {
            "regex": "regex literal",
            "`": "template literal",
            "'": "string",
            '"': "string",
            "expr": "template ${} expression",
        }[stack[-1]]
        raise ValueError(
            f"comment stripping failed: the file ends inside an unterminated {kind}, so the parser "
            "lost sync and the rest of the source was copied verbatim. Check for an unclosed "
            "backtick, quote, brace or regex literal."
        )
    return stripped


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


def split_advice(stripped: str, groups: list[str], floor: int) -> str | None:
    """What to tell an operator whose script is over the cap — or None when there is nothing to say.

    Three genuinely different answers, and the middle one used to be given for all three. Past the
    FLOOR every 2-way split is over the cap by construction, so naming the smallest of them as "a
    split that fits" contradicts the exhaustion line printed beside it and sends the reader off to
    rewrite CHECKS.md around a call that would be refused.
    """
    if len(groups) < 2:
        return None
    size, left = min(
        (
            max(
                len(select_rows(stripped, set(sub))[0]),
                len(select_rows(stripped, set(groups) - set(sub))[0]),
            ),
            sub,
        )
        for sub in _proper_subsets(groups)
    )
    right = [g for g in groups if g not in left]
    if size <= CAP:
        return (
            f"a 2-way split that fits: --rows {','.join(left)} then --rows {','.join(right)} "
            f"(larger call {size:,}) — update CHECKS.md and DOCUMENTED_CALLS together"
        )
    if floor <= CAP:
        return (
            f"no 2-way split fits — the best is --rows {','.join(left)} / --rows {','.join(right)} "
            f"at {size:,}. The floor is {floor:,}, under the {CAP:,} cap, so a finer split does: "
            "try three calls."
        )
    # Floor over the cap: the line above already says no split fits and names the only remedy left.
    return None


def _proper_subsets(groups: list[str]) -> Iterator[tuple[str, ...]]:
    """Every way to split `groups` into two non-empty calls, as the left half of each split."""
    for size in range(1, len(groups)):
        yield from itertools.combinations(groups, size)


def _documented_problem(groups: list[str], calls: list[tuple[str, ...]]) -> str | None:
    """A declared workflow must send every row group exactly once, or the measurement is a fiction.

    Without this, a `#region` added to a script and not written into the call list is simply absent
    from what --check measures: the number stays comfortable while a row nobody sends goes unchecked
    in the frame. That is the same wrong-answer shape as measuring an undocumented split.
    """
    flat = [g for call in calls for g in call]
    parts = []
    if missing := [g for g in groups if g not in flat]:
        parts.append(f"in the file but never sent: {', '.join(missing)}")
    if unknown := [g for g in flat if g not in groups]:
        parts.append(f"documented but not in the file: {', '.join(unknown)}")
    if repeated := sorted({g for g in flat if flat.count(g) > 1}):
        parts.append(f"sent by more than one call: {', '.join(repeated)}")
    return "; ".join(parts) or None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("script", nargs="?", help="e.g. verify_page.js")
    ap.add_argument("--check", action="store_true", help="report sizes for every script; print none")
    ap.add_argument(
        "--rows",
        help="comma-separated row groups to keep (verify_page.js: type, series, geometry, "
        "annotations, skipped). Omit for the whole file. Use --list-rows to see them with their sizes.",
    )
    ap.add_argument("--list-rows", action="store_true", help="list a script's row groups and exit")
    ap.add_argument(
        "--frame-id",
        help="rewrite CONFIG.frameId in the emitted source, so the output is ready to send as-is",
    )
    ap.add_argument(
        "--config",
        action="append",
        metavar="KEY=VALUE",
        help="rewrite any other CONFIG key in the emitted source, repeatable "
        "(e.g. --config faceted=true --config chartName=chart-body). Bare true/false/null and "
        "numbers are emitted as literals; anything else is quoted as a string. An unknown key is "
        "an error, not a no-op — a silently ignored flag means running the pass with the wrong "
        "frame facts and never being told.",
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
        # #region markers is a slice, not the whole file. Judging a sliceable script on its total
        # would report a failure for a file that runs perfectly — verify_page.js is now PAST the cap
        # whole, and that changes nothing about how it is used, because the whole-file path is
        # already refused above CROWDED.
        #
        # But the largest SINGLE group is not the answer either: `--rows` combines groups, and the
        # documented pass covers all of them in three calls (CHECKS.md), so nobody sends one slice at
        # a time. So measure THE DOCUMENTED CALLS, the ones an operator is actually told to send —
        # not the best split this script can find for itself. Minimising over every partition answers
        # a question nobody asked, and it can hold the check green by discovering an undocumented
        # split while the workflow in the doc is over the cap. That is not hypothetical: growth in
        # `annotations` took the then-documented `geometry,annotations` call to 50,594 — over the cap
        # and refused — while `annotations` alone measured 45,163, so an optimiser would have reported
        # 90% and exited 0 for a workflow that no longer ran. This check failed instead, and the doc
        # and the list above were re-split to three calls. The number has to be the one the operator
        # will hit.
        failed = False
        print(f"{'script':<30} {'raw':>8} {'stripped':>9} {'sent':>9} {'of cap':>8} {'floor':>8}")
        for p in sorted(SCRIPTS.glob("*.js")):
            if p.name.startswith("test_"):
                continue
            stripped = strip_js(p.read_text())
            groups = list(dict.fromkeys(select_rows(stripped, set())[1]))
            documented = DOCUMENTED_CALLS.get(p.name)
            problem = None
            if documented is not None:
                problem = _documented_problem(groups, documented)
                sizes = [(call, len(select_rows(stripped, set(call))[0])) for call in documented]
                sent = max(size for _, size in sizes)
                how = (
                    "  (largest of the documented calls: "
                    + ", ".join(f"--rows {','.join(call)} {size:,}" for call, size in sizes)
                    + ")"
                )
            elif len(groups) > 1:
                # A sliceable script with no declared workflow is itself the gap: measuring it on
                # some split of this tool's choosing would report a size nobody sends.
                problem = (
                    f"{len(groups)} row groups but no entry in DOCUMENTED_CALLS — add the calls "
                    "CHECKS.md gives operators, so this measures what they send"
                )
                sent = len(select_rows(stripped, set(groups))[0])
                how = "  (every slice at once — no documented split to measure)"
            elif groups:
                sent = len(select_rows(stripped, set(groups))[0])
                how = "  (its only slice)"
            else:
                sent = len(stripped)
                how = ""
            # The FLOOR: preamble + the single largest group, i.e. the smallest any call can ever be
            # made by re-splitting. `sent` says whether today's documented calls fit; the floor says
            # whether ANY partition can. They move independently, and confusing them misreads the
            # risk in both directions — verify_page.js sat at 82% sent against a 72% floor, so it
            # looked ~9,000 characters from trouble when re-splitting bought it ~14,000.
            #
            # Once the floor passes the cap, slicing is exhausted: the shared preamble plus one row
            # group no longer fits, and the only remaining move is to break the script into separate
            # files with their own preambles. That is a rewrite, so it wants warning, not discovery.
            floor = max((len(select_rows(stripped, {g})[0]) for g in groups), default=len(stripped))
            over = sent > CAP
            failed = failed or over or bool(problem)
            print(
                f"{p.name:<30} {len(p.read_text()):>8,} {len(stripped):>9,} {sent:>9,} "
                f"{sent / CAP * 100:>7.0f}% {floor / CAP * 100:>7.0f}%"
                f"{'  OVER CAP' if over else ''}{how}"
            )
            if problem:
                print(f"{'':<30}   ^ {problem}")
            if groups and floor > CAP:
                failed = True
                print(
                    f"{'':<30}   ^ FLOOR OVER CAP: preamble + the largest single group is {floor:,}, "
                    "so no split fits. Move rows into a separate script with its own preamble."
                )
            elif groups and floor > CAP * 0.85:
                print(
                    f"{'':<30}   ^ floor at {floor / CAP * 100:.0f}% — re-splitting is nearly "
                    f"exhausted; {CAP - floor:,} characters of shared preamble + largest group left."
                )
            # Over the cap, the useful next step is the split that would fit, so the doc can be
            # rewritten to it. Enumerating the subsets is fine here: these are hand-authored #region
            # markers and there is a handful of them per script.
            if over:
                advice = split_advice(stripped, groups, floor)
                if advice:
                    print(f"{'':<30}   ^ {advice}")
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
        # A slice can only fail the rows it carries, so its verdict has to say which those were.
        # Without this, "no mechanical row failed" out of ONE documented call reads as a verdict on
        # the frame — the same confident silence the SKIPPED rows exist to prevent, reached by
        # slicing rather than by not looking. REQUIRED rather than best-effort: a sliceable script
        # with no declaration cannot produce an honest partial verdict, and discovering that from a
        # misleading result later is exactly the failure being closed here.
        emitted = [g for g in dict.fromkeys(groups) if g in wanted]
        stripped, n_rows = re.subn(
            r"(const EMITTED_ROWS = )\[[^\]]*\];",
            lambda m: m.group(1) + json.dumps(emitted) + ";",
            stripped,
            count=1,
        )
        if not n_rows:
            print(
                f"{args.script} carries #region markers but declares no EMITTED_ROWS, so a slice of it "
                "cannot report which rows it left out. Add "
                '`const EMITTED_ROWS = [...];` beside its CONFIG.',
                file=sys.stderr,
            )
            return 1

    if args.frame_id:
        stripped, n_sub = re.subn(
            r'(frameId:\s*)"[^"]*"', lambda m: f'{m.group(1)}"{args.frame_id}"', stripped, count=1
        )
        if not n_sub:
            print(f"--frame-id given but {args.script} has no CONFIG.frameId to rewrite", file=sys.stderr)
            return 1

    if args.config:
        # Scope every substitution to the CONFIG object. A bare `key:` pattern would also match an
        # object literal further down the file, which is how a "config" flag quietly edits a row's
        # internals instead of its configuration.
        block = re.search(r"const CONFIG = \{.*?^\};", stripped, re.S | re.M)
        if not block:
            print(f"--config given but {args.script} has no CONFIG block to rewrite", file=sys.stderr)
            return 1
        body = block.group(0)
        for pair in args.config:
            if "=" not in pair:
                ap.error(f"--config expects KEY=VALUE, got {pair!r}")
            key, _, val = pair.partition("=")
            key, val = key.strip(), val.strip()
            # ONE json round-trip covers every shape CONFIG holds, and json.dumps is what makes it
            # safe: JSON's syntax for strings, numbers, booleans, null and arrays is a subset of
            # JavaScript's, and the emitted slice is pasted STRAIGHT into use_figma. Hand-building a
            # pair of quotes instead broke both ends of that. A value carrying a double quote or a
            # backslash (`--config 'chartName=A "quoted" chart'`) closed the string early and reached
            # the plugin as a syntax error whose cause is nowhere in the message. And an ARRAY —
            # `gapTarget=[12,16]`, `frameIds=["1:2","3:4"]`, both documented as arrays in
            # verify_page.js's header — was quoted into a STRING, which is worse than an error
            # because it runs: `gapTarget` is truthy so it survives its `||` default and then indexes
            # character-by-character into "[", "1"; `frameIds` fails Array.isArray and is silently
            # dropped back to the single frameId. A parse failure means it was never JSON, so it is a
            # bare string and gets quoted as one.
            try:
                val = json.dumps(json.loads(val))
            except json.JSONDecodeError:
                val = json.dumps(val.strip("\"'"))
            body, n = re.subn(
                # The value runs to the LAST comma on its line, not the first: `[12, 16]` contains
                # one, and a first-comma match would rewrite `gapTarget: [12,` and leave ` 16],`
                # stranded behind it. Greedy to end-of-line, then backtrack to the separator.
                rf"(\n\s*{re.escape(key)}:\s*)[^\n]*(,)(?=[^\n]*\n)",
                lambda m: f"{m.group(1)}{val}{m.group(2)}",
                body,
                count=1,
            )
            if not n:
                ap.error(
                    f"--config {key}: no such key in {args.script}'s CONFIG. "
                    "Rewriting a key that is not there would emit a script running on defaults."
                )
        stripped = stripped[: block.start()] + body + stripped[block.end() :]

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
