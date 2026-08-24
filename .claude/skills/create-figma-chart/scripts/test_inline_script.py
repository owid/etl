#!/usr/bin/env python3
"""Harness for `inline_script.py`.

It had none, which is the wrong shape for a tool whose whole job is to decide what is safe to send:
a mistake here does not raise, it emits a *slightly different* script, and a corrupted check reports
a wrong verdict rather than failing. The cases below are the ones that have actually gone wrong.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/test_inline_script.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from inline_script import CAP, DOCUMENTED_CALLS, select_rows, split_advice, strip_js  # noqa: E402

results: list[tuple[str, bool, str]] = []


def check(name: str, cond: object, detail: object = "") -> None:
    results.append((name, bool(cond), str(detail)[:220]))


# --- stripping keeps what is NOT a comment -------------------------------------------------------
# Each of these three was called out in the module docstring as a thing a naive regex breaks, so
# they are the cases most worth pinning: the corruption they cause surfaces as a plugin syntax
# error with no clue to its cause.
src = """const url = "https://example.com/x"; // trailing comment
const re = /\\/\\*not a comment\\*\\//;
const tpl = `line one
// not a comment, inside a template
line two`;
/* block
   comment */
const after = 1;
"""
out = strip_js(src)
check("a URL's // survives", "https://example.com/x" in out, out)
check("a regex holding /* survives", "/\\/\\*not a comment\\*\\//" in out, out)
check("a template's // line survives", "// not a comment, inside a template" in out, out)
check("the trailing line comment is gone", "// trailing comment" not in out, out)
check("the block comment is gone", "block" not in out.replace("not a comment", ""), out)
check("code after a block comment survives", "const after = 1;" in out, out)

# --- nested templates, which are ordinary JS ------------------------------------------------------
# A template literal can hold a `${}` expression that holds another template. A flat context could
# not see that: the INNER opening backtick read as the outer one closing, and from there the parser
# was one level out of step. Balanced nesting still ended at context None, so the end-of-file guard
# passed it and the emitted script differed from the file on disk — silently, which is the one
# failure this stripper exists to prevent. Parsed with a stack, each level closes its own.
nested = 'const a = `x${c ? "" : ` inner \\`${y}\\``}`;\n// must be stripped\nconst b = 1;\n'
kept_nested = strip_js(nested)
check("a balanced nested template parses instead of refusing", "` inner" in kept_nested, kept_nested)
check("and the parser is still in sync after it", "// must be stripped" not in kept_nested, kept_nested)
check("so the code after it survives", "const b = 1;" in kept_nested, kept_nested)

# The case that made this visible: a `//` inside the INNER template is template text, not a comment,
# and the outer template resumes after the expression closes.
inner_comment = "const t = `outer ${`inner // keep\nline`} tail`;\n// gone\nconst z = 2;\n"
kept_inner = strip_js(inner_comment)
check("a // inside a nested template is not stripped", "// keep" in kept_inner, kept_inner)
check("the outer template's tail survives", "tail`" in kept_inner, kept_inner)
check("and the comment after it is still stripped", "// gone" not in kept_inner, kept_inner)

# Inside a `${}` the content is CODE, so a comment there is a comment and must go — the mirror of
# the case above, and the reason the expression is tracked rather than copied verbatim.
expr_comment = "const v = `a${ x /* drop */ }b`;\n"
check("a comment inside a ${} expression IS stripped", "drop" not in strip_js(expr_comment), strip_js(expr_comment))

# --- the corruption guard ------------------------------------------------------------------------
# Well-formed JS closes every literal it opens, so ending inside one means the parse desynchronized
# and the rest of the file was copied verbatim. That is the backstop the stack cannot replace.
for label, bad in [
    ("template", "const a = `unterminated\n// must be stripped\n"),
    ("${} expression", "const a = `x${ y\n// must be stripped\n"),
]:
    try:
        strip_js(bad)
        check(f"an unterminated {label} is REFUSED", False, "no exception raised")
    except ValueError as exc:
        check(f"an unterminated {label} is REFUSED", True, str(exc))
        check(f"and the {label} message names what is open", "unterminated" in str(exc), str(exc))

# Region markers are structure, not commentary — `--rows` selects on them, so they must survive.
regioned = "const pre = 1;\n// #region alpha\nconst a = 1; // gone\n// #endregion\n// #region beta\nconst b = 2;\n// #endregion\n"
kept = strip_js(regioned)
check("region markers survive stripping", "// #region alpha" in kept and "// #endregion" in kept, kept)
check("comments inside a region still go", "// gone" not in kept, kept)

# --- slicing -------------------------------------------------------------------------------------
preamble, groups = select_rows(kept, set())
check("groups are discovered in order", groups == ["alpha", "beta"], groups)
check(
    "an empty selection yields the preamble only",
    "const pre = 1;" in preamble and "const a = 1;" not in preamble,
    preamble,
)
one, _ = select_rows(kept, {"alpha"})
check("a slice carries the preamble", "const pre = 1;" in one, one)
check("a slice carries its own group", "const a = 1;" in one, one)
check("a slice excludes the others", "const b = 2;" not in one, one)
both, _ = select_rows(kept, {"alpha", "beta"})
check("groups combine", "const a = 1;" in both and "const b = 2;" in both, both)
check(
    "an unmarked file comes back whole",
    select_rows("const x = 1;\n", set())[0].strip() == "const x = 1;",
    select_rows("const x = 1;\n", set())[0],
)

# --- the floor metric ----------------------------------------------------------------------------
# `sent` answers "do today's documented calls fit". The FLOOR answers "can ANY split fit" — preamble
# plus the single largest group. They move independently: verify_page.js sat at 82% sent against a
# 72% floor, so it read as ~9,000 characters from trouble when re-splitting bought it ~14,000.
big = (
    "const pre = 1;\n"
    + "// #region alpha\n"
    + "const a = 1;\n" * 50
    + "// #endregion\n"
    + "// #region beta\n"
    + "const b = 2;\n" * 10
    + "// #endregion\n"
)
stripped_big = strip_js(big)
gs = select_rows(stripped_big, set())[1]
floor = max(len(select_rows(stripped_big, {g})[0]) for g in gs)
whole = len(select_rows(stripped_big, set(gs))[0])
check("the floor is the largest SINGLE group, not the total", floor < whole, f"floor {floor} whole {whole}")
check("the floor is preamble + largest group", floor == len(select_rows(stripped_big, {"alpha"})[0]), floor)
check("the floor never exceeds the whole file", floor <= whole, f"{floor} vs {whole}")


# --- what to advise when a script is over the cap -------------------------------------------------
# Three different situations, and one of them used to be given the middle answer: past the FLOOR
# every 2-way split is over the cap by construction, so naming the smallest of them as "a split that
# fits" contradicts the exhaustion line printed beside it and points at a call that would be refused.
def group_src(a: int, b: int) -> str:
    return (
        "const pre = 1;\n"
        + "// #region alpha\n"
        + "const a = 1;\n" * a
        + "// #endregion\n"
        + "// #region beta\n"
        + "const b = 2;\n" * b
        + "// #endregion\n"
    )


def advice_for(a: int, b: int) -> tuple[str | None, int]:
    kept_src = strip_js(group_src(a, b))
    gs = select_rows(kept_src, set())[1]
    fl = max(len(select_rows(kept_src, {g})[0]) for g in gs)
    return split_advice(kept_src, gs, fl), fl


# Two modest groups whose combined call is over the cap but either one alone is well under it.
fits, _ = advice_for(3_600, 3_600)
check("a split that fits is offered as one", fits is not None and "a 2-way split that fits" in fits, fits)

# One group alone already past the cap: no partition can help, so nothing may be described as fitting.
exhausted, fl_ex = advice_for(4_500, 100)
check("the floor fixture really is exhausted", fl_ex > 50_000, fl_ex)
check("past the floor, no split is advertised as fitting", exhausted is None or "fits" not in exhausted, exhausted)

# --- the declared workflow must cover the file exactly once --------------------------------------
# A declaration that drifts from the #regions is how the check goes on reporting a comfortable
# number for a workflow that no longer covers the script.
for script, calls in DOCUMENTED_CALLS.items():
    path = Path(__file__).resolve().parent / script
    check(f"{script} exists to be measured", path.exists(), str(path))
    if not path.exists():
        continue
    actual = select_rows(strip_js(path.read_text()), set())[1]
    declared = [g for call in calls for g in call]
    check(
        f"{script}: declared calls cover every group exactly once",
        sorted(declared) == sorted(actual),
        f"declared {sorted(declared)} vs actual {sorted(actual)}",
    )
    for call in calls:
        size = len(select_rows(strip_js(path.read_text()), set(call))[0])
        check(f"{script}: --rows {','.join(call)} fits the cap", size <= CAP, f"{size:,} > {CAP:,}")

bad = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"  >> {detail}"))
print(f"\n{len(bad)} FAILURES" if bad else f"\nall checks passed ({len(results)} checks)")
sys.exit(1 if bad else 0)
