#!/usr/bin/env python3
"""Harness for `inline_script.py`.

It had none, which is the wrong shape for a tool whose whole job is to decide what is safe to send:
a mistake here does not raise, it emits a *slightly different* script, and a corrupted check reports
a wrong verdict rather than failing. The cases below are the ones that have actually gone wrong.

    .venv/bin/python .claude/skills/create-figma-chart/scripts/test_inline_script.py
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
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

# --- a slice has to declare its own coverage ------------------------------------------------------
# A slice can only fail the rows it carries. Without a stamped EMITTED_ROWS its verdict cannot name
# what it omitted, and "no mechanical row failed" out of one documented call reads as a verdict on the
# whole frame — the confident silence the SKIPPED rows exist to prevent, reached by slicing.
_vp = Path(__file__).resolve().parent / "verify_page.js"
if _vp.exists():
    check(
        "verify_page.js declares EMITTED_ROWS for the slicer to stamp",
        "const EMITTED_ROWS = " in _vp.read_text(),
        "no EMITTED_ROWS declaration",
    )
    for _call in DOCUMENTED_CALLS["verify_page.js"]:
        _out = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent / "inline_script.py"),
             "verify_page.js", "--rows", ",".join(_call)],
            capture_output=True, text=True,
        )
        _want = "const EMITTED_ROWS = [" + ", ".join(f'"{g}"' for g in _call) + "];"
        check(
            f"--rows {','.join(_call)} stamps its own coverage",
            _want in _out.stdout,
            f"expected {_want}",
        )
    # The two declared lists and the file's actual #regions must agree. A region added to the script
    # and left out of them is invisible: the slicer stamps a coverage list that omits it, and the
    # verdict then reports full coverage for a group nobody sent. `--whole` is deliberately NOT the
    # vehicle for this — verify_page.js is over the cap whole and correctly refuses to emit — so the
    # invariant is read off the source.
    _src = _vp.read_text()
    _regions = list(dict.fromkeys(select_rows(strip_js(_src), set())[1]))
    for _name in ("EMITTED_ROWS", "ALL_ROW_GROUPS"):
        _m = re.search(_name + r" = \[([^\]]*)\];", _src)
        check(f"verify_page.js declares {_name}", bool(_m), "declaration missing")
        if _m:
            _declared = re.findall(r'"([^"]+)"', _m.group(1))
            check(
                f"{_name} lists every #region exactly once",
                _declared == _regions,
                f"{_name} {_declared} vs regions {_regions}",
            )

# --- a slice must PARSE, not merely fit ------------------------------------------------------------
# Fitting the cap and being valid JavaScript are different properties, and `#region` markers are what
# separates them: a marker in the wrong place slices mid-expression and emits text that is the right
# SIZE and syntactically broken. That surfaces in Figma as a plugin error with no clue to its cause,
# which is the one failure mode `inline_script.py` exists to prevent. Skipped where node is absent.
_node = shutil.which("node")
check("node is available to parse-check the slices", bool(_node), "node not on PATH — slices unverified")
if _node and _vp.exists():
    _here = Path(__file__).resolve().parent
    for _call in DOCUMENTED_CALLS["verify_page.js"]:
        _emit = subprocess.run(
            [sys.executable, str(_here / "inline_script.py"), "verify_page.js", "--rows", ",".join(_call)],
            capture_output=True, text=True,
        )
        # verify_page.js uses top-level `return`, which is legal only inside a function — the same
        # wrapper `use_figma` puts around the code it is handed.
        _wrapped = "async function __w(){\n" + _emit.stdout + "\n}\n"
        _tmp = Path(tempfile.gettempdir()) / "verify_page_slice_check.js"
        _tmp.write_text(_wrapped)
        _parsed = subprocess.run([_node, "--check", str(_tmp)], capture_output=True, text=True)
        check(
            f"--rows {','.join(_call)} emits parseable JavaScript",
            _parsed.returncode == 0,
            _parsed.stderr[:200],
        )
        _tmp.unlink(missing_ok=True)

# --- discoverability: the file that cannot be pasted must say so ----------------------------------
# This is the failure that actually happens, and it happened across several sessions: someone opens
# verify_page.js, reads a header saying "paste the whole file as one `use_figma` call", finds it is
# 142,000 characters, concludes the check is unrunnable, and hand-rolls a subset of the rows — which
# reports a pass nobody earned. The slicing was documented in CHECKS.md the whole time; the script's
# own header contradicted it, and the header is what a reader reaches first. So the claim is pinned
# here rather than left to prose discipline.
_skill = Path(__file__).resolve().parent.parent
_targets = {
    "scripts/verify_page.js": _skill / "scripts" / "verify_page.js",
    "SKILL.md": _skill / "SKILL.md",
    "reference/CHECKS.md": _skill / "reference" / "CHECKS.md",
}
# Phrases that assert the whole file goes in one call. `diff_against_template.js` genuinely IS one
# call, so match only claims naming verify_page or its own header.
_banned = ["paste the whole file", "in one read-only `use_figma` call"]
for _label, _path in _targets.items():
    if not _path.exists():
        check(f"{_label} exists", False, str(_path))
        continue
    _text = _path.read_text()
    for _phrase in _banned:
        # CHECKS.md legitimately uses the second phrase about diff_against_template.js, so only the
        # verify_page.js header is held to it; the docs are held to the explicit one-call claim.
        if _label != "scripts/verify_page.js" and _phrase == "in one read-only `use_figma` call":
            continue
        check(
            f"{_label} does not claim verify_page.js fits in one call ({_phrase!r})",
            _phrase not in _text,
            f"found {_phrase!r}",
        )

_vp_head = (_skill / "scripts" / "verify_page.js").read_text()[:3000]
check(
    "verify_page.js's header names inline_script.py",
    "inline_script.py" in _vp_head,
    "a reader who opens the file must be told how to emit it",
)
check(
    "verify_page.js's header names --rows",
    "--rows" in _vp_head,
    "the header must carry the slicing flag, not just the tool",
)
check(
    "verify_page.js's header warns against hand-rolling a subset",
    "hand-roll" in _vp_head,
    "the header must name the failure it is preventing",
)
# The spine is what an agent reads first, so the commands have to be reachable from there without
# a detour through CHECKS.md.
_spine = (_skill / "SKILL.md").read_text()
check(
    "SKILL.md Step 8c carries the emit command",
    "inline_script.py verify_page.js --rows" in _spine,
    "the spine must carry the command, not just a pointer",
)

# --- --config, the way a declared frame fact reaches a slice ---------------------------------------
# CONFIG.faceted and friends are frame FACTS, and before this flag the only way to set one was to
# hand-edit verify_page.js before emitting — which means the committed file carries one run's
# settings, and the next run inherits them silently.
def _run_cfg(*a: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(Path(__file__).resolve().parent / "inline_script.py"), "verify_page.js", *a],
        capture_output=True, text=True,
    )

_out = _run_cfg("--rows", "series", "--config", "faceted=true")
check("--config sets a boolean as a literal", "faceted: true," in _out.stdout, _out.stdout[:200] + _out.stderr[:200])
check("--config leaves the other keys alone", 'chartName: "chart",' in _out.stdout, "chartName was disturbed")

_out2 = _run_cfg("--rows", "series", "--config", "chartName=chart-body")
check("--config quotes a string value", 'chartName: "chart-body",' in _out2.stdout, _out2.stdout[:200])

_out3 = _run_cfg("--rows", "series", "--config", "faceted=true", "--config", "textFloor=11")
check("--config is repeatable", "faceted: true," in _out3.stdout and "textFloor: 11," in _out3.stdout, _out3.stdout[:200])

# An unknown key must be an ERROR. Silently ignoring it emits a script running on defaults while the
# operator believes they declared something — the same confident-wrong-answer shape as a row that
# cannot fail.
_bad = _run_cfg("--rows", "series", "--config", "nosuchkey=true")
check("--config refuses an unknown key", _bad.returncode != 0, f"exit {_bad.returncode}")
check("and says why rather than just failing", "no such key" in _bad.stderr, _bad.stderr[:200])

_malformed = _run_cfg("--rows", "series", "--config", "faceted")
check("--config refuses a malformed pair", _malformed.returncode != 0, f"exit {_malformed.returncode}")

# --frame-id and --config must compose: the documented call sets both.
_both = _run_cfg("--rows", "series", "--frame-id", "12:34", "--config", "faceted=true")
check("--frame-id and --config compose",
      'frameId: "12:34",' in _both.stdout and "faceted: true," in _both.stdout,
      _both.stdout[:200])

# A string value must be ESCAPED, not just wrapped in a pair of quotes. The slice is pasted straight
# into use_figma, so a value carrying a double quote or a backslash closed the string early and the
# plugin got a syntax error with nothing in it pointing back at the flag that caused it. The emitted
# value has to parse as JavaScript, so the assertion is that the CONFIG block still parses — checking
# only that the characters survived would pass on the broken output too.
_quoted = _run_cfg("--rows", "series", "--config", 'chartName=A "quoted" chart')
check("--config escapes a double quote", _quoted.returncode == 0, _quoted.stderr[:200])
check("and emits it as one valid JS string",
      r'chartName: "A \"quoted\" chart",' in _quoted.stdout, _quoted.stdout[:300])

_slash = _run_cfg("--rows", "series", "--config", r"chartName=share\rate")
check("--config escapes a backslash", r'chartName: "share\\rate",' in _slash.stdout, _slash.stdout[:300])

# json.loads is the arbiter: it accepts exactly the string syntax JavaScript does, so a value that
# round-trips through it is one the plugin can parse.
_emitted = re.search(r"\n\s*chartName:\s*(\"(?:[^\"\\]|\\.)*\"),", _quoted.stdout)
check("the escaped value round-trips", _emitted is not None, _quoted.stdout[:300])
check("and decodes back to what was asked for",
      _emitted is not None and json.loads(_emitted.group(1)) == 'A "quoted" chart',
      _emitted.group(1) if _emitted else "no match")

bad = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"  >> {detail}"))
print(f"\n{len(bad)} FAILURES" if bad else f"\nall checks passed ({len(results)} checks)")
sys.exit(1 if bad else 0)
