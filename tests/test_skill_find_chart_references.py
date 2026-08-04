"""Guards for the `find-chart-references` skill script.

Nothing under `.claude/` is covered by CI: the pre-commit hook does not lint it and no
other test collects it. That gap is why three separate defects reached the branch by way
of a merge, each invisible to every check we ran:

  1. a `rec()` call left with 11 positional arguments against a 7-parameter signature;
  2. a sweep reading `r["queryString"]` from a SELECT that does not select that column;
  3. a slug filter (`target LIKE '%/grapher/%'`) that silently matched nothing, because
     the column stores bare slugs for typed links.

All three sit in code paths that match zero production rows today, so neither a live run
nor the reference-count canary could fail on them. The two static tests below encode the
checks that would have caught (1) and (2) at commit time; the behavioural test executes
the raw-URL branch — the one that has now hosted both — against synthetic rows.
"""

import ast
import importlib.util
import re
from pathlib import Path

import pandas as pd
import pytest

RELATIVE = Path(".claude/skills/find-chart-references/scripts/find_references.py")


def _locate_script() -> Path:
    """Find the skill script by walking up from this file to the repo root.

    Resolved by search rather than a fixed `parent.parent` so the test keeps working from
    a git worktree, and so moving it between test directories cannot silently turn it into
    a skipped no-op.
    """
    for directory in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = directory / RELATIVE
        if candidate.is_file():
            return candidate
    raise AssertionError(f"could not locate {RELATIVE} from {__file__}")


SCRIPT = _locate_script()

# `rec()` takes 7 positional parameters; everything after is keyword-only. A merge that
# grafts a call written against an older signature lands here.
REC_POSITIONAL_LIMIT = 7


@pytest.fixture(scope="module")
def mod():
    """The skill script, loaded by path (it is not an importable package)."""
    spec = importlib.util.spec_from_file_location("find_references", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tree():
    return ast.parse(SCRIPT.read_text())


def test_rec_calls_stay_within_the_positional_limit(tree):
    """A `rec()` call may not rely on positional arguments past the keyword-only marker."""
    offenders = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "rec"
        and len(node.args) > REC_POSITIONAL_LIMIT
    ]
    assert not offenders, (
        f"rec() calls at line(s) {offenders} pass more than {REC_POSITIONAL_LIMIT} positional "
        "arguments. The tail of rec() is keyword-only; passing positionally shifts values into "
        "the wrong parameters (this raised a TypeError once already)."
    )


def _select_output_names(sql: str) -> set[str]:
    """Column names a SELECT actually yields: the alias where there is one, else the column.

    Matching against the raw SQL text instead would let `pgl.queryString` appearing in a
    WHERE clause — or in a comment — vouch for a column the SELECT never returns, which is
    precisely the defect this guard exists to catch.
    """
    names: set[str] = set()
    for select in re.findall(r"SELECT\s+(.*?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL):
        depth, current = 0, ""
        for char in select + ",":
            if char == "," and depth == 0:
                item = current.strip()
                alias = re.search(r"\bAS\s+([A-Za-z_]\w*)\s*$", item, re.IGNORECASE)
                if alias:
                    names.add(alias.group(1))
                elif item:
                    names.add(item.split()[-1].split(".")[-1].strip("`"))
                current = ""
                continue
            depth += (char == "(") - (char == ")")
            current += char
    return names


def _sql_of_read_sql_call(node) -> str | None:
    """The SQL string of an `OWID_ENV.read_sql(...)` call, however its parts are spliced."""
    if not (isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "read_sql"):
        return None
    return " ".join(
        part.value for part in ast.walk(node.args[0]) if isinstance(part, ast.Constant) and isinstance(part.value, str)
    )


def _dataframe_sources(fn) -> dict[str, str]:
    """`{dataframe variable -> its SQL}` for every read_sql assignment in this function."""
    frames = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            sql = _sql_of_read_sql_call(node.value)
            if sql:
                frames[node.targets[0].id] = sql
    return frames


def _row_iterations(fn, frames: dict[str, str]):
    """Yield `(row variable, SQL)` for each iteration over one of those frames' records.

    Pairing every row variable with **its own** query is the point. Unioning a function's
    SELECTs would have declared the real defect harmless, because `sweep_mdim_subject`
    selects `queryString` in a different query than the one whose rows read it.
    """

    def frame_of(call):
        if isinstance(call, ast.Subscript):  # `df.to_dict("records")[0]`
            call = call.value
        if not (isinstance(call, ast.Call) and getattr(call.func, "attr", "") == "to_dict"):
            return None
        owner = getattr(call.func, "value", None)
        return frames.get(owner.id) if isinstance(owner, ast.Name) else None

    for node in ast.walk(fn):
        if isinstance(node, (ast.For, ast.comprehension)):
            sql = frame_of(node.iter)
            if sql and isinstance(node.target, ast.Name):
                body = node.body if isinstance(node, ast.For) else []
                yield node.target.id, sql, body or [node]
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            sql = frame_of(node.value)
            if sql:
                yield node.targets[0].id, sql, list(ast.walk(fn))


def test_every_column_access_is_selected(tree):
    """Each `r["col"]` must name a column that *that row's own* SELECT returns.

    A merge can graft a row-reading line into a loop whose query lacks the column. That
    raises KeyError only once the query returns a row, so on a branch nothing in production
    matches it stays silently broken — which is how it reached this file twice.
    """
    problems, checked = [], 0
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        frames = _dataframe_sources(fn)
        for row_var, sql, scope in _row_iterations(fn, frames):
            selected = _select_output_names(sql)
            if "*" in selected:  # shape unknown — don't pretend to check it
                continue
            checked += 1
            keys = {
                node.slice.value
                for stmt in scope
                for node in ast.walk(stmt)
                if isinstance(node, ast.Subscript)
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
                and isinstance(node.value, ast.Name)
                and node.value.id == row_var
            }
            problems += [f"{fn.name}/{row_var}: {k}" for k in sorted(keys - selected)]
    assert checked, "found no SQL row iterations to check — the matcher has drifted from the code"
    assert not problems, "column(s) read but never returned by that row's SELECT: " + ", ".join(problems)


def test_mdim_raw_url_branch_emits_a_reference(mod, monkeypatch):
    """An article pasting an MDIM's raw grapher URL must surface as a reference.

    No production row exercises this branch today, so it is only ever run here. It has
    hosted two crashes; both would have failed this test rather than waiting for the first
    article to paste such a URL.
    """

    def fake_read_sql(sql, params=None):
        if "multi_dim_data_pages" in sql:
            return pd.DataFrame([{"id": 42, "slug": "poverty-explorer", "catalogPath": "grapher/x"}])
        if "linkType = 'url'" in sql:
            return pd.DataFrame(
                [
                    {
                        "gdoc_id": "gdoc-1",
                        "post_slug": "an-article",
                        "post_type": "article",
                        "published": 1,
                        "target": "https://ourworldindata.org/grapher/poverty-explorer?tab=map#fragment",
                        "componentType": "chart",
                        "text": "see the data",
                    },
                    # A longer slug that merely starts with the subject must NOT match.
                    {
                        "gdoc_id": "gdoc-2",
                        "post_slug": "other",
                        "post_type": "article",
                        "published": 1,
                        "target": "https://ourworldindata.org/grapher/poverty-explorer-extended",
                        "componentType": "chart",
                        "text": "",
                    },
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(mod.OWID_ENV, "read_sql", fake_read_sql)
    refs = [r for r in mod.sweep_mdim_subject("poverty-explorer") if r["surface"] == "gdoc (url link)"]

    assert len(refs) == 1, "the loose SQL prefilter must be re-checked on the path segment"
    ref = refs[0]
    assert ref["kind"] == mod.EMBED, "a non-span componentType is a block embed, not a hyperlink"
    assert ref["surface_id"] == "gdoc-1", "the article id is what makes the doc link resolvable"
    # The parameters select the view, so losing them collapses the reference to the base page;
    # the fragment is not a parameter and must not leak into the query string.
    assert ref["query_string"] == "tab=map"
    assert ref["where_path"] == "/an-article"
