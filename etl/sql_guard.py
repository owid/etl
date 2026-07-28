"""Guard for SQL that we did not write ourselves.

Some code paths hand a SQL string composed by an LLM (the Expert agent, the MCP server)
straight to a database connection. Those connections are shared credentials, not read-only
ones, so the only thing keeping such a query from writing is the prompt asking it not to.

`validate_read_only_sql` is a cheap structural check that the string is *one* statement and
that the statement is a `SELECT` (optionally with a leading `WITH` clause). It is defence in
depth, not a trust boundary: the credential the query travels on is what actually bounds the
damage, and anyone holding that credential can bypass this function entirely by calling the
underlying API. Its job is to stop a confused model — or a prompt-injected one — from turning
a read tool into a write tool.

Two properties matter more than the keyword list:

* **It never rewrites the query.** The sanitized copy is used for analysis only; the caller
  executes the original string. A validator that strips comments and executes the result will
  silently truncate a query whose string literal contains `--`.
* **It only looks at code, never at data.** String literals, quoted identifiers, comments and
  Metabase template tags are blanked before analysis, so a `;` or the word `delete` inside
  `WHERE url LIKE '%delete%'` cannot change the verdict.
"""

import re

__all__ = ["SqlNotReadOnlyError", "validate_read_only_sql"]


class SqlNotReadOnlyError(ValueError):
    """The SQL string is not a single read-only SELECT statement."""


# Comments, string literals and quoted identifiers. Everything matched here is blanked out
# before the query is inspected, so its contents can never influence the verdict.
_LITERALS_AND_COMMENTS_RE = re.compile(
    r"""
      \{\{[^{}]*\}\}           # {{template_tag}} — must come before the # comment rule, because
                               # Metabase card references look like {{#141-some-card}}
    | --[^\n]*                 # -- line comment
    | \#[^\n]*                 # # line comment (BigQuery accepts both)
    | /\*.*?(?:\*/|$)          # /* block comment */, unterminated included
    | '(?:\\.|[^'\\])*'        # 'single-quoted string'
    | "(?:\\.|[^"\\])*"        # "double-quoted string"
    | `[^`]*`                  # `backtick-quoted identifier` (BigQuery table references)
    """,
    re.VERBOSE | re.DOTALL,
)

_WITH_RE = re.compile(r"\s*with\b", re.IGNORECASE)
_RECURSIVE_RE = re.compile(r"\s*recursive\b", re.IGNORECASE)
# One CTE definition up to (and including) the opening parenthesis of its body:
# `name [(col, ...)] AS [NOT MATERIALIZED] (`. The name may be empty here, because a
# backtick-quoted name has already been blanked out by the sanitizer.
_CTE_HEAD_RE = re.compile(
    r"\s*\w*\s*(?:\([^()]*\)\s*)?as\s*(?:(?:not\s+)?materialized\s*)?\(",
    re.IGNORECASE,
)
_SELECT_RE = re.compile(r"\s*select\b", re.IGNORECASE)


def _blank_literals_and_comments(sql: str) -> str:
    """Replace every comment, string literal and quoted identifier with a space.

    Length is not preserved and nothing is unbalanced: each match becomes a single space, which
    keeps neighbouring tokens apart (`a'x'b` -> `a b`) without leaving any of the original
    content behind.
    """
    return _LITERALS_AND_COMMENTS_RE.sub(" ", sql)


def _skip_parenthesized(sql: str, start: int) -> int | None:
    """Return the index just past the `)` matching the `(` at `start`, or None if unbalanced."""
    assert sql[start] == "("
    depth = 0
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return None


def _body_after_with_clause(sql: str) -> str | None:
    """Given a sanitized query starting with `WITH`, return the main query that follows.

    Returns None if the CTE list cannot be parsed, in which case the query is rejected: we
    would otherwise be guessing about what the statement actually does.
    """
    match = _WITH_RE.match(sql)
    assert match is not None
    position = match.end()

    recursive = _RECURSIVE_RE.match(sql, position)
    if recursive:
        position = recursive.end()

    while True:
        head = _CTE_HEAD_RE.match(sql, position)
        if head is None:
            return None
        # head.end() - 1 is the opening parenthesis of the CTE body.
        after_body = _skip_parenthesized(sql, head.end() - 1)
        if after_body is None:
            return None
        position = after_body

        next_comma = re.match(r"\s*,", sql[position:])
        if next_comma is None:
            return sql[position:]
        position += next_comma.end()


def validate_read_only_sql(sql: str) -> None:
    """Raise `SqlNotReadOnlyError` unless `sql` is a single read-only SELECT statement.

    The query is *not* modified — callers execute the string they passed in.

    Accepted: a single `SELECT`, optionally preceded by a `WITH` clause and optionally
    wrapped in parentheses (`(SELECT ...) UNION ALL (SELECT ...)`), with or without a
    trailing semicolon.

    Rejected: anything else — DDL/DML, multi-statement scripts, BigQuery scripting
    (`DECLARE`, `EXPORT DATA`, …), and queries whose structure cannot be parsed.
    """
    query = _blank_literals_and_comments(sql)

    if not query.strip():
        raise SqlNotReadOnlyError("Empty query.")

    # A trailing semicolon is fine; anything after it means more than one statement.
    if ";" in re.sub(r"[\s;]+$", "", query):
        raise SqlNotReadOnlyError("Only a single statement can be executed (found `;` mid-query).")
    query = re.sub(r"[\s;]+$", "", query)

    if _WITH_RE.match(query):
        body = _body_after_with_clause(query)
        if body is None:
            raise SqlNotReadOnlyError(
                "Could not verify that this is a read-only SELECT: the WITH clause could not be parsed."
            )
    else:
        body = query

    # `(SELECT ...) UNION ALL (SELECT ...)` and other parenthesized set operations.
    body = body.lstrip()
    while body.startswith("("):
        body = body[1:].lstrip()

    if not _SELECT_RE.match(body):
        first_word = re.match(r"\s*(\w+)", body)
        found = first_word.group(1).upper() if first_word else "nothing"
        raise SqlNotReadOnlyError(f"Only SELECT queries can be executed (query starts with `{found}`).")
