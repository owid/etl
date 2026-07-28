"""Tests for the read-only SQL guard."""

import pytest

from etl.analytics.metabase import assert_question_is_read_only
from etl.sql_guard import SqlNotReadOnlyError, validate_read_only_sql

ACCEPTED = [
    # Plain SELECT, with and without a trailing semicolon.
    "SELECT 1",
    "  select 1  ",
    "SELECT * FROM `prod_semantic.views_detailed` LIMIT 10;",
    # Multi-CTE query, the shape most Expert queries have.
    """
    WITH sessions AS (
        SELECT user_pseudo_id, day FROM `prod_ga4.session_metrics` WHERE day >= '2026-01-01'
    ),
    ranked AS (
        SELECT user_pseudo_id, COUNT(*) AS n FROM sessions GROUP BY 1
    )
    SELECT * FROM ranked ORDER BY n DESC
    """,
    # A `--` inside a string literal must not be read as a comment.
    "SELECT COUNT(*) FROM t WHERE url LIKE '%--%'",
    # Words that a keyword blacklist would trip over, inside literals and identifiers.
    "SELECT last_updated FROM t WHERE action IN ('insert', 'update', 'delete', 'drop table')",
    "SELECT COUNT(*) FROM t WHERE title = 'How we update our data; and why'",
    # Real comments.
    "-- count the rows\nSELECT COUNT(*) FROM t",
    "/* block\n comment */ SELECT COUNT(*) FROM t",
    "SELECT COUNT(*) FROM t # trailing BigQuery comment",
    # Parenthesized set operations.
    "(SELECT 1) UNION ALL (SELECT 2)",
    "WITH a AS (SELECT 1 AS x) (SELECT x FROM a) UNION ALL (SELECT 2)",
    # Backtick-quoted CTE name (blanked by the sanitizer, so the parser must cope).
    "WITH `my cte` AS (SELECT 1 AS x) SELECT * FROM `my cte`",
    # Nested parentheses and a subquery inside the CTE body.
    "WITH a AS (SELECT (SELECT MAX(y) FROM u) AS m FROM t) SELECT * FROM a",
    # Metabase template tags, including a card reference — the `#` in `{{#141-...}}` is not the
    # start of a BigQuery comment, and treating it as one used to swallow the closing paren.
    "with t as (select source_url from `prod_semantic.media_mentions`"
    " join (select source_url from {{#141-media-sources-metadata}}) using(source_url)"
    " where {{date_range}}) select * from t",
]

REJECTED = [
    "",
    "   ",
    "-- nothing but a comment",
    "DROP TABLE x",
    "drop table `prod_mysql.users`",
    "DELETE FROM prod_mysql.sessions WHERE 1=1",
    "INSERT INTO t VALUES (1)",
    "CREATE OR REPLACE TABLE t AS SELECT 1",
    "SELECT 1; DELETE FROM y",
    "SELECT 1;\nDROP TABLE y;",
    "SELECT 1; SELECT 2;",
    # BigQuery scripting.
    "DECLARE x INT64; SET x = 1;",
    "EXPORT DATA OPTIONS(uri='gs://bucket/*.csv') AS SELECT * FROM t",
    "CALL my_procedure()",
    # A statement smuggled after what looks like a comment: the comment ends at the newline.
    "SELECT 1 -- harmless\n; DROP TABLE y",
    # WITH clause that does not lead into a SELECT.
    "WITH a AS (SELECT 1) DELETE FROM t",
    "WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a",
    # Unparseable WITH clause: reject rather than guess.
    "WITH a AS (SELECT 1",
]


@pytest.mark.parametrize("sql", ACCEPTED)
def test_accepts_read_only_queries(sql: str) -> None:
    validate_read_only_sql(sql)


@pytest.mark.parametrize("sql", REJECTED)
def test_rejects_everything_else(sql: str) -> None:
    with pytest.raises(SqlNotReadOnlyError):
        validate_read_only_sql(sql)


def _mbql_lib_card(sql: str) -> dict:
    """A saved question in the shape our Metabase instance currently returns."""
    return {
        "id": 1782,
        "dataset_query": {
            "lib/type": "mbql/query",
            "database": 3,
            "stages": [{"lib/type": "mbql.stage/native", "native": sql}],
        },
    }


def _legacy_card(sql: str) -> dict:
    """A saved question in Metabase's older `dataset_query` shape."""
    return {"id": 812, "dataset_query": {"type": "native", "database": 3, "native": {"query": sql}}}


@pytest.mark.parametrize("card", [_mbql_lib_card, _legacy_card])
def test_accepts_read_only_saved_questions(card) -> None:
    assert_question_is_read_only(card("WITH a AS (SELECT 1 AS x) SELECT * FROM a"))


@pytest.mark.parametrize("card", [_mbql_lib_card, _legacy_card])
def test_rejects_saved_questions_that_write(card) -> None:
    with pytest.raises(SqlNotReadOnlyError):
        assert_question_is_read_only(card("DELETE FROM `prod_mysql.sessions`"))


def test_accepts_query_builder_questions() -> None:
    """Query-builder ("MBQL") questions hold no SQL and can only read."""
    assert_question_is_read_only(
        {"id": 5, "dataset_query": {"lib/type": "mbql/query", "stages": [{"lib/type": "mbql.stage/mbql"}]}}
    )
    assert_question_is_read_only({"id": 5, "dataset_query": {"type": "query", "query": {"source-table": 1}}})


def test_rejects_unrecognised_question_shapes() -> None:
    """Fail closed: an unknown shape means we cannot tell what the card would run."""
    with pytest.raises(SqlNotReadOnlyError):
        assert_question_is_read_only({"id": 5, "dataset_query": {}})
    with pytest.raises(SqlNotReadOnlyError):
        assert_question_is_read_only({"id": 5, "dataset_query": {"type": "something-new"}})
    with pytest.raises(SqlNotReadOnlyError):
        assert_question_is_read_only({"id": 5, "dataset_query": {"stages": [{"lib/type": "mbql.stage/new"}]}})


def test_does_not_modify_the_query() -> None:
    """The guard inspects a sanitized copy; the caller executes the original string."""
    sql = "SELECT '--' AS dashes FROM t"
    validate_read_only_sql(sql)
    assert sql == "SELECT '--' AS dashes FROM t"
