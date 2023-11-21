import random

from owid.catalog import Table, Variable, VariableMeta
from owid.catalog import processing_log as pl
from owid.catalog.processing_log import LogEntry, ProcessingLog


def enable_pl(func):
    def wrapper(*args, **kwargs):
        with pl.enable_processing_log():
            random.seed(0)
            return func(*args, **kwargs)

    return wrapper


@enable_pl
def test_addition() -> None:
    t: Table = Table({"a": [1, 2], "b": [3, 4]})
    t["c"] = t["a"] + t["b"]
    assert t["c"].metadata.processing_log.as_dict() == [
        {"variable": "a", "parents": ["a", "b"], "operation": "+", "target": "a#7921731533"},
        {"variable": "c", "parents": ["a#7921731533"], "operation": "rename", "target": "c#1806341205"},
    ]


@enable_pl
def test_tables_addition() -> None:
    t1: Table = Table({"a": [1, 2]})
    t2: Table = Table({"a": [3, 4]})
    t3 = t1 + t2
    assert t3["a"].metadata.processing_log.as_dict() == [
        {"variable": "a", "parents": ["a", "a"], "operation": "+", "target": "a#7921731533"}
    ]


@enable_pl
def test_sum() -> None:
    random.seed(0)
    with pl.enable_processing_log():
        t: Table = Table({"a": [1, 2], "b": [3, 4]})
        t["c"] = t[["a", "b"]].sum(axis=1)
        assert t["c"].metadata.processing_log.as_dict() == [
            {
                "variable": "**TEMPORARY UNNAMED VARIABLE**",
                "parents": ["a", "b"],
                "operation": "+",
                "target": "**TEMPORARY UNNAMED VARIABLE**#7921731533",
            },
            {
                "variable": "c",
                "parents": ["**TEMPORARY UNNAMED VARIABLE**#7921731533"],
                "operation": "rename",
                "target": "c#1806341205",
            },
        ]


@enable_pl
def test_proper_type_after_copy():
    v1 = Variable([1], name="v", metadata=VariableMeta(processing_log=ProcessingLog([LogEntry("a", "+", "")])))
    assert isinstance(v1.metadata.processing_log, ProcessingLog)
    v2 = v1.copy()
    assert isinstance(v2.metadata.processing_log, ProcessingLog)


@enable_pl
def test_serialization():
    entry = LogEntry("c", "+", "", ("a", "b"))
    meta1 = VariableMeta(processing_log=ProcessingLog([entry]))
    d = meta1.to_dict()
    meta2 = VariableMeta.from_dict(d)
    assert meta1 == meta2
    assert isinstance(meta2.processing_log, ProcessingLog)


@enable_pl
def test_processing_log_add():
    t = Table(
        {
            "a": [1],
            "b": [2],
        }
    )
    t.a.metadata = VariableMeta(processing_log=ProcessingLog([LogEntry("a", "create", "a")]))
    t.b.metadata = VariableMeta(processing_log=ProcessingLog([LogEntry("b", "create", "b")]))
    t["c"] = t["a"] + t["b"]

    assert t["c"].metadata.processing_log.as_dict() == [
        {"variable": "a", "operation": "create", "target": "a"},
        {"variable": "b", "operation": "create", "target": "b"},
        {"variable": "a", "parents": ["a", "b"], "operation": "+", "target": "a#7921731533"},
        {"variable": "c", "parents": ["a#7921731533"], "operation": "rename", "target": "c#1806341205"},
    ]

    # do not modify original objects
    assert t["a"].metadata.processing_log.as_dict() == [{"variable": "a", "operation": "create", "target": "a"}]
    assert t["b"].metadata.processing_log.as_dict() == [{"variable": "b", "operation": "create", "target": "b"}]


@enable_pl
def test_preprocess_log_rename_after_add():
    """preprocess_log should squeeze + and rename operations together."""
    out = pl.preprocess_log(
        ProcessingLog(
            [
                LogEntry("a", "+", "a#123", ("a", "b")),
                LogEntry("b", "rename", "b#234", ("a#123",)),
            ]
        )
    )
    assert out.as_dict() == [{"variable": "b", "parents": ["a", "b"], "operation": "+", "target": "b#234"}]
