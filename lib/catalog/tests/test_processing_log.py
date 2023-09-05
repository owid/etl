import random

from owid.catalog import LogEntry, ProcessingLog, Table, Variable, VariableMeta
from owid.catalog.processing_log import preprocess_log


def test_proper_type_after_copy():
    v1 = Variable([1], name="v", metadata=VariableMeta(processing_log=ProcessingLog([LogEntry("a", (), "+", "")])))
    assert isinstance(v1.metadata.processing_log, ProcessingLog)
    v2 = v1.copy()
    assert isinstance(v2.metadata.processing_log, ProcessingLog)


def test_serialization():
    meta1 = VariableMeta(processing_log=ProcessingLog([LogEntry("a", (), "+", "")]))
    d = meta1.to_dict()
    meta2 = VariableMeta.from_dict(d)
    assert meta1 == meta2
    assert isinstance(meta2.processing_log, ProcessingLog)


def test_processing_log_add():
    random.seed(0)
    t = Table(
        {
            "a": [1],
            "b": [2],
        }
    )
    t.a.metadata = VariableMeta(processing_log=ProcessingLog([LogEntry("a", (), "create", "a")]))
    t.b.metadata = VariableMeta(processing_log=ProcessingLog([LogEntry("b", (), "create", "b")]))
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


def test_preprocess_log_rename_after_add():
    out = preprocess_log(
        ProcessingLog(
            [
                LogEntry("a", ("a", "b"), "+", "a#123"),
                LogEntry("b", ("a#123",), "rename", "b#234"),
            ]
        )
    )
    assert out.as_dict() == [{"variable": "b", "parents": ["a", "b"], "operation": "+", "target": "b#234"}]
