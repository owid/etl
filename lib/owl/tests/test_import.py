from owl import Dataset, Snapshot


def test_import() -> None:
    assert Dataset is not None
    assert Snapshot is not None
