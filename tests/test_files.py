from etl import files


def test_walk_ignore_set(tmp_path):
    (tmp_path / "test.py").write_text("test")

    d = tmp_path / "__pycache__"
    d.mkdir()
    (d / "cache.py").write_text("test")

    outfiles = files.walk(tmp_path)
    assert [f.name for f in outfiles] == ["test.py"]
