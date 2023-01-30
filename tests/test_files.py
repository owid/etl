from etl import files


def test_walk_ignore_set(tmp_path):
    (tmp_path / "test.py").write_text("test")

    d = tmp_path / "__pycache__"
    d.mkdir()
    (d / "cache.py").write_text("test")

    outfiles = files.walk(tmp_path)
    assert [f.name for f in outfiles] == ["test.py"]


def test_yaml_dump():
    d = {
        "a": "Multi-line\nstring\n with whitespaces and \ttab!",
        "b": "One-liner",
        "c": {"nested": {"d": ["\nline  \n"]}},
    }

    out = files.yaml_dump(d)
    print(out)
    assert (
        out
        == """a: |-
  Multi-line
  string
  with whitespaces and   tab!
b: One-liner
c:
  nested:
    d:
    - line
"""
    )
