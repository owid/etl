import numpy as np
import pandas as pd

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


def test_yaml_empty():
    d = {
        "a": "",
    }
    assert files.yaml_dump(d) == "a: ''\n"


def test_checksum_file_regions(tmp_path):
    s = """
- code: "FOO"
- code: "BAR"
  aliases:
    - "BAR"
    """.strip()
    (tmp_path / "regions.yml").write_text(s)

    checksum1 = files.checksum_file(tmp_path / "regions.yml")

    # add alias
    s = """
- code: "FOO"
- code: "BAR"
  aliases:
    - "BAR"
    - "BAZ"
    """.strip()
    (tmp_path / "regions.yml").write_text(s)

    checksum2 = files.checksum_file(tmp_path / "regions.yml")

    assert checksum1 == checksum2


def test_checksum_dict():
    d = {
        "a": 1,
        "b": "x",
        "c": [1, 2, 3, np.nan],
    }
    assert files.checksum_dict(d) == "f5ec37a48fc5bdbe085608c8689b696f"


def test_checksum_df():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "x", "y"]})
    assert files.checksum_df(df) == "34c7a3a435e4a0703b37904f09f967f1"
