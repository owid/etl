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


def test_checksum_file_dvc(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("a,b,c\r\n1,2,3")

    # DVC replaces \r\n with \n, while md5sum does not
    assert files.checksum_file(f, typ="dvc") == "1a477f6d2f9c9fc827fa46b5ace1a145"
    assert files.checksum_file(f, typ="md5sum") == "71687777aed243ae2f42756ca886c9d6"


def test_checksum_str_dvc():
    s = "a,b,c\r\n1,2,3"

    # DVC replaces \r\n with \n, while md5sum does not
    assert files.checksum_str(s, typ="dvc") == "1a477f6d2f9c9fc827fa46b5ace1a145"
    assert files.checksum_str(s, typ="md5sum") == "71687777aed243ae2f42756ca886c9d6"
