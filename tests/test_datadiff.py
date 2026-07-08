import hashlib
import os
import shutil
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from owid.catalog import Dataset, DatasetMeta, Table

from etl.datadiff import DatasetDiff, RemoteDataset, _dataset_files_match
from etl.datadiff_report import DiffReport, render_html


def _create_datasets(tmp_path):
    (tmp_path / "catalog_a").mkdir()
    (tmp_path / "catalog_b").mkdir()

    ds_meta_a = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="1")
    ds_a = Dataset.create_empty(tmp_path / "catalog_a" / "ds", ds_meta_a)
    ds_a.metadata.channel = "garden"  # ty: ignore

    ds_meta_b = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="2")
    ds_b = Dataset.create_empty(tmp_path / "catalog_b" / "ds", ds_meta_b)
    ds_b.metadata.channel = "garden"  # ty: ignore

    return ds_a, ds_b


@pytest.mark.filterwarnings("ignore:Table `tab` does not have a primary_key")
@patch.dict(os.environ, {"OWID_STRICT": ""})
def test_DatasetDiff_summary(tmp_path):
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table(pd.DataFrame({"a": [1, 2]}), short_name="tab")
    tab_a.metadata.description = "tab"

    tab_b = Table(pd.DataFrame({"a": [1, 3], "b": ["a", "b"]}), short_name="tab")
    tab_b["a"].metadata.description = "col a"

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    out = []
    differ = DatasetDiff(ds_a, ds_b, print=lambda x: out.append(x))
    differ.summary()

    assert out == [
        "[white]= Dataset [b]garden/n/v/ds[/b]",
        "\t[yellow]~ Table [b]tab[/b] (changed [u]metadata[/u])",
        "\t\t[yellow]~ Column [b]a[/b] (changed [u]metadata[/u], changed [u]data[/u])",
        "\t\t[green]+ Column [b]b[/b]",
    ]


@pytest.mark.filterwarnings("ignore:Table `tab` does not have a primary_key")
@patch.dict(os.environ, {"OWID_STRICT": ""})
def test_new_data(tmp_path):
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table({"country": ["UK", "US"], "a": [1, 3]}, short_name="tab")
    tab_b = Table({"country": ["UK", "US", "FR"], "a": [1, 2, 3]}, short_name="tab")

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    out = []
    differ = DatasetDiff(ds_a, ds_b, print=lambda x: out.append(x), verbose=True)
    differ.summary()

    assert out == [
        "[white]= Dataset [b]garden/n/v/ds[/b]",
        "\t[white]= Table [b]tab[/b]",
        "\t\t[yellow]~ Dim [b]country[/b]",
        "\t\t\t\t[violet]+ New values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country\n\t\t\t\t[violet]       FR",
        "\t\t[yellow]~ Column [b]a[/b] (new [u]data[/u], changed [u]data[/u])",
        "\t\t\t\t[violet]+ New values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country  a\n\t\t\t\t[violet]       FR  3\n\t\t\t\t[violet]~ Changed values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country  a -  a +\n\t\t\t\t[violet]       US    3    2",
    ]


@pytest.mark.filterwarnings("ignore:Table `tab` does not have a primary_key")
@patch.dict(os.environ, {"OWID_STRICT": ""})
def test_structured_result(tmp_path):
    """The structured `result` mirrors the printed summary, even without verbose."""
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table({"country": ["UK", "US"], "a": [1, 3]}, short_name="tab")
    tab_b = Table({"country": ["UK", "US", "FR"], "a": [1, 2, 3]}, short_name="tab")

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    differ = DatasetDiff(ds_a, ds_b, print=lambda x: None, details=True)
    differ.summary()
    res = differ.result

    assert res.path == "garden/n/v/ds"
    assert res.change_kind == "changed"

    (tab,) = res.tables
    assert tab.kind == "identical"  # table metadata unchanged

    dim = next(c for c in tab.columns if c.is_dim)
    assert dim.name == "country"
    assert [v.kind for v in dim.value_diffs] == ["new"]

    col = next(c for c in tab.columns if not c.is_dim)
    assert col.name == "a"
    assert col.changes == ["new data", "changed data"]
    value_diffs = {v.kind: v for v in col.value_diffs}
    assert value_diffs["new"].count == 1
    assert value_diffs["new"].total == 3
    assert value_diffs["new"].sample == [{"country": "FR", "a": "3"}]
    assert value_diffs["changed"].sample == [{"country": "US", "a -": "3", "a +": "2"}]

    # JSON round-trip
    report = DiffReport(datasets=[res], skipped_cascade=2)
    report2 = DiffReport.from_json(report.to_json())
    assert report2.to_json() == report.to_json()
    assert report2.n_changed == 1
    assert report2.n_identical == 0
    assert report2.status == "changed"


@pytest.mark.filterwarnings("ignore:Table `tab` does not have a primary_key")
@patch.dict(os.environ, {"OWID_STRICT": ""})
def test_dataset_files_match_covers_metadata(tmp_path):
    """The checksum-cascade fast path must not skip datasets whose table metadata changed."""
    ds_a, _ = _create_datasets(tmp_path)
    tab = Table({"country": ["UK"], "a": [1]}, short_name="tab")
    ds_a.add(tab)

    # "remote" is a frozen copy of the dataset as currently published
    remote_dir = tmp_path / "remote"
    shutil.copytree(ds_a.path, remote_dir)
    ds_remote = RemoteDataset(ds_a.metadata, ["tab"])

    def fake_head(url, timeout=None):
        resp = MagicMock()
        remote_file = remote_dir / url.rsplit("/", 1)[1]
        if remote_file.exists():
            resp.status_code = 200
            resp.headers = {"ETag": f'"{hashlib.md5(remote_file.read_bytes()).hexdigest()}"'}
        else:
            resp.status_code = 404
            resp.headers = {}
        return resp

    with patch("etl.datadiff.http_session.head", side_effect=fake_head):
        # nothing changed -> skip is allowed
        assert _dataset_files_match(ds_a, ds_remote)

        # metadata-only change (feather untouched) -> must NOT be skipped
        meta_json = tmp_path / "catalog_a" / "ds" / "tab.meta.json"
        meta_json.write_text(meta_json.read_text().replace("{", '{"description": "new", ', 1))
        assert not _dataset_files_match(ds_a, ds_remote)


@pytest.mark.filterwarnings("ignore:Table `tab` does not have a primary_key")
@patch.dict(os.environ, {"OWID_STRICT": ""})
def test_render_html(tmp_path):
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table({"country": ["UK", "US"], "a": [1, 3]}, short_name="tab")
    tab_b = Table({"country": ["UK", "US", "FR"], "a": [1, 2, 3]}, short_name="tab")

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    differ = DatasetDiff(ds_a, ds_b, print=lambda x: None, details=True)
    differ.summary()

    html = render_html(DiffReport(datasets=[differ.result], skipped_cascade=2))

    assert "❌ Found differences" in html
    assert "garden/n/v/ds" in html
    assert "Changed values" in html
    # old/new sample values are rendered in the table
    assert ">US<" in html
    assert "2 more dataset(s) skipped" in html
