import hashlib
import os
import shutil
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from owid.catalog import Dataset, DatasetMeta, Table

from etl.datadiff import DatasetDiff, RemoteDataset, _changed_records, _dataset_files_match
from etl.datadiff_report import (
    ColumnDiffResult,
    DatasetDiffResult,
    DiffReport,
    TableDiffResult,
    ValueDiff,
    render_html,
)


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
    # Numeric changed samples carry an absolute "Δ %" display column and are marked delta-sorted.
    assert value_diffs["changed"].sample == [{"country": "US", "a -": "3", "a +": "2", "Δ %": "33.3%"}]
    assert value_diffs["changed"].sorted_by_delta
    # 3 -> 2: BARD = |3-2| / (3+2) = 0.2
    assert value_diffs["changed"].median_bard == pytest.approx(0.2)

    # JSON round-trip
    report = DiffReport(datasets=[res], skipped_cascade=2)
    report2 = DiffReport.from_json(report.to_json())
    assert report2.to_json() == report.to_json()
    assert report2.n_changed == 1
    assert report2.n_identical == 0
    assert report2.status == "changed"


def test_changed_records_sorts_numeric_by_change_size():
    both = pd.DataFrame(
        {
            "country": ["small", "big", "from_zero"],
            "a -": [100.0, 100.0, 0.0],
            "a +": [101.0, 250.0, 5.0],  # BARD ≈ 0.005, 0.43, 1.0
        }
    )
    records, sorted_by_delta, median_bard = _changed_records(both, "a")
    assert sorted_by_delta
    # Biggest changes (by BARD) first; growth from zero is a maximal change and ranks on top.
    assert [r["country"] for r in records] == ["from_zero", "big", "small"]
    assert [r["Δ %"] for r in records] == ["∞%", "150.0%", "1.0%"]
    # Median BARD across all changed rows: median(0.005, 0.43, 1.0) ≈ 0.43.
    assert median_bard == pytest.approx(150 / 350, abs=1e-6)

    # A sample larger than the limit keeps only the biggest movers.
    top, _, _ = _changed_records(both, "a", limit=2)
    assert [r["country"] for r in top] == ["from_zero", "big"]


def test_changed_records_non_numeric_falls_back_to_random_sample():
    both = pd.DataFrame({"country": ["UK", "US"], "a -": ["x", "y"], "a +": ["y", "z"]})
    records, sorted_by_delta, median_bard = _changed_records(both, "a")
    assert not sorted_by_delta
    assert median_bard is None
    assert all("Δ %" not in r for r in records)
    assert len(records) == 2


def _changed_col(name, median_bard, is_dim=False):
    return ColumnDiffResult(
        name=name,
        kind="changed",
        is_dim=is_dim,
        changes=["changed data"],
        value_diffs=[ValueDiff(kind="changed", count=10, total=100, median_bard=median_bard)],
    )


def _changed_ds(path, median_bard):
    return DatasetDiffResult(
        path=path,
        kind="identical",
        tables=[TableDiffResult(name="t", kind="identical", columns=[_changed_col("a", median_bard)])],
    )


def test_severity_tiers():
    assert _changed_ds("garden/n/v/x", 0.5).tier == "large"
    assert _changed_ds("garden/n/v/x", 0.05).tier == "moderate"
    assert _changed_ds("garden/n/v/x", 0.002).tier == "small"
    assert DatasetDiffResult(path="garden/n/v/x", kind="identical").tier == "none"
    # Errors and removed datasets are always large.
    assert DatasetDiffResult(path="garden/n/v/x", kind="identical", error="boom").tier == "large"
    assert DatasetDiffResult(path="garden/n/v/x", kind="removed").tier == "large"


def test_coverage_loss_forces_large_tier():
    # A dim with removed values = entities that disappeared -> coverage loss -> 🔴, even though
    # the value change itself is tiny.
    dim = ColumnDiffResult(
        name="country",
        kind="changed",
        is_dim=True,
        value_diffs=[
            ValueDiff(kind="removed", count=2, total=100, sample=[{"country": "Vietnam"}, {"country": "Philippines"}])
        ],
    )
    ds = DatasetDiffResult(
        path="garden/n/v/x",
        kind="identical",
        tables=[TableDiffResult(name="t", kind="identical", columns=[dim, _changed_col("a", 0.001)])],
    )
    assert ds.has_coverage_loss
    assert ds.removed_row_count == 2
    assert ds.removed_labels == ["Vietnam", "Philippines"]
    assert ds.tier == "large"

    # The coverage chip is rendered on the dataset summary line.
    html = render_html(DiffReport(datasets=[ds]))
    assert "row(s) removed" in html
    assert "Vietnam" in html


def test_triage_aids_only_on_big_reports():
    small = DiffReport(datasets=[_changed_ds("garden/n/v/a", 0.5)])
    html_small = render_html(small)
    # A single changed dataset: no watch list, no tier strip — but the row chip still renders.
    # (Assert on rendered elements, not bare class names — those always appear in the stylesheet.)
    assert "Top changes" not in html_small
    assert '<div class="tier-strip">' not in html_small
    assert "typical change" in html_small

    big = DiffReport(datasets=[_changed_ds(f"garden/n/v/d{i}", 0.5 - i * 0.1) for i in range(4)])
    html_big = render_html(big)
    assert "Top changes" in html_big
    assert '<div class="tier-strip">' in html_big
    # Watch-list entries link to the column detail blocks (anchor present in both places).
    assert 'href="#c-garden-n-v-d0-t-a"' in html_big
    assert 'id="c-garden-n-v-d0-t-a"' in html_big


def test_report_sorts_by_severity():
    """Datasets, tables and columns render biggest-differences-first."""

    col = _changed_col

    # Table/column order in the model is deliberately "small change first".
    ds_small = DatasetDiffResult(
        path="garden/n/v/small",
        kind="identical",
        tables=[TableDiffResult(name="t", kind="identical", columns=[col("a", 0.01)])],
    )
    ds_big = DatasetDiffResult(
        path="garden/n/v/big",
        kind="identical",
        tables=[
            TableDiffResult(name="minor", kind="identical", columns=[col("x", 0.05)]),
            TableDiffResult(name="major", kind="identical", columns=[col("tiny", 0.02), col("huge", 0.9)]),
        ],
    )
    html = render_html(DiffReport(datasets=[ds_small, ds_big]))

    # Dataset with the biggest change first.
    assert html.index("garden/n/v/big") < html.index("garden/n/v/small")
    # Within a dataset, the most-changed table first; within a table, the most-changed column first.
    assert html.index("major") < html.index("minor")
    assert html.index("major.huge") < html.index("major.tiny")
    # Severity levels: dataset takes the max of its tables.
    assert ds_big.severity == pytest.approx(0.9)
    assert ds_small.severity == pytest.approx(0.01)


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
