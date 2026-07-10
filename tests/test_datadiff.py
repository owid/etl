import hashlib
import os
import shutil
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from owid.catalog import Dataset, DatasetMeta, Table

from etl.datadiff import DatasetDiff, RemoteDataset, _changed_records, _dataset_files_match
from etl.datadiff_report import (
    HTML_SAMPLE_ROW_BUDGET,
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
    # Numeric changed samples carry an "anomaly score" (BARD) display column, sorted by it.
    assert value_diffs["changed"].sample == [{"country": "US", "a -": "3", "a +": "2", "anomaly score": "20%"}]
    assert value_diffs["changed"].sorted_by_score
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
    records, sorted_by_score, median_bard = _changed_records(both, "a")
    assert sorted_by_score
    # Biggest changes (by anomaly score = BARD) first; growth from zero is maximal (score 1).
    assert [r["country"] for r in records] == ["from_zero", "big", "small"]
    assert [r["anomaly score"] for r in records] == ["100%", "43%", "0.50%"]
    # Median BARD across all changed rows: median(0.005, 0.43, 1.0) ≈ 0.43.
    assert median_bard == pytest.approx(150 / 350, abs=1e-6)

    # A sample larger than the limit keeps only the biggest movers.
    top, _, _ = _changed_records(both, "a", limit=2)
    assert [r["country"] for r in top] == ["from_zero", "big"]


def test_changed_records_non_numeric_falls_back_to_random_sample():
    both = pd.DataFrame({"country": ["UK", "US"], "a -": ["x", "y"], "a +": ["y", "z"]})
    records, sorted_by_score, median_bard = _changed_records(both, "a")
    assert not sorted_by_score
    assert median_bard is None
    assert all("anomaly score" not in r for r in records)
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

    # The coverage chip is rendered on the dataset summary line, and the tier chip agrees with
    # the forced 🔴 tier (not the severity-derived 🟡/🟢) so the row matches the filters.
    html = render_html(DiffReport(datasets=[ds]))
    assert "row(s) removed" in html
    assert "Vietnam" in html
    assert 'class="chip tier large">🔴 median anomaly score' in html


def test_triage_aids_gate_on_content():
    # Each triage element renders only when it can discriminate.
    # (Assert on rendered elements, not bare class names — those always appear in the stylesheet.)

    # One dataset, ONE indicator: nothing to rank — no watch list, no tier strip; chips only.
    tiny = DiffReport(datasets=[_changed_ds("garden/n/v/a", 0.5)])
    html_tiny = render_html(tiny)
    assert "Top changes" not in html_tiny
    assert '<div class="tier-strip">' not in html_tiny
    assert "anomaly score" in html_tiny

    # One dataset, MANY indicators: the Indicators list ranks them (that's where it helps most),
    # but the singleton Datasets sub-list and the "Of the 1 dataset" strip stay hidden.
    many_cols = DiffReport(
        datasets=[
            DatasetDiffResult(
                path="garden/n/v/wide",
                kind="identical",
                tables=[
                    TableDiffResult(
                        name="t",
                        kind="identical",
                        columns=[_changed_col(f"c{i}", 0.5 - i * 0.1) for i in range(4)],
                    )
                ],
            )
        ]
    )
    html_many = render_html(many_cols)
    assert ">Indicators<" in html_many
    assert ">Datasets<" not in html_many
    assert '<div class="tier-strip">' not in html_many

    big = DiffReport(datasets=[_changed_ds(f"garden/n/v/d{i}", 0.5 - i * 0.1) for i in range(4)])
    html_big = render_html(big)
    assert "Top changes" in html_big
    assert '<div class="tier-strip">' in html_big
    # Watch-list entries link to the column detail blocks (anchor present in both places).
    assert 'href="#c-garden-n-v-d0-t-a"' in html_big
    assert 'id="c-garden-n-v-d0-t-a"' in html_big
    # Both perspectives: a Datasets sub-list (linking to dataset blocks) and an Indicators one.
    assert ">Datasets<" in html_big and ">Indicators<" in html_big
    assert 'href="#d-garden-n-v-d0"' in html_big
    assert 'id="d-garden-n-v-d0"' in html_big


def test_filter_attributes():
    # The filter box matches on names only, via data-search (path + tables + changed columns);
    # data-tier feeds the tier dropdown.
    ds = _changed_ds("garden/n/v/a", 0.5)
    html = render_html(DiffReport(datasets=[ds]))
    assert 'data-search="garden/n/v/a t a"' in html
    assert 'data-tier="large"' in html
    assert 'id="match-count"' in html
    # No identical datasets -> the show-identical toggle is dropped entirely; a single tier ->
    # the tier dropdown is dropped too (a filter with one choice is dead weight).
    assert 'id="show-identical"' not in html
    assert 'id="tier-filter"' not in html

    # With identical datasets, the toggle carries the count so its effect is discoverable; with
    # two tiers present, the dropdown appears offering exactly those (with counts).
    mixed = DiffReport(
        datasets=[
            ds,
            _changed_ds("garden/n/v/b", 0.05),
            DatasetDiffResult(path="garden/n/v/same", kind="identical"),
        ]
    )
    html_mixed = render_html(mixed)
    assert "show 1 identical dataset<" in html_mixed
    assert 'id="tier-filter"' in html_mixed
    assert "all dataset tiers" in html_mixed
    assert "🔴 large (1)" in html_mixed and "🟡 moderate (1)" in html_mixed
    assert "🟢 small" not in html_mixed.split('id="tier-filter"')[1].split("</select>")[0]

    # The indicator tier dropdown follows the same rules, counting non-dim changed columns;
    # column blocks carry data-tier for it to filter on.
    assert 'id="ind-tier-filter"' in html_mixed
    assert "all indicator tiers" in html_mixed
    assert '<div class="col changed" id="c-garden-n-v-a-t-a" data-tier="large">' in html_mixed
    # Single-tier report -> no indicator dropdown either.
    assert 'id="ind-tier-filter"' not in html


def test_top_changes_show_more():
    # 12 changed datasets: the Datasets watch list shows 10 and hides 2 behind "show 2 more";
    # 12 indicators fit within the visible limit (15), so only one toggle renders.
    report = DiffReport(datasets=[_changed_ds(f"garden/n/v/d{i:02d}", 0.9 - i * 0.01) for i in range(12)])
    html = render_html(report)
    assert html.count('<button class="show-more"') == 1
    assert 'data-more="2">show 2 more</button>' in html
    # The hidden entries carry the extra class and are the lowest-ranked ones.
    assert html.count('<li class="extra">') == 2
    assert 'class="extra"><span class="ti">🔴</span> <a href="#d-garden-n-v-d11"' in html


def test_headline_counts_datasets():
    one = DiffReport(datasets=[_changed_ds("garden/n/v/a", 0.5)])
    assert "❌ Found differences in the compared dataset" in render_html(one)

    mixed = DiffReport(
        datasets=[
            _changed_ds("garden/n/v/a", 0.5),
            DatasetDiffResult(path="garden/n/v/b", kind="identical"),
        ]
    )
    assert "❌ Found differences in 1 of 2 compared datasets" in render_html(mixed)

    clean = DiffReport(datasets=[DatasetDiffResult(path="garden/n/v/b", kind="identical")])
    assert "✅ No differences found in the compared dataset" in render_html(clean)

    all_changed = DiffReport(datasets=[_changed_ds(f"garden/n/v/d{i}", 0.5) for i in range(3)])
    assert "❌ Found differences in all 3 compared datasets" in render_html(all_changed)


def test_top_changes_lists_data_losses_first():
    # A dataset that lost rows (removed dim values) plus bigger-magnitude changed datasets:
    # the loss entry must lead the watch list and say explicitly that data points are gone.
    dim = ColumnDiffResult(
        name="country",
        kind="changed",
        is_dim=True,
        value_diffs=[ValueDiff(kind="removed", count=111, total=1000, sample=[{"country": "Low-income countries"}])],
    )
    lossy = DatasetDiffResult(
        path="garden/n/v/lossy",
        kind="identical",
        tables=[TableDiffResult(name="t", kind="identical", columns=[dim, _changed_col("a", 0.02)])],
    )
    others = [_changed_ds(f"garden/n/v/d{i}", 0.9) for i in range(3)]
    html = render_html(DiffReport(datasets=[*others, lossy]))

    assert "lost 111 data point(s): Low-income countries" in html
    # The loss entry leads the Indicators list, even though other datasets have larger changes.
    ind_start = html.index(">Indicators<")
    assert html.index("lossy", ind_start) < html.index("garden/n/v/d0", ind_start)
    # And the lossy dataset (🔴 via coverage loss) leads the Datasets list despite its small severity.
    ds_start = html.index(">Datasets<")
    assert html.index("lossy", ds_start) < html.index("garden/n/v/d0", ds_start)


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
    # Within a dataset, the most-changed table first; within a table, the most-changed column
    # first. (Match the rendered markup, not bare names — those also occur in data-search attrs.)
    assert html.index("Table <b>major</b>") < html.index("Table <b>minor</b>")
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


def test_sample_note_in_header():
    # Truncated samples announce it up front, in the header line — not below the table.
    col = ColumnDiffResult(
        name="a",
        kind="changed",
        changes=["changed data"],
        value_diffs=[
            ValueDiff(
                kind="changed",
                count=18479,
                total=119537,
                sample=[{"country": "FR", "a -": "1", "a +": "2", "anomaly score": "33%"}],
                sorted_by_score=True,
                median_bard=0.2,
            )
        ],
    )
    ds = DatasetDiffResult(
        path="garden/n/v/x", kind="identical", tables=[TableDiffResult(name="t", kind="identical", columns=[col])]
    )
    html = render_html(DiffReport(datasets=[ds]))
    assert '(15.46%) <span class="head-note">— showing the 1 most anomalous rows</span>' in html


def test_metadata_only_changes_are_not_tiered():
    # A metadata-only change is not an anomaly: no tier, no score chip, no tier-strip count —
    # it surfaces as "metadata-only" instead.
    meta_only = DatasetDiffResult(
        path="garden/n/v/meta",
        kind="identical",
        tables=[
            TableDiffResult(
                name="t",
                kind="identical",
                columns=[
                    ColumnDiffResult(name="a", kind="changed", changes=["changed metadata"], meta_diff="- x\n+ y")
                ],
            )
        ],
    )
    assert meta_only.change_kind == "changed"
    assert meta_only.severity == 0.0
    assert meta_only.tier == "none"

    others = [_changed_ds(f"garden/n/v/d{i}", 0.5) for i in range(3)]
    html = render_html(DiffReport(datasets=[*others, meta_only]))
    # Strip total matches the headline (4 differing datasets) and lists the metadata-only one.
    assert "Of the 4 datasets with differences" in html
    assert "📝 1 metadata-only" in html
    # The watch list labels it honestly instead of a 0% anomaly score.
    assert "metadata-only changes" in html
    # Its dataset row carries no score chip and a filterable "meta" category, which both
    # dropdowns offer as an option.
    assert 'data-tier="meta" data-search="garden/n/v/meta t a"' in html
    assert html.count('<option value="meta">📝 metadata-only (1)</option>') == 2


def test_structural_changes_are_not_metadata_only():
    # A removed column is coverage loss (🔴) — classifying it "meta" would let the tier filter
    # hide it. Added/removed tables and columns carry no value_diffs but are structural.
    removed_col = DatasetDiffResult(
        path="garden/n/v/dropped_col",
        kind="identical",
        tables=[
            TableDiffResult(
                name="t",
                kind="identical",
                columns=[ColumnDiffResult(name="gone", kind="removed")],
            )
        ],
    )
    assert not removed_col.is_metadata_only
    assert removed_col.has_coverage_loss
    assert removed_col.tier == "large"
    html = render_html(DiffReport(datasets=[removed_col]))
    assert 'data-tier="large"' in html
    assert 'data-tier="meta"' not in html

    new_table = DatasetDiffResult(
        path="garden/n/v/new_table",
        kind="identical",
        tables=[TableDiffResult(name="extra", kind="new", columns=[ColumnDiffResult(name="a", kind="new")])],
    )
    assert not new_table.is_metadata_only


def _sampled_ds(path, n_diffs, rows_per_diff):
    sample = [{"country": f"c{i}", "year": "2020", "x -": "1", "x +": "2"} for i in range(rows_per_diff)]
    cols = [
        ColumnDiffResult(
            name=f"col{j}",
            kind="changed",
            changes=["changed data"],
            value_diffs=[ValueDiff(kind="changed", count=rows_per_diff, total=10_000, sample=list(sample))],
        )
        for j in range(n_diffs)
    ]
    return DatasetDiffResult(
        path=path, kind="identical", tables=[TableDiffResult(name="t", kind="identical", columns=cols)]
    )


def test_html_sample_rows_capped_on_huge_reports():
    # 1,200 value diffs x 100 rows = 120,000 sampled rows — over the budget, so each diff
    # renders only its first rows with an explicit note.
    report = DiffReport(datasets=[_sampled_ds(f"garden/n/v/ds{i}", n_diffs=120, rows_per_diff=100) for i in range(10)])
    html = render_html(report)
    per_diff = max(5, HTML_SAMPLE_ROW_BUDGET // 1200)
    assert "of 100 sampled — display capped" in html
    # The first rows survive, anything past the cap is dropped.
    assert ">c0<" in html
    assert f">c{per_diff - 1}<" in html
    assert f">c{per_diff}<" not in html

    # A small report keeps its full samples, with no cap note.
    small = DiffReport(datasets=[_sampled_ds("garden/n/v/small", n_diffs=2, rows_per_diff=100)])
    small_html = render_html(small)
    assert "display capped" not in small_html
    assert "c99" in small_html
