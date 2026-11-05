import pytest

from apps.owidbot import data_diff
from apps.owidbot.data_diff import format_comment
from etl.datadiff_report import (
    ColumnDiffResult,
    DatasetDiffResult,
    DiffReport,
    TableDiffResult,
    ValueDiff,
)


def _sample_report() -> DiffReport:
    return DiffReport(
        datasets=[
            DatasetDiffResult(
                path="garden/who/2025-01-17/vaccine_stock_out",
                kind="identical",
                tables=[
                    TableDiffResult(name="global_cause", kind="identical"),
                    TableDiffResult(
                        name="vaccine_stock_out",
                        kind="identical",
                        columns=[
                            ColumnDiffResult(
                                name="value",
                                kind="changed",
                                changes=["changed data"],
                                value_diffs=[
                                    ValueDiff(
                                        kind="changed",
                                        count=4,
                                        total=119537,
                                        sample=[
                                            {
                                                "country": "Austria",
                                                "year": "2019",
                                                "value -": "Inaccurtae forecasts",
                                                "value +": "Inaccurate forecasts",
                                            }
                                        ]
                                        * 4,
                                    )
                                ],
                            )
                        ],
                    ),
                ],
            ),
            DatasetDiffResult(path="garden/worldbank_wdi/2026-02-27/wdi", kind="identical", tables=[]),
        ],
        skipped_cascade=16,
    )


def test_format_comment_changed():
    body = format_comment(_sample_report(), "https://example.org/data-diff.html")

    # summary line leads with the counts and links to the full report
    assert "❌ 1 changed · 1 identical · 16 skipped" in body
    assert '<a href="https://example.org/data-diff.html">' in body

    # only the changed dataset appears in the diff block
    assert "~ garden/who/2025-01-17/vaccine_stock_out" in body
    assert "garden/worldbank_wdi/2026-02-27/wdi" not in body

    # counts and capped sample rows
    assert "~ Changed values: 4 / 119,537 (0.00%)" in body
    assert body.count("Inaccurtae forecasts → Inaccurate forecasts") == 3
    assert "(+1 more)" in body


def test_format_comment_clean():
    body = format_comment(DiffReport(datasets=[], skipped_cascade=3), None)

    assert "✅ no differences · 3 skipped" in body
    assert "No differences found." in body
    assert "```diff" not in body


def test_format_comment_error():
    report = DiffReport(datasets=[DatasetDiffResult(path="garden/n/v/ds", kind="error", error="Index must be unique.")])
    body = format_comment(report, None)

    assert "⚠️ 1 error(s)" in body
    assert "! garden/n/v/ds — error: Index must be unique." in body


def test_format_comment_puts_data_losses_first():
    # A small-severity dataset with coverage loss must lead the (truncatable) comment, ahead of
    # a big-but-benign value change — same triage order as the HTML report's watch list.
    lossy = DatasetDiffResult(
        path="garden/n/v/lossy",
        kind="identical",
        tables=[
            TableDiffResult(
                name="t",
                kind="identical",
                columns=[
                    ColumnDiffResult(
                        name="country",
                        kind="changed",
                        is_dim=True,
                        value_diffs=[ValueDiff(kind="removed", count=2, total=1000, sample=[{"country": "Vietnam"}])],
                    ),
                    ColumnDiffResult(
                        name="a",
                        kind="changed",
                        changes=["changed data"],
                        value_diffs=[ValueDiff(kind="changed", count=10, total=100, median_bard=0.001)],
                    ),
                ],
            )
        ],
    )
    big = DatasetDiffResult(
        path="garden/n/v/big",
        kind="identical",
        tables=[
            TableDiffResult(
                name="t",
                kind="identical",
                columns=[
                    ColumnDiffResult(
                        name="a",
                        kind="changed",
                        changes=["changed data"],
                        value_diffs=[ValueDiff(kind="changed", count=10, total=100, median_bard=0.9)],
                    )
                ],
            )
        ],
    )
    body = format_comment(DiffReport(datasets=[big, lossy]), None)
    assert body.index("garden/n/v/lossy") < body.index("garden/n/v/big")


def test_call_etl_diff_accepts_completed_runs(monkeypatch, tmp_path):
    """`etl diff` exits 1 when the diff completed and wrote its reports, with differences or with
    datasets that failed to compare. That must not raise, otherwise the PR comment silently keeps
    its stale content; any other exit code must."""
    from etl import datadiff

    def exit_with(code: int | None) -> None:
        def main(*args, **kwargs):
            raise SystemExit(code)

        monkeypatch.setattr(datadiff.cli, "main", main)

    out_json, out_html = tmp_path / "d.json", tmp_path / "d.html"

    for code in (None, 0, 1):
        exit_with(code)
        data_diff.call_etl_diff("garden", output_json=out_json, output_html=out_html)

    exit_with(2)
    with pytest.raises(RuntimeError):
        data_diff.call_etl_diff("garden", output_json=out_json, output_html=out_html)
