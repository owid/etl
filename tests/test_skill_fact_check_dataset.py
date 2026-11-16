"""Guards for the `fact-check-dataset` skill's scripts.

Nothing under `.claude/` is linted or collected by CI, so these tests pin the logic a review depends on: which
values the anomaly scan flags, how indicators are selected and skipped, how a chart's strings and numbers are read,
when a saved document may be reused, and that a report is never assembled from an incomplete review.
All data is synthetic and nothing here touches the network.
"""

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(".claude/skills/fact-check-dataset/scripts")


def _load(name: str):
    for directory in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = directory / SCRIPTS / f"{name}.py"
        if candidate.is_file():
            if str(candidate.parent) not in sys.path:
                sys.path.insert(0, str(candidate.parent))
            spec = importlib.util.spec_from_file_location(name, candidate)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            return module
    raise AssertionError(f"could not locate {SCRIPTS / name} from {__file__}")


anomaly_scan = _load("anomaly_scan")
fetch_docs = _load("fetch_docs")
assemble_report = _load("assemble_report")
prepare = _load("prepare")


# ----------------------------------------------------------------------------------------------------------------
# Anomaly scan
# ----------------------------------------------------------------------------------------------------------------


def _series(country: str, values: list, start: int = 2000) -> pd.DataFrame:
    return pd.DataFrame({"country": country, "year": range(start, start + len(values)), "x": values})


def _kinds(frame: pd.DataFrame, unit: str = "", **kwargs) -> list[str]:
    return [f["kind"] for f in anomaly_scan.scan_table(frame, "t", {"x": unit}, **kwargs)]


def test_scan_clean_series_has_no_flags():
    frame = pd.concat(
        [_series("A", [10, 11, 12, 13, 14, 15, 16, 17, 18]), _series("B", [5, 6, 7, 8, 9, 10, 11, 12, 13])]
    )
    assert _kinds(frame) == []


def test_scan_flags_empty_column():
    assert _kinds(_series("A", [float("nan")] * 3)) == ["EMPTY"]


def test_scan_flags_percent_stored_as_fraction_and_percent_above_150():
    assert _kinds(_series("A", [0.2, 0.3, 0.4]), unit="%") == ["UNIT"]
    assert _kinds(_series("A", [20, 30, 400]), unit="share of total") == ["UNIT", "BREAK"]
    assert _kinds(_series("A", [20, 30, 40]), unit="%") == []


def test_scan_flags_negative_count():
    assert _kinds(_series("A", [3, -1, 2]), unit="deaths") == ["SIGN"]
    assert "SIGN" not in _kinds(_series("A", [3, -1, 2]), unit="°C")


def test_scan_flags_outlier_only_in_long_enough_series():
    spike = [10, 11, 10, 12, 11, 10, 11, 500, 12]
    assert "OUTLIER" in _kinds(_series("A", spike))
    # Under 8 points the median/MAD z-score is unstable, so no outlier is called.
    assert "OUTLIER" not in _kinds(_series("A", spike[3:]))


def test_scan_flags_tenfold_break_with_its_period():
    findings = anomaly_scan.scan_table(_series("A", [10, 11, 120, 125]), "t", {})
    assert [(f["kind"], f["period"]) for f in findings] == [("BREAK", 2002)]


def test_scan_flags_coverage_drop_in_latest_period():
    frame = pd.concat([_series(c, [1, 2, 3]) for c in "ABCDEFGHIJ"])
    frame = frame[~((frame["year"] == 2002) & frame["country"].isin(list("DEFGHIJ")))]
    assert _kinds(frame) == ["COVERAGE"]


def test_scan_coverage_is_judged_per_dimension_slice():
    """A slice that loses its countries in the latest year is flagged even when the other slice keeps them all."""
    men = pd.concat([_series(c, [1, 2, 3]) for c in "ABCDEFGHIJ"]).assign(sex="men")
    women = pd.concat([_series(c, [1, 2]) for c in "ABCDEFGHIJ"]).assign(sex="women")
    findings = anomaly_scan.scan_table(pd.concat([men, women]), "t", {}, dims=["sex"])
    assert [(f["kind"], f["period"]) for f in findings] == [("COVERAGE", 2002)]
    assert findings[0]["detail"] == "sex=women, 2002: 0 countries vs 10"


def test_scan_flags_repeated_values_but_not_repeated_zeroes():
    assert _kinds(_series("A", [7, 7, 7, 7, 8])) == ["CONSTANT"]
    # A sparse count that is mostly zero is not a forward-fill.
    assert _kinds(_series("A", [0, 0, 0, 0, 3])) == []


def test_scan_jump_from_zero_is_not_a_break():
    """0 → 3 has no ratio; 3 → 40 is the ×10 signature."""
    assert _kinds(_series("A", [0, 3, 4])) == []
    assert _kinds(_series("A", [3, 40, 41])) == ["BREAK"]


def test_scan_year_only_table_gets_unit_checks_only():
    frame = pd.DataFrame({"year": range(2000, 2010), "x": [0.1] * 10})
    assert _kinds(frame, unit="%") == ["UNIT"]


def test_scan_world_sum_only_for_additive_units():
    frame = pd.concat([_series("World", [100, 100]), _series("A", [30, 30]), _series("B", [30, 30])])
    assert "WORLD_SUM" in _kinds(frame, unit="tonnes", countries={"A", "B"})
    assert "WORLD_SUM" not in _kinds(frame, unit="tonnes per person", countries={"A", "B"})
    # World within 5% of the sum of countries is not flagged.
    ok = pd.concat([_series("World", [61, 61]), _series("A", [30, 30]), _series("B", [30, 30])])
    assert "WORLD_SUM" not in _kinds(ok, unit="tonnes", countries={"A", "B"})


def test_scan_dimension_keeps_series_apart():
    """Two sexes of one country are two series: read as one, the alternation looks like a string of breaks."""
    men, women = _series("A", [10, 11, 12, 13]), _series("A", [1000, 1010, 1020, 1030])
    frame = pd.concat([men.assign(sex="men"), women.assign(sex="women")]).sort_values(["year", "sex"])
    assert anomaly_scan.scan_table(frame, "t", {}, dims=["sex"]) == []
    assert anomaly_scan.scan_table(frame, "t", {}) == []  # inferred: rows repeat within country and year
    assert "BREAK" in [f["kind"] for f in anomaly_scan.scan_table(frame, "t", {}, dims=[])]


def test_scan_accepts_grapher_column_names():
    """A chart's dossier gives grapher names, which carry the garden dimensions as a suffix."""
    assert anomaly_scan.column_matches("libdem_vdem__estimate_best", "libdem_vdem")
    assert anomaly_scan.column_matches("libdem_vdem", "libdem_vdem")
    assert not anomaly_scan.column_matches("libdem_vdem_low", "libdem_vdem")
    assert not anomaly_scan.column_matches("libdem", "libdem_vdem")


def test_scan_text_indicator_in_wide_table_is_not_a_dimension():
    """A categorical indicator beside a numeric one must not split the numeric series."""
    frame = _series("A", [10, 11, 120, 125]).assign(regime=["a", "b", "a", "b"])
    assert anomaly_scan.infer_dims(frame, ["country", "year"]) == []
    assert [f["kind"] for f in anomaly_scan.scan_table(frame, "t", {})] == ["BREAK"]


# ----------------------------------------------------------------------------------------------------------------
# Documentation: links and cache
# ----------------------------------------------------------------------------------------------------------------


def test_extract_links_cleans_and_filters():
    text = (
        "See [the methodology](https://producer.example.org/methods.pdf). Also https://producer.example.org/a(b), "
        "our own https://ourworldindata.org/grapher/x and a template https://producer.example.org/{{ year }}/file."
    )
    assert fetch_docs.extract_links(text) == [
        "https://producer.example.org/methods.pdf",
        "https://producer.example.org/a(b)",
    ]


def test_data_and_endpoint_links_are_not_documentation():
    assert fetch_docs.is_data_url("https://x.org/files/data.CSV")
    assert not fetch_docs.is_data_url("https://x.org/methodology.pdf")
    assert fetch_docs.looks_like_endpoint("https://api.x.org/v2/indicator")
    assert fetch_docs.looks_like_endpoint("https://x.org/data?format=csv")
    assert not fetch_docs.looks_like_endpoint("https://x.org/rapid-response/methods.html")


def test_saved_document_is_reused_for_seven_days_only(tmp_path):
    now = dt.datetime(2026, 11, 16, 12, 0)
    (tmp_path / "01-doc.txt").write_text("text")
    entry = {"status": "ok", "file": "01-doc.txt", "fetched_at": "2026-11-10 12:00"}
    assert fetch_docs.is_fresh(entry, tmp_path, now)
    assert not fetch_docs.is_fresh({**entry, "fetched_at": "2026-11-09 11:59"}, tmp_path, now)
    # A page that could not be read, or whose text is gone from disk, is fetched again.
    assert not fetch_docs.is_fresh({**entry, "status": "unreachable"}, tmp_path, now)
    assert not fetch_docs.is_fresh({**entry, "file": "missing.txt"}, tmp_path, now)


def test_fetch_all_reuses_cache_without_network(tmp_path, monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("the network was used for a document that is saved and fresh")

    monkeypatch.setattr(fetch_docs.requests, "get", no_network)
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "01-doc.txt").write_text("text")
    now = dt.datetime(2026, 11, 16, 12, 0)
    entry = {
        "url": "https://x.org/methods",
        "role": "metadata",
        "status": "ok",
        "file": "01-doc.txt",
        "fetched_at": "2026-11-15 09:00",
        "chars": 4,
    }
    (docs_dir / "index.json").write_text(json.dumps([entry]))
    docs = fetch_docs.fetch_all([("https://x.org/methods", "metadata")], tmp_path, now=now)
    assert [(d.via, d.status, d.fetched_at) for d in docs] == [("cache", "ok", "2026-11-15 09:00")]
    # --refresh ignores the saved copy, so it goes to the network.
    with pytest.raises(AssertionError):
        fetch_docs.fetch_all([("https://x.org/methods", "metadata")], tmp_path, refresh=True, now=now)


def test_pdf_text_marks_pages():
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    writer.add_blank_page(width=200, height=200)
    import io

    buffer = io.BytesIO()
    writer.write(buffer)
    text, _, _ = fetch_docs.to_text(buffer.getvalue(), "application/pdf", "https://x.org/a.pdf")
    assert "[page 1]" in text and "[page 2]" in text


def test_links_from_metadata_are_capped_but_origin_links_never_are(tmp_path):
    meta = tmp_path / "big.meta.yml"
    meta.write_text("\n".join(f"- https://producer.example.org/series/{i}" for i in range(40)))
    origins = [{"url_main": "https://producer.example.org/", "url_download": "https://producer.example.org/d.csv"}]
    links = dict(prepare.collect_links(origins, [meta], first_texts=["see https://producer.example.org/chart-note"]))
    assert links["https://producer.example.org/"] == "url_main"
    assert links["https://producer.example.org/d.csv"] == "url_download"
    # The chart's own text is read first, so the cap never drops it.
    assert links["https://producer.example.org/chart-note"] == "metadata"
    assert sum(1 for role in links.values() if role == "metadata") == prepare.MAX_TEXT_LINKS
    assert sum(1 for role in links.values() if role == "not-fetched") == 41 - prepare.MAX_TEXT_LINKS


# ----------------------------------------------------------------------------------------------------------------
# Targets, selection, strings
# ----------------------------------------------------------------------------------------------------------------

DAG = {
    "data://grapher/ns/2026-01-01/thing": {"data://garden/ns/2026-01-01/thing"},
    "data://garden/ns/2026-01-01/thing": {"data://meadow/ns/2026-01-01/thing"},
    "data://garden/ns/2025-01-01/thing": set(),
    "data://garden/a/2024-01-01/twice": set(),
    "data://garden/b/2024-01-01/twice": set(),
}


def test_parse_step_forms():
    assert prepare.parse_step("ns/2026-01-01/thing", DAG) == ("ns", "2026-01-01", "thing")
    assert prepare.parse_step("data://garden/ns/2026-01-01/thing", DAG) == ("ns", "2026-01-01", "thing")
    assert prepare.parse_step("grapher/ns/2026-01-01/thing/", DAG) == ("ns", "2026-01-01", "thing")
    # A bare short name resolves to its latest active version.
    assert prepare.parse_step("thing", DAG) == ("ns", "2026-01-01", "thing")


def test_parse_step_stops_on_ambiguous_or_unknown_name():
    with pytest.raises(SystemExit, match="ambiguous"):
        prepare.parse_step("twice", DAG)
    with pytest.raises(SystemExit, match="No active garden step"):
        prepare.parse_step("nothing", DAG)


def test_parse_chart_reference():
    assert prepare.parse_chart_reference("life-expectancy") == ("life-expectancy", "")
    url = "https://ourworldindata.org/grapher/gdp-per-capita-worldbank?tab=line&country=~GUY&time=2000..latest"
    assert prepare.parse_chart_reference(url) == ("gdp-per-capita-worldbank", "tab=line&country=~GUY&time=2000..latest")
    assert prepare.parse_chart_reference("https://ourworldindata.org/grapher/x.csv?v=1")[0] == "x"


def _indicators(n: int) -> list[dict]:
    return [{"identity": f"t#c{i}", "column": f"c{i}"} for i in range(n)]


def _ranking(views: dict[str, int]):
    frame = pd.DataFrame([{"identity": k, "n_charts": 1, "views_365d": v} for k, v in views.items()])
    return frame, "chart views"


def test_selection_top_n_plus_flagged_and_everything_else_skipped_with_a_reason():
    ranking = _ranking({"t#c0": 900, "t#c1": 500, "t#c2": 100})
    findings = [{"table": "t", "column": "c4", "kind": "BREAK"}]
    rows, _ = prepare.select_indicators(_indicators(6), ranking, findings, top=2, full=False, forced=[])
    by = {row["identity"]: row for row in rows}
    assert [by[f"t#c{i}"]["selected"] for i in range(6)] == [True, True, False, False, True, False]
    assert by["t#c4"]["why"] == "flagged by the anomaly scan"
    assert by["t#c2"]["why"].startswith("SKIPPED: rank 3, 100 views")
    # Nothing drops out: every indicator is in the inventory, selected or skipped with its reason.
    assert len(rows) == 6 and all(row["why"] for row in rows)


def test_selection_caps_flagged_indicators_and_says_so():
    ranking = _ranking({"t#c0": 900})
    findings = [{"table": "t", "column": f"c{i}", "kind": "OUTLIER"} for i in range(1, 6)]
    rows, _ = prepare.select_indicators(_indicators(6), ranking, findings, 1, False, [], max_flagged=2)
    assert sum(row["selected"] for row in rows) == 3
    skipped = [row for row in rows if not row["selected"]]
    assert len(skipped) == 3 and all("past the cap of 2 flagged indicators" in row["why"] for row in skipped)


def test_selection_new_dataset_reviews_all_when_small_and_asks_when_large():
    empty = (pd.DataFrame(columns=["identity", "n_charts", "views_365d"]), "chart views")
    rows, note = prepare.select_indicators(_indicators(5), empty, [], 10, False, [])
    assert all(row["selected"] for row in rows)
    rows, note = prepare.select_indicators(_indicators(prepare.SMALL_DATASET + 1), empty, [], 10, False, [])
    assert not any(row["selected"] for row in rows)
    assert "--indicators" in note


def test_selection_full_forced_and_renamed():
    ranking = _ranking({"t#c0": 900, "t#gone": 400})
    rows, note = prepare.select_indicators(_indicators(3), ranking, [], 1, False, ["c2"])
    assert [row["selected"] for row in rows] == [True, False, True]
    assert "`t#gone`" in note and "not in this build" in note
    rows, _ = prepare.select_indicators(_indicators(3), ranking, [], 1, True, [])
    assert all(row["selected"] for row in rows)


def test_selection_folds_dimensioned_grapher_columns_into_the_built_column():
    """Grapher publishes `t#c0__sex_female` and `t#c0__sex_male`; the local build has the one column `t#c0`."""
    ranking = _ranking({"t#c0__sex_female": 500, "t#c0__sex_male": 400, "t#c1": 100})
    rows, note = prepare.select_indicators(_indicators(prepare.SMALL_DATASET + 1), ranking, [], 1, False, [])
    by = {row["identity"]: row for row in rows}
    assert by["t#c0"]["views_365d"] == 900 and by["t#c0"]["selected"]
    assert "not in this build" not in note


def test_selection_keys_anomaly_flags_by_table_and_column():
    indicators = [{"identity": "a#x", "column": "x"}, {"identity": "b#x", "column": "x"}]
    empty = (pd.DataFrame(columns=["identity", "n_charts", "views_365d"]), "chart views")
    rows, _ = prepare.select_indicators(
        indicators, empty, [{"table": "b", "column": "x", "kind": "BREAK"}], 0, False, []
    )
    assert {row["identity"]: row["anomaly_flags"] for row in rows} == {"a#x": 0, "b#x": 1}


def test_converted_download_is_not_compared_byte_for_byte():
    origin = {"url_download": "https://example.org/data/file.zip?raw=1", "stored": "file.xlsx"}
    assert "`.zip`" in prepare.converted_download(origin)
    assert prepare.converted_download({"url_download": "https://example.org/file.csv", "stored": "file.csv"}) == ""
    assert prepare.converted_download({"url_download": "https://example.org/api/v1", "stored": "file.json"}) == ""


def test_daily_periods_are_shown_as_dates():
    data = pd.DataFrame({"Entity": "A", "Day": ["2020-01-01", "2020-01-02"], "cases": [1.0, 3.0]})
    lines = prepare.section_data(data, None, [])
    assert any("(2020-01-01) → 3 (2020-01-02)" in line for line in lines)
    yearly = prepare.section_data(pd.DataFrame({"Entity": "A", "Year": [2000, 2001], "x": [1.0, 2.0]}), None, [])
    assert any("(2000) → 2 (2001)" in line for line in yearly)


def test_selection_survives_a_failed_ranking():
    rows, note = prepare.select_indicators(_indicators(2), prepare.Failed("TimeoutError: x"), [], 10, False, [])
    assert len(rows) == 2 and "FAILED" in note


def test_failed_job_is_told_apart_from_a_text_result():
    assert prepare.ok("# DoDs referenced by ns/thing")
    assert not prepare.ok(prepare.Failed("ValueError: x"))
    assert not prepare.ok(None)


def test_numbers_in_strings():
    def read(text):
        return [(n["text"], n["value"], n["year"]) for n in prepare.numbers_in(text)]

    assert read("$8,025") == [("$8,025", 8025.0, False)]
    assert read("grew between 2000 and 2025") == [("2000", 2000.0, True), ("2025", 2025.0, True)]
    assert read("fell to 10.4% in 2019") == [("10.4%", 10.4, False), ("2019", 2019.0, True)]
    assert read("1.2 million people")[0][:2] == ("1.2 million", 1.2)
    assert prepare.numbers_in("1.2 million people")[0]["scale"] == 1e6
    assert read("a change of −3.5 points") == [("−3.5", -3.5, False)]
    # A range is two numbers, not a negative one; a bare digit is not matched against data.
    assert read("2000–2025") == [("2000", 2000.0, True), ("2025", 2025.0, True)]
    assert read("children under 5") == []


DATA = pd.DataFrame(
    {
        "Entity": ["Guyana", "Guyana", "Chile"],
        "Code": ["GUY", "GUY", "CHL"],
        "Year": [2000, 2025, 2025],
        "GDP per capita": [8025.2935, 83658.68, 31000.4],
    }
)


def test_match_number_at_the_precision_shown():
    number = prepare.numbers_in("$83,659")[0]
    assert prepare.match_number(number, DATA) == ["Guyana 2025: GDP per capita = 83,658.7"]
    assert prepare.match_number(prepare.numbers_in("$83,660")[0], DATA) == []
    assert prepare.match_number(prepare.numbers_in("31 thousand")[0], DATA) == ["Chile 2025: GDP per capita = 31,000.4"]


def test_match_number_prefers_the_series_the_string_names_then_the_latest_period():
    shares = pd.DataFrame(
        {
            "Entity": ["United Kingdom"] * 3,
            "Year": [1997, 2024, 2025],
            "Nuclear": [26.7, 13.0, 12.0],
            "Wind": [0.1, 26.9, 26.7],
        }
    )
    number = prepare.numbers_in("Wind\n27%")[0]
    # A rounded 27% matches three cells: Wind's come first (the string names it), its latest year first.
    assert prepare.match_number(number, shares, "Wind\n27%", limit=2) == [
        "United Kingdom 2025: Wind = 26.7",
        "United Kingdom 2024: Wind = 26.9",
        "(and 1 more cells)",
    ]


def test_number_drops_the_comma_that_follows_it():
    assert [n["text"] for n in prepare.numbers_in("By 2025, coal had almost disappeared")] == ["2025"]
    assert prepare.numbers_in("By 2025, coal")[0]["year"]


def _dvc(path: Path, producer: str, title: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"meta:\n  origin:\n    producer: {producer}\n    title: {title}\n    url_main: https://x.org/{path.stem}\n"
        f"outs:\n  - md5: abc\n    size: 10\n    path: {path.stem}\n"
    )
    return path


def test_derived_dataset_takes_the_upstream_origins_its_indicators_carry(tmp_path, monkeypatch):
    """A dataset that combines others has no snapshot of its own: the origins come from further up the DAG."""
    monkeypatch.setattr(prepare.paths, "SNAPSHOTS_DIR", tmp_path)
    monkeypatch.setattr(prepare.paths, "BASE_DIR", tmp_path)
    _dvc(tmp_path / "ember/2026-01-01/electricity.csv.dvc", "Ember", "Yearly Electricity Data")
    _dvc(tmp_path / "demography/2024-01-01/population.csv.dvc", "Various", "Population")
    dag = {
        "data://garden/energy/2026-01-01/mix": {"data://garden/ember/2026-01-01/electricity", "data://garden/x/1/pop"},
        "data://garden/ember/2026-01-01/electricity": {"snapshot://ember/2026-01-01/electricity.csv"},
        "data://garden/x/1/pop": {"snapshot://demography/2024-01-01/population.csv"},
    }
    carried = prepare.unique_origins(
        [
            {"producer": "Ember", "title": "Yearly electricity data", "urlMain": "https://ember.org"},
            {"producer": "Ember", "title": "Yearly Electricity Data"},  # the same origin on a second indicator
            {"producer": "Energy Institute", "title": "Statistical Review", "urlMain": "https://ei.org"},
        ]
    )
    assert len(carried) == 2 and carried[0]["url_main"] == "https://ember.org"
    files = {"dvc": []}
    origins = prepare.all_origins(dag, ["data://garden/energy/2026-01-01/mix"], files, carried)
    # Ember's snapshot is matched (with its md5, for the live check); population is upstream but not an origin
    # of these indicators; the Energy Institute origin has no snapshot upstream and is kept as the indicator states.
    assert [(o["producer"], o.get("md5"), o["file"]) for o in origins] == [
        ("Ember", "abc", "ember/2026-01-01/electricity.csv.dvc"),
        ("Energy Institute", None, "(indicator metadata)"),
    ]
    # A dataset with its own snapshot and no further origins is left alone.
    own = {"dvc": [tmp_path / "ember/2026-01-01/electricity.csv.dvc"]}
    assert len(prepare.all_origins(dag, [], own, carried[:1])) == 1


def test_strings_section_reports_matches_misses_and_years():
    out = "\n".join(
        prepare.section_strings(["$8,025", "ten times the $7,000 of 1990", "Chile in 2000", "Source (2026)"], DATA)
    )
    assert "`$8,025`: in the data at Guyana 2000" in out
    assert "`$7,000`: NOT in the displayed data" in out
    assert "`1990`: a year outside the displayed data (2000–2025)" in out
    assert "`2000`: NO data that year for Chile" in out
    assert "`2026`: a year outside the displayed data" in out


def test_parse_brief(tmp_path):
    brief = tmp_path / "check_facts.md"
    brief.write_text(
        "# Fact check of a static chart's texts\n\nBrief: 8ae4aec01628\n\n"
        "- chart: `gdp-per-capita-worldbank`, view `tab=line&country=~GUY`\n"
        "- link: https://ourworldindata.org/grapher/gdp-per-capita-worldbank?tab=line&country=~GUY\n"
        "- the displayed data: `/tmp/run/data.csv`\n\n"
        "## The strings\n\n### `frame` (static horizontal)\n"
        '- title: "Guyana’s GDP per capita grew more than tenfold"\n'
        '- note: "In international-$ at 2021 prices."\n\n'
        "### Annotations\n"
        '- "Guyana\\n$83,659" (label at {"series": "line__Guyana", "x": 2025})\n\n'
        "## Report\n\nStart the report with the line `Brief: 8ae4aec01628`.\n"
        '- not a string: "ignored"\n'
    )
    parsed = prepare.parse_brief(brief)
    assert parsed["id"] == "8ae4aec01628"
    assert parsed["slug"] == "gdp-per-capita-worldbank"
    assert parsed["data"] == "/tmp/run/data.csv"
    assert parsed["strings"] == [
        "Guyana’s GDP per capita grew more than tenfold",
        "In international-$ at 2021 prices.",
        "Guyana\n$83,659",
    ]


def test_description_key_list_or_string():
    assert prepare.as_text(["a", "b"]) == "- a\n- b"
    assert prepare.as_text('["a", "b"]') == "- a\n- b"
    assert prepare.as_text("- a\n- b") == "- a\n- b"
    assert prepare.as_text([]) is None and prepare.as_text(None) is None


# ----------------------------------------------------------------------------------------------------------------
# Report assembly
# ----------------------------------------------------------------------------------------------------------------

CLAIMS = """## Part 0 — Source verification
- Methodology read (2026 edition).

## Findings
1. 🟡 [text-level] Title says "global" but the source covers 120 countries
   Evidence: methodology p. 3
   Fix: say "in reporting countries"
   Why: scope overclaim.

## Notes for the other lane
none
"""
VALUES = """## Findings
1. 🟢 [data-level] Chad 2019 could not be verified
   Evidence: both sources unreachable
2. 🔴 [data-level] Laos 2024 is ten times its neighbours' value
   Evidence: IMF 5.1, ADB 5.3, ours 51
   Fix: corrections.yml

## Values checked
| indicator | entity | year | our value | independent value(s) | source link | verdict | anchor or anomaly |
|---|---|---|---|---|---|---|---|
| gdp | World | 2024 | 3.2 | 3.2 (WB) | https://x.org | ✓ | anchor |

## Anomalies not checked
none

## Notes for the other lane
none
"""


def _dossier(tmp_path: Path, claims: str | None = CLAIMS, values: str | None = VALUES) -> Path:
    dossier = tmp_path / "ai" / "fact-check" / "ns" / "2026-01-01" / "thing"
    dossier.mkdir(parents=True)
    if claims is not None:
        (dossier / "lane_claims.result.md").write_text(claims)
    if values is not None:
        (dossier / "lane_values.result.md").write_text(values)
    pd.DataFrame(
        [
            {"identity": "t#a", "selected": True, "why": "top 10 by chart use"},
            {"identity": "t#b", "selected": False, "why": "SKIPPED: rank 2, 0 views, 0 charts, no anomaly flag"},
        ]
    ).to_csv(dossier / "ranking.csv", index=False)
    return dossier


def test_assemble_orders_findings_by_severity_and_lists_skipped(tmp_path):
    text, problems, counts = assemble_report.assemble(_dossier(tmp_path), False, None, "2026-11-16")
    assert problems == []
    assert counts == {"🔴": 1, "🟡": 1, "🟢": 1}
    assert text.index("1. 🔴 [data-level] Laos") < text.index("2. 🟡 [text-level] Title") < text.index("3. 🟢")
    assert "Fix: corrections.yml" in text  # a finding keeps its evidence and fix lines
    assert "- `t#b`: SKIPPED: rank 2" in text and "`t#a`" not in text.split("### SKIPPED indicators")[1]
    assert text.startswith("# Adversarial data review — ns/2026-01-01/thing")
    assert "1 🔴, 1 🟡, 1 🟢. Fix first: 🔴 [data-level] Laos" in text


def test_assemble_refuses_when_a_lane_has_not_reported(tmp_path):
    text, problems, _ = assemble_report.assemble(_dossier(tmp_path, values=None), True, None, "2026-11-16")
    assert text == "" and problems == ["lane_values.result.md is missing: that lane has not reported"]


def test_assemble_refuses_when_a_section_is_missing(tmp_path):
    values = VALUES.replace("## Values checked", "## Checked values")
    text, problems, _ = assemble_report.assemble(_dossier(tmp_path, values=values), True, None, "2026-11-16")
    assert text == "" and problems == ["lane_values.result.md has no `## Values checked` section"]


def test_assemble_refuses_unsettled_notes_between_lanes(tmp_path):
    claims = CLAIMS.replace("none\n", "The codebook says Chad 2019 = 4.1: check it against the data.\n")
    dossier = _dossier(tmp_path, claims=claims)
    text, problems, _ = assemble_report.assemble(dossier, False, None, "2026-11-16")
    assert text == "" and "Chad 2019 = 4.1" in problems[0] and "--reconciled" in problems[0]
    text, problems, _ = assemble_report.assemble(dossier, True, None, "2026-11-16")
    assert problems == [] and "Notes between the lanes (settled by the caller)" in text


def test_assemble_refuses_findings_without_a_severity(tmp_path):
    claims = CLAIMS.replace("1. 🟡 [text-level]", "1. [text-level]")
    text, problems, _ = assemble_report.assemble(_dossier(tmp_path, claims=claims), False, None, "2026-11-16")
    assert text == "" and "no line starting with 🔴, 🟡 or 🟢" in problems[0]


def test_assemble_clean_review(tmp_path):
    claims = CLAIMS.split("## Findings")[0] + "## Findings\nNo findings.\n\n## Notes for the other lane\nnone\n"
    values = "## Findings\nnone\n\n## Values checked" + VALUES.split("## Values checked")[1]
    text, problems, counts = assemble_report.assemble(_dossier(tmp_path, claims, values), False, None, "2026-11-16")
    assert problems == [] and sum(counts.values()) == 0
    assert "No findings: the review came back clean." in text
