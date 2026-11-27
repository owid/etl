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
from unittest.mock import patch

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
anomalist_candidates = _load("anomalist_candidates")


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


def test_garden_flags_reach_the_grapher_slices_of_their_column():
    """The scan reads garden columns; a built grapher dataset names each dimension slice separately."""
    flags = {"t#rate": 3, "t#rate_other": 1, "u#rate": 5}
    assert prepare.flags_for("t#rate__sex_female", flags) == 3
    assert prepare.flags_for("t#rate", flags) == 3
    # Neither a column that merely starts with the same letters nor another table's column counts.
    assert prepare.flags_for("t#rate_other__age_all", flags) == 1
    assert prepare.flags_for("t#rates", flags) == 0
    rows, _ = prepare.select_indicators(
        [{"identity": "t#rate__sex_female", "column": "rate__sex_female"}, {"identity": "t#x", "column": "x"}],
        (pd.DataFrame(columns=["identity", "n_charts", "views_365d"]), "chart views"),
        [{"table": "t", "column": "rate", "kind": "BREAK"}],
        top=10,
        full=False,
        forced=[],
    )
    by = {row["identity"]: row for row in rows}
    assert by["t#rate__sex_female"]["anomaly_flags"] == 1 and by["t#rate__sex_female"]["selected"]


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
    # A dotted identifier at the end of an API path is not a file type: the download is still hashed.
    dotted = {"url_download": "https://api.wb.org/v2/indicator/NY.GDP.DEFL.ZS.AD", "stored": "wdi_gdp_linked.zip"}
    assert prepare.converted_download(dotted) == ""


def test_daily_periods_are_shown_as_dates():
    data = pd.DataFrame({"Entity": "A", "Day": ["2020-01-01", "2020-01-02"], "cases": [1.0, 3.0]})
    lines = prepare.section_data(data, None, [])
    assert any("(2020-01-01) → 3 (2020-01-02)" in line for line in lines)
    yearly = prepare.section_data(pd.DataFrame({"Entity": "A", "Year": [2000, 2001], "x": [1.0, 2.0]}), None, [])
    assert any("(2000) → 2 (2001)" in line for line in yearly)


def test_stated_years_are_checked_against_daily_data():
    data = pd.DataFrame({"Entity": ["A", "A", "B"], "Day": ["2020-01-01", "2021-06-01", "2020-03-01"], "x": [1, 2, 3]})
    lines = prepare.section_strings(["Cases rose from 2019 to 2021."], data)
    assert any("`2019`: a year outside the displayed data (2020–2021)" in line for line in lines)
    assert any("`2021`: NO data that year for B" in line for line in lines)


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
    # A dataset with a snapshot of its own keeps only its own origins, even when its indicators carry others
    # (picked up from merged-in datasets such as population): those are other datasets' sources.
    own = {"dvc": [tmp_path / "ember/2026-01-01/electricity.csv.dvc"]}
    assert [o["producer"] for o in prepare.all_origins(dag, list(dag), own, carried)] == ["Ember"]


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


ANOMALIST_RESULT = """## Findings
1. 🟡 [data-level] anomalist:upgrade_change Germany 2022 revised by 40% with no release note
   Evidence: old 45.9, new 64.3

## Values checked
| indicator | entity | year | our value | independent value(s) | source link | verdict | anchor or anomaly |
|---|---|---|---|---|---|---|---|
| point_in_time_1 | Germany | 2022 | 64.3 | 64.3 (Destatis) | https://y.org | ✓ | anomalist:upgrade_change |

## Anomalies not checked
- Japan 2023: past the cap

## Notes for the other lane
none
"""


def test_assemble_folds_in_the_anomalist_pass(tmp_path):
    dossier = _dossier(tmp_path)
    (dossier / "lane_anomalist.result.md").write_text(ANOMALIST_RESULT)
    text, problems, counts = assemble_report.assemble(dossier, False, None, "2026-11-27")
    assert problems == [] and counts == {"🔴": 1, "🟡": 2, "🟢": 1}
    assert "Germany 2022 revised by 40% with no release note _(anomalist lane)_" in text
    assert "### Values checked: Anomalist candidates" in text and "Destatis" in text
    assert "### Anomalist candidates not checked" in text and "Japan 2023: past the cap" in text


def test_assemble_waits_for_a_started_anomalist_pass(tmp_path):
    dossier = _dossier(tmp_path)
    (dossier / "lane_anomalist.md").write_text("brief")
    text, problems, _ = assemble_report.assemble(dossier, False, None, "2026-11-27")
    assert text == "" and "lane_anomalist.result.md is not" in problems[0]
    # Without a brief, the first pass assembles on its own.
    (dossier / "lane_anomalist.md").unlink()
    assert assemble_report.assemble(dossier, False, None, "2026-11-27")[1] == []


def test_assemble_holds_unsettled_anomalist_notes(tmp_path):
    dossier = _dossier(tmp_path)
    notes = ANOMALIST_RESULT.replace("## Notes for the other lane\nnone", "## Notes for the other lane\nUnit is %.")
    (dossier / "lane_anomalist.result.md").write_text(notes)
    _, problems, _ = assemble_report.assemble(dossier, False, None, "2026-11-27")
    assert "anomalist lane → the other lanes" in problems[0]


# ----------------------------------------------------------------------------------------------------------------
# Anomalist candidates
# ----------------------------------------------------------------------------------------------------------------


def _stored(rows: list[tuple]) -> pd.DataFrame:
    """Stored anomalies as `view.build_table` returns them: (type, indicator_id, entity, year, relevance 0-1)."""
    frame = pd.DataFrame(rows, columns=["type", "indicator_id", "entity_name", "year", "score_weighted"])
    frame["indicator"] = frame["indicator_id"].map(lambda i: f"col{i}")
    frame["indicator_name"], frame["unit"] = "Title", "%"
    frame["anomaly"], frame["relevance"] = 100, (frame["score_weighted"] * 100).round().astype(int)
    return frame


INDICATORS = pd.DataFrame(
    {"indicator_id": [1, 2], "catalogPath": ["grapher/ns/v/thing/t#rate__sex_female", "grapher/ns/v/thing/t#gdp"]}
)
NO_FIRST_PASS = {"checked": set(), "flagged": set(), "has_result": False, "has_scan": False}


def test_anomalist_candidates_keep_the_ranking_and_list_everything_past_the_cap():
    stored = _stored(
        [
            ("time_change", 2, "Chad", 2020, 0.5),
            ("upgrade_change", 1, "France", 2023, 0.9),
            ("gp_outlier", 2, "Peru", 2021, 0.4),
        ]
    )
    rows = anomalist_candidates.candidates(stored, INDICATORS, NO_FIRST_PASS, top=2)
    assert [r["entity"] for r in rows] == ["France", "Chad", "Peru"]
    assert [r["selected"] for r in rows] == [True, True, False]
    assert "past the cap of 2" in rows[2]["why"]
    assert rows[0]["identity"] == "t#rate__sex_female" and rows[0]["year"] == "2023"


def test_anomalist_candidates_skip_what_the_first_pass_covered(tmp_path):
    (tmp_path / "lane_values.result.md").write_text(VALUES.replace("| gdp | World | 2024 |", "| gdp | Chad | 2020 |"))
    anomaly_scan.write_csv(
        [
            {
                "table": "t",
                "column": "rate",
                "kind": "OUTLIER",
                "period": 2023,
                "detail": "France, sex=female 2023: 9.9 (|z|>6)",
            }
        ],
        tmp_path / "anomalies.csv",
    )
    first_pass = anomalist_candidates.first_pass_keys(tmp_path)
    stored = _stored(
        [
            ("upgrade_change", 1, "France", 2023, 0.9),
            ("time_change", 2, "Chad", 2020, 0.5),
            ("time_change", 2, "Peru", 2020, 0.5),
        ]
    )
    rows = anomalist_candidates.candidates(stored, INDICATORS, first_pass, top=5)
    assert [r["status"] for r in rows] == [
        anomalist_candidates.FLAGGED,  # garden `rate` at sex=female is the grapher slice `rate__sex_female`
        anomalist_candidates.CHECKED,
        anomalist_candidates.NEW,
    ]
    assert [r["selected"] for r in rows] == [False, False, True]


def test_anomalist_first_pass_flag_needs_the_same_year():
    first_pass = {**NO_FIRST_PASS, "flagged": {("t#rate", "France: 9.9", "2019")}}
    assert anomalist_candidates.status_of("t#rate", "France", "2023", first_pass) == anomalist_candidates.NEW
    assert anomalist_candidates.status_of("t#rate", "France", "2019", first_pass) == anomalist_candidates.FLAGGED


def test_anomalist_scan_flag_covers_only_the_dimension_slice_it_names():
    flagged = {("t#rate", "France, sex=male, age=0-4 2023: ≥×10 jump from the previous period", "2023")}
    first_pass = {**NO_FIRST_PASS, "flagged": flagged}
    status = anomalist_candidates.status_of
    assert status("t#rate__sex_male__age_0_4", "France", "2023", first_pass) == anomalist_candidates.FLAGGED
    assert status("t#rate__sex_female__age_0_4", "France", "2023", first_pass) == anomalist_candidates.NEW
    assert status("t#rate__sex_male", "France", "2023", first_pass) == anomalist_candidates.NEW
    # A flag on a series with no dimensions covers the undimensioned column only.
    plain = {**NO_FIRST_PASS, "flagged": {("t#rate", "France 2023: 9.9 (|z|>6)", "2023")}}
    assert status("t#rate", "France", "2023", plain) == anomalist_candidates.FLAGGED
    assert status("t#rate__sex_female", "France", "2023", plain) == anomalist_candidates.NEW


def test_anomalist_checked_row_names_the_exact_column_not_a_related_one():
    first_pass = {**NO_FIRST_PASS, "checked": {("gdp_per_capita", "chad", "2020")}}
    assert anomalist_candidates.status_of("t#gdp", "Chad", "2020", first_pass) == anomalist_candidates.NEW
    assert anomalist_candidates.names_column("gdp", "t#gdp")
    assert anomalist_candidates.names_column("t#gdp", "t#gdp")
    assert anomalist_candidates.names_column("grapher/ns/v/thing/t#gdp", "t#gdp")
    assert anomalist_candidates.names_column("gdp per person (`gdp`)", "t#gdp")
    assert not anomalist_candidates.names_column("other#gdp", "t#gdp")
    assert not anomalist_candidates.names_column("deaths_per_100k", "t#deaths")
    assert not anomalist_candidates.names_column("gdp per capita", "t#gdp")


def test_anomalist_first_pass_flag_names_the_entity_not_a_longer_name_containing_it():
    first_pass = {**NO_FIRST_PASS, "flagged": {("t#rate", "Equatorial Guinea 2023: 9.9 (|z|>6)", "2023")}}
    assert anomalist_candidates.status_of("t#rate", "Guinea", "2023", first_pass) == anomalist_candidates.NEW
    assert anomalist_candidates.status_of("t#rate", "Equatorial Guinea", "2023", first_pass) == (
        anomalist_candidates.FLAGGED
    )
    assert anomalist_candidates.names_entity("Niger, sex=female 2020: ≥×10 jump", "Niger")
    assert not anomalist_candidates.names_entity("Nigeria 2020: ≥×10 jump", "Niger")
    assert not anomalist_candidates.names_entity("United States Virgin Islands 2020: 1", "United States")


def test_anomalist_neighbors_and_old_values():
    series = pd.Series([1.0, 2.0, 30.0], index=[2019, 2020, 2022])
    assert anomalist_candidates.neighbors(series, 2020) == ("2", "1 (2019)", "30 (2022)")
    assert anomalist_candidates.value_at(series, 2021) == "missing"
    assert anomalist_candidates.neighbors(None, 2020)[0].startswith("not on disk")


def test_anomalist_values_tell_a_corrupt_dataset_from_a_missing_one(tmp_path):
    """A dataset that is on disk but fails to read must say so, not pass as "not on disk"."""
    (tmp_path / "index.json").write_text("{not json")
    series, reason = anomalist_candidates.read_series({}, tmp_path, "t", "gdp", "Chad")
    assert series is None and reason.startswith("READ FAILED")
    assert anomalist_candidates.neighbors(series, 2020, reason)[0] == reason
    series, reason = anomalist_candidates.read_series({}, tmp_path / "absent", "t", "gdp", "Chad")
    assert series is None and reason == anomalist_candidates.NOT_ON_DISK


def test_anomalist_coverage_says_what_was_not_looked_at():
    stored = _stored([("time_change", 1, "Chad", 2020, 0.5)])
    notes = {"types": ["time_change"], "checksums": {"time_change": "old"}, "relevance": "KeyError: population"}
    text = " ".join(anomalist_candidates.coverage_notes(stored, INDICATORS, {"sourceChecksum": "new"}, notes))
    assert "1 of 2 indicators" in text
    assert "upgrade_change, upgrade_missing, gp_outlier" in text and "old-vs-new detectors are missing" in text
    assert "Stale: time_change" in text and "raw anomaly score" in text


def test_anomalist_rerun_sets_aside_the_previous_brief_and_result(tmp_path):
    """A rerun answers a new candidate set: the report must not fold in a result written for the old one."""
    for name in ("lane_anomalist.md", "lane_anomalist.result.md", "anomalist.csv"):
        (tmp_path / name).write_text("old")
    notes = anomalist_candidates.clear_previous_run(tmp_path)
    assert not (tmp_path / "lane_anomalist.md").exists() and not (tmp_path / "anomalist.csv").exists()
    assert not (tmp_path / "lane_anomalist.result.md").exists()
    assert (tmp_path / anomalist_candidates.PREVIOUS_RESULT).read_text() == "old"
    assert notes and "another candidate set" in notes[0]
    assert anomalist_candidates.clear_previous_run(tmp_path) == []


def test_anomalist_mapping_failure_leaves_old_values_unavailable():
    """Only a missing mapping table falls back to the same-named dataset; any other DB error is reported."""
    from sqlalchemy.exc import OperationalError, ProgrammingError

    class Engine:
        def __init__(self, error):
            self.error = error

    def fake_read_sql(sql, engine, params=None):
        if "wiz__variable_mapping" in sql:
            raise engine.error
        return pd.DataFrame({"catalogPath": ["ns/2025-01-01/thing"]})

    dataset = {"id": 7, "namespace": "ns", "shortName": "thing", "version": "2026-01-01"}
    missing = ProgrammingError("SELECT", {}, Exception(1146, "Table 'wiz__variable_mapping' doesn't exist"))
    denied = OperationalError("SELECT", {}, Exception(1142, "SELECT command denied"))
    with patch("etl.db.read_sql", fake_read_sql):
        assert anomalist_candidates.old_dataset(Engine(missing), dataset) == ("ns/2025-01-01/thing", {}, "")
        old_path, olds, reason = anomalist_candidates.old_dataset(Engine(denied), dataset)
    assert old_path is None and olds == {} and "could not be read" in reason and "denied" in reason
    rows = [{"selected": True, "identity": "t#gdp", "year": "2020", "entity": "Chad", "indicator_id": 2}]
    anomalist_candidates.add_values(rows, Path("/nonexistent"), "ns/2026-01-01/thing", (old_path, olds, reason))
    assert rows[0]["old_value"] == reason


# ----------------------------------------------------------------------------------------------------------------
# Review fixes
# ----------------------------------------------------------------------------------------------------------------


def test_refetched_dataset_page_drops_documents_it_no_longer_links(tmp_path, monkeypatch):
    """A methodology the producer took down must not live on in the index once the dataset page is read again."""
    now = dt.datetime(2026, 11, 16, 12, 0)
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    saved = [
        {
            "url": "https://x.org/",
            "role": "url_main",
            "status": "ok",
            "file": "01.txt",
            "fetched_at": "2026-11-01 09:00",
        },
        {
            "url": "https://x.org/old-method.pdf",
            "role": "linked-from-main",
            "parent": "https://x.org/",
            "status": "ok",
            "file": "02.txt",
            "fetched_at": "2026-11-15 09:00",
        },
    ]
    for entry in saved:
        (docs_dir / entry["file"]).write_text("text")
    (docs_dir / "index.json").write_text(json.dumps(saved))

    def fake_fetch(doc, docs_dir, number, now):
        doc.status, doc.via, doc.links = "ok", "direct", [("Methodology", "https://x.org/new-method.pdf")]
        return doc

    monkeypatch.setattr(fetch_docs, "fetch_one", fake_fetch)
    # The dataset page is older than seven days, so it is fetched again: its current links are the list.
    urls = {d.url for d in fetch_docs.fetch_all([("https://x.org/", "url_main")], tmp_path, now=now)}
    assert "https://x.org/new-method.pdf" in urls and "https://x.org/old-method.pdf" not in urls
    # While the page itself is served from the cache, the documents it led to last time are reused.
    saved[0]["fetched_at"] = "2026-11-15 09:00"
    (docs_dir / "index.json").write_text(json.dumps(saved))
    urls = {d.url for d in fetch_docs.fetch_all([("https://x.org/", "url_main")], tmp_path, now=now)}
    assert "https://x.org/old-method.pdf" in urls


def test_stated_year_is_checked_against_the_series_the_string_names():
    data = pd.DataFrame(
        {
            "Entity": ["France", "France"],
            "Year": [1990, 2000],
            "GDP": [1.0, 2.0],
            "Unemployment": [float("nan"), 8.0],
        }
    )
    out = "\n".join(prepare.section_strings(["Unemployment in France since 1990"], data))
    assert "`1990`: NO data that year in Unemployment for France" in out
    # A string that names no series is held to any series having data that year.
    out = "\n".join(prepare.section_strings(["France since 1990"], data))
    assert "NO data that year" not in out


def test_earlier_versions_of_script_and_package_steps(tmp_path):
    for version in ("2024-01-01", "2025-01-01", "2026-01-01"):
        (tmp_path / "ns" / version / "thing").mkdir(parents=True)
        (tmp_path / "ns" / version / "flat.meta.yml").write_text(version)
        (tmp_path / "ns" / version / "thing" / "thing.meta.yml").write_text(version)
    flat = tmp_path / "ns" / "2025-01-01" / "flat.meta.yml"
    package = tmp_path / "ns" / "2026-01-01" / "thing" / "thing.meta.yml"
    assert [v for v, _ in prepare.earlier_versions(flat, "2025-01-01")] == ["2024-01-01"]
    assert [v for v, _ in prepare.earlier_versions(package, "2026-01-01")] == ["2024-01-01", "2025-01-01"]


def test_chart_on_an_inactive_version_keeps_its_own_origins(tmp_path, monkeypatch):
    """The steps on disk are a newer release; the chart's indicators say which release it shows."""
    monkeypatch.setattr(prepare.paths, "STEPS_DATA_DIR", tmp_path / "steps")
    monkeypatch.setattr(prepare.paths, "SNAPSHOTS_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(prepare.paths, "BASE_DIR", tmp_path)
    (tmp_path / "steps" / "garden" / "ns" / "2026-01-01").mkdir(parents=True)
    (tmp_path / "steps" / "garden" / "ns" / "2026-01-01" / "thing.py").write_text("")
    _dvc(tmp_path / "snapshots/ns/2026-01-01/thing.csv.dvc", "Producer", "Release 2026")
    dag = {
        "data://garden/ns/2026-01-01/thing": {"snapshot://ns/2026-01-01/thing.csv"},
    }
    monkeypatch.setattr(prepare.fetch_docs, "fetch_all", lambda links, dossier, refresh: [])
    monkeypatch.setattr(prepare, "check_sources", lambda origins: [])
    carried = [{"producer": "Producer", "title": "Release 2025", "urlMain": "https://x.org/2025"}]
    result = prepare.dataset_sources(dag, ("ns", "2025-01-01", "thing"), False, [], carried)
    assert "not in this checkout's active DAG" in result["version_note"]
    assert [(o["title"], o["file"]) for o in result["origins"]] == [
        ("Release 2025", "(the chart's indicators, version 2025-01-01)")
    ]


def test_linked_documents_are_reused_per_cached_page(tmp_path, monkeypatch):
    """One dataset page cached and another fetched again: the cached page's documents stay."""
    now = dt.datetime(2026, 11, 16, 12, 0)
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    saved = [
        {
            "url": "https://a.org/",
            "role": "url_main",
            "status": "ok",
            "file": "01.txt",
            "fetched_at": "2026-11-15 09:00",
        },
        {
            "url": "https://b.org/",
            "role": "url_main",
            "status": "ok",
            "file": "02.txt",
            "fetched_at": "2026-11-01 09:00",
        },
        {
            "url": "https://a.org/codebook.pdf",
            "role": "linked-from-main",
            "parent": "https://a.org/",
            "status": "ok",
            "file": "03.txt",
            "fetched_at": "2026-11-15 09:00",
        },
        {
            "url": "https://b.org/gone.pdf",
            "role": "linked-from-main",
            "parent": "https://b.org/",
            "status": "ok",
            "file": "04.txt",
            "fetched_at": "2026-11-15 09:00",
        },
    ]
    for entry in saved:
        (docs_dir / entry["file"]).write_text("text")
    (docs_dir / "index.json").write_text(json.dumps(saved))

    def fake_fetch(doc, docs_dir, number, now):
        doc.status, doc.via, doc.links = "ok", "direct", []
        return doc

    monkeypatch.setattr(fetch_docs, "fetch_one", fake_fetch)
    links = [("https://a.org/", "url_main"), ("https://b.org/", "url_main")]
    urls = {d.url for d in fetch_docs.fetch_all(links, tmp_path, now=now)}
    assert "https://a.org/codebook.pdf" in urls and "https://b.org/gone.pdf" not in urls


def test_world_sum_is_checked_within_each_slice():
    rows = []
    for sex, world in (("female", 100), ("male", 61)):
        rows += [
            {"country": "World", "year": 2020, "sex": sex, "x": world},
            {"country": "A", "year": 2020, "sex": sex, "x": 30},
            {"country": "B", "year": 2020, "sex": sex, "x": 30},
        ]
    findings = anomaly_scan.scan_table(pd.DataFrame(rows), "t", {"x": "deaths"}, {"A", "B"}, dims=["sex"])
    assert [(f["kind"], f["detail"].split(":")[0]) for f in findings] == [("WORLD_SUM", "sex=female, 2020")]
    # A table of aggregates only (World and regions, no country rows) has nothing to compare.
    aggregates = pd.DataFrame(
        [{"country": "World", "year": 2020, "x": 100}, {"country": "Asia", "year": 2020, "x": 60}]
    )
    assert anomaly_scan.scan_table(aggregates, "t", {"x": "deaths"}, {"A", "B"}) == []


class _FakeEnv:
    def __init__(self, frames):
        self.frames, self.queries = frames, []

    def read_sql(self, sql, params=None):
        self.queries.append(sql)
        for marker, frame in self.frames.items():
            if marker in sql:
                return frame
        raise AssertionError(f"unexpected query: {sql}")


def test_staging_chart_reads_the_rendered_config_through_its_chart(monkeypatch):
    import etl.config

    env = _FakeEnv({"chart_configs": pd.DataFrame({"config": [json.dumps({"slug": "x", "title": "T"})]})})
    monkeypatch.setattr(etl.config.OWIDEnv, "from_staging", staticmethod(lambda branch: env))
    config, texts = prepare.load_chart("x", "my-branch")
    assert texts["title"] == "T"
    assert "JOIN chart_configs cc ON cc.id = c.configId" in env.queries[0] and "cc.config" in env.queries[0]


def test_staging_indicator_carries_its_origins(monkeypatch):
    import etl.config

    variables = pd.DataFrame(
        [
            {
                "id": 7,
                "name": "GDP",
                "unit": "$",
                "shortUnit": "$",
                "catalogPath": "grapher/ns/2025-01-01/d/t#gdp",
                "titlePublic": None,
                "descriptionShort": "s",
                "descriptionKey": None,
                "descriptionProcessing": None,
                "descriptionFromProducer": None,
            }
        ]
    )
    origins = pd.DataFrame(
        [
            {
                "producer": "World Bank",
                "title": "WDI",
                "urlMain": "https://wb.org",
                "urlDownload": None,
                "datePublished": "2025-07-01",
                "dateAccessed": None,
                "versionProducer": None,
                "citationFull": None,
            }
        ]
    )
    env = _FakeEnv({"FROM variables": variables, "FROM origins": origins})
    monkeypatch.setattr(etl.config.OWIDEnv, "from_staging", staticmethod(lambda branch: env))
    indicator = prepare.load_indicator(7, "my-branch")
    assert indicator["dataset"] == ("ns", "2025-01-01", "d")
    assert indicator["origins"] == [
        {"producer": "World Bank", "title": "WDI", "urlMain": "https://wb.org", "datePublished": "2025-07-01"}
    ]


def test_indicator_that_cannot_be_loaded_is_an_error(monkeypatch):
    class Response:
        status_code = 503

    monkeypatch.setattr(prepare.session, "get", lambda *a, **k: Response())
    with pytest.raises(LookupError, match="HTTP 503"):
        prepare.load_indicator(7, None)


def test_production_metadata_carries_each_indicators_origins(monkeypatch, tmp_path):
    """With no local build, a derived dataset's sources come from the origins its published indicators carry."""
    monkeypatch.setattr(prepare.paths, "DATA_DIR", tmp_path)
    calls = []

    def fake_query(sql):
        calls.append(sql)
        if "FROM origins o" in sql:
            return [
                {
                    "variableId": 7,
                    "producer": "Ember",
                    "title": "Yearly data",
                    "urlMain": "https://ember.org",
                    "urlDownload": None,
                    "datePublished": None,
                    "dateAccessed": None,
                    "versionProducer": None,
                    "citationFull": None,
                }
            ]
        return [
            {
                "id": 7,
                "catalogPath": "grapher/energy/2026-01-01/mix/mix#coal",
                "name": "Coal",
                "unit": "TWh",
                "shortUnit": None,
                "titlePublic": None,
                "descriptionShort": None,
                "descriptionKey": None,
                "descriptionProcessing": None,
                "descriptionFromProducer": None,
            }
        ]

    monkeypatch.setattr(prepare.collect_dods, "_query", fake_query)
    source, indicators = prepare.rendered_metadata("energy", "2026-01-01", "mix")
    assert source.startswith("production")
    assert indicators[0]["origins"] == [{"producer": "Ember", "title": "Yearly data", "urlMain": "https://ember.org"}]
    assert prepare.unique_origins(indicators[0]["origins"])[0]["url_main"] == "https://ember.org"
