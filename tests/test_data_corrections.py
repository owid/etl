"""Tests for etl.data_corrections (the .corrections.yml mechanism)."""

import json

import pandas as pd
import pytest
import yaml
from owid.catalog import Table

from etl.data_corrections import apply_corrections, load_corrections
from etl.paths import BASE_DIR, STEP_DIR


def _make_table() -> Table:
    tb = Table(
        {
            "country": ["Panama", "Panama", "Panama", "France", "France"],
            "year": [2006, 2008, 2016, 2006, 2016],
            "value": [-1.0, -2.0, -3.0, 10.0, 20.0],
        }
    )
    # Give the indicator column some metadata so we can check it survives.
    tb["value"].metadata.unit = "tonnes"
    tb["value"].metadata.title = "Some emissions"
    return tb


def _correction(**overrides):
    base = {
        "indicator": "value",
        "action": "drop",
        "reason": "Test reason.",
        "provider": "Test provider.",
        "status": "open",
    }
    base.update(overrides)
    return base


def test_drop_by_entity_and_year_list():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years=[2006, 2016])])
    # Two Panama rows dropped; France untouched.
    assert list(zip(out["country"], out["year"])) == [("Panama", 2008), ("France", 2006), ("France", 2016)]
    # Index is renumbered (matching the df.drop(...).reset_index(drop=True) idiom).
    assert list(out.index) == [0, 1, 2]
    # Metadata preserved.
    assert out["value"].metadata.unit == "tonnes"


def test_drop_does_not_mutate_input():
    tb = _make_table()
    apply_corrections(tb, [_correction(entity="Panama", years=[2006])])
    assert len(tb) == 5


def test_years_latest_resolves_to_global_max():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years="latest")])
    # The table's latest year is 2016; only Panama's 2016 row is dropped.
    assert (out["country"] == "Panama").sum() == 2
    assert not ((out["country"] == "Panama") & (out["year"] == 2016)).any()
    # France's 2016 row is untouched (the correction is scoped to Panama).
    assert ((out["country"] == "France") & (out["year"] == 2016)).any()


def test_years_range_from_to():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years={"from": 2007, "to": 2016})])
    # Drops Panama 2008 and 2016, keeps 2006.
    assert ((out["country"] == "Panama") & (out["year"] == 2006)).any()
    assert (out["country"] == "Panama").sum() == 1


def test_years_range_after_before():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="France", years={"after": 2006, "before": 2017})])
    assert (out["country"] == "France").sum() == 1  # only 2016 dropped


def test_override_by_match_value():
    tb = _make_table()
    out = apply_corrections(
        tb,
        [_correction(action="override", match={"value": 10.0}, value=99.0)],
    )
    # France 2006 had value 10.0 → now 99.0.
    assert out.loc[(out["country"] == "France") & (out["year"] == 2006), "value"].item() == 99.0
    # Metadata preserved through override.
    assert out["value"].metadata.unit == "tonnes"


def test_override_requires_value():
    # Validation rejects an override entry without a `value`.
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError):
        _validate_correction(_correction(action="override", match={"value": 10.0}), "<test>")


def test_match_column_reference_matches_across_columns():
    # A `match` value of {column: <name>} compares against another column of the same row instead
    # of a literal — e.g. dropping a row where two columns happen to hold the same (spurious) value.
    tb = Table(
        {
            "country": ["Belgium", "Belgium", "France"],
            "year": [2024, 2025, 2025],
            "barn": [5.0, 3.0, 3.0],
            "free_range": [5.0, 3.0, 4.0],
        }
    )
    out = apply_corrections(
        tb,
        [
            _correction(
                indicator="barn",
                action="drop",
                match={"country": "Belgium", "year": 2025, "value": {"column": "free_range"}},
            )
        ],
    )
    assert list(zip(out["country"], out["year"])) == [("Belgium", 2024), ("France", 2025)]


def test_match_column_reference_no_match_when_columns_differ():
    tb = Table({"country": ["Belgium"], "year": [2025], "barn": [5.0], "free_range": [7.0]})
    with pytest.raises(AssertionError, match="matched no rows"):
        apply_corrections(
            tb,
            [
                _correction(
                    indicator="barn",
                    action="drop",
                    match={"country": "Belgium", "year": 2025, "value": {"column": "free_range"}},
                )
            ],
        )


def test_match_column_reference_missing_column_raises():
    tb = _make_table()
    with pytest.raises(KeyError, match="not found"):
        apply_corrections(tb, [_correction(action="drop", match={"value": {"column": "does_not_exist"}})])


def test_validate_rejects_malformed_match_column_reference():
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="column"):
        _validate_correction(_correction(action="drop", match={"value": {"col": "free_range"}}), "<test>")


def test_validate_rejects_column_reference_as_override_value():
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="column"):
        _validate_correction(_correction(action="override", match={"value": 10.0}, value={"column": "other"}), "<test>")


def test_load_corrections_accepts_column_reference_match(tmp_path):
    p = tmp_path / "x.corrections.yml"
    p.write_text(
        "- indicator: barn\n"
        "  match: {country: Belgium, year: 2025, value: {column: free_range}}\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    corrections = load_corrections(p)
    assert corrections[0]["match"]["value"] == {"column": "free_range"}


def test_build_audit_handles_column_reference_match():
    from etl.data_corrections import build_audit

    tb = Table(
        {
            "country": ["Belgium", "France"],
            "year": [2025, 2025],
            "barn": [5.0, 3.0],
            "free_range": [5.0, 4.0],
        }
    )
    records = build_audit(
        tb,
        [
            _correction(
                indicator="barn",
                action="drop",
                match={"country": "Belgium", "year": 2025, "value": {"column": "free_range"}},
            )
        ],
    )
    assert records[0]["entities"][0]["entity"] == "Belgium"


def test_match_ignores_missing_values_in_other_rows():
    # A `match` correction must not blow up when other rows of the same column hold pandas' NA
    # sentinel (e.g. a nullable "string" dtype column) — comparing NA to the match target should
    # read as "no match", not raise `TypeError: boolean value of NA is ambiguous`.
    tb = Table(
        {
            "country": ["Panama", "France", "France"],
            "year": [2006, 2006, 2016],
            "value": pd.array(["-1", pd.NA, "Inaccurtae forecasts"], dtype="string"),
        }
    )
    out = apply_corrections(
        tb,
        [_correction(action="override", match={"value": "Inaccurtae forecasts"}, value="Inaccurate forecasts")],
    )
    assert out.loc[out["country"] == "France", "value"].tolist()[-1] == "Inaccurate forecasts"


def test_flag_is_a_noop_on_data():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(action="flag", entity="Panama", years=[2006])])
    assert len(out) == len(tb)


def test_unmatched_drop_raises():
    tb = _make_table()
    with pytest.raises(AssertionError, match="matched no rows"):
        apply_corrections(tb, [_correction(entity="Atlantis", years=[2006])])


def test_works_with_country_year_in_index():
    tb = _make_table().set_index(["country", "year"])
    out = apply_corrections(tb, [_correction(entity="Panama", years=[2006])])
    assert ("Panama", 2006) not in out.index
    assert len(out) == 4


def test_load_corrections_validates(tmp_path):
    p = tmp_path / "x.corrections.yml"
    p.write_text(
        "- indicator: value\n"
        "  entity: Panama\n"
        "  years: [2006]\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    corrections = load_corrections(p)
    assert corrections[0]["entity"] == "Panama"


def test_load_corrections_rejects_missing_required_field(tmp_path):
    p = tmp_path / "x.corrections.yml"
    # Missing 'reason'.
    p.write_text(
        "- indicator: value\n  entity: Panama\n  years: [2006]\n  action: drop\n  provider: p\n  status: open\n"
    )
    with pytest.raises(AssertionError, match="missing required 'reason'"):
        load_corrections(p)


def test_validate_rejects_empty_match():
    # An empty match would build an all-true mask and apply the correction to every row (wiping a table
    # on drop, or overwriting a whole indicator on override). Validation must reject it.
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="non-empty mapping"):
        _validate_correction(_correction(action="drop", match={}), "<test>")


def test_validate_rejects_match_combined_with_years():
    # `match` + `years` (without entity) passes the XOR check but `years` would be silently ignored.
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="do not combine 'match'"):
        _validate_correction(_correction(action="drop", match={"value": 10.0}, years=[2006]), "<test>")


def test_scale_multiplies_matched_values():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(action="scale", factor=0.5, entity="France", years="all")])
    # France 2006 (10.0) and 2016 (20.0) halved; Panama untouched.
    assert sorted(out.loc[out["country"] == "France", "value"].tolist()) == [5.0, 10.0]
    assert sorted(out.loc[out["country"] == "Panama", "value"].tolist()) == [-3.0, -2.0, -1.0]
    # Metadata preserved through scale.
    assert out["value"].metadata.unit == "tonnes"


def test_scale_requires_numeric_factor():
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="numeric 'factor'"):
        _validate_correction(_correction(action="scale", entity="France", years="all"), "<test>")
    # A boolean is not a valid factor (bool is an int subclass — guard against it).
    with pytest.raises(AssertionError, match="numeric 'factor'"):
        _validate_correction(_correction(action="scale", factor=True, entity="France", years="all"), "<test>")


def test_years_all_selects_every_year_for_entity():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(action="drop", entity="Panama", years="all")])
    # All three Panama rows dropped; France untouched.
    assert (out["country"] == "Panama").sum() == 0
    assert (out["country"] == "France").sum() == 2


def test_expect_passes_when_anomaly_present():
    tb = _make_table()
    # France values are 10 and 20, both > 5 → expectation holds, scale applies.
    out = apply_corrections(
        tb, [_correction(action="scale", factor=0.1, entity="France", years="all", expect={"gt": 5})]
    )
    assert sorted(out.loc[out["country"] == "France", "value"].tolist()) == [1.0, 2.0]


def test_expect_raises_when_anomaly_fixed():
    tb = _make_table()
    # Panama values are negative; expecting them > 0 fails (simulates an upstream fix).
    with pytest.raises(AssertionError, match="may have been fixed"):
        apply_corrections(
            tb, [_correction(action="override", value=0.0, entity="Panama", years="all", expect={"gt": 0})]
        )


def test_expect_rejected_on_flag():
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="cannot be combined with action 'flag'"):
        _validate_correction(_correction(action="flag", entity="Panama", years="all", expect={"gt": 0}), "<test>")


def test_expect_rejects_unknown_operator():
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError, match="unknown 'expect' operators"):
        _validate_correction(
            _correction(action="scale", factor=0.5, entity="France", years="all", expect={"approx": 10}), "<test>"
        )


def test_all_repo_corrections_match_schema():
    """Every `.corrections.yml` in the repo must validate against schemas/corrections-schema.json
    (the same schema VSCode uses), so the editor lint and CI agree."""
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((BASE_DIR / "schemas" / "corrections-schema.json").read_text())
    jsonschema.Draft7Validator.check_schema(schema)
    validator = jsonschema.Draft7Validator(schema)
    files = sorted(STEP_DIR.rglob("*.corrections.yml"))
    assert files, "expected at least one .corrections.yml in the repo"
    for path in files:
        data = yaml.safe_load(path.read_text())
        errors = [f"{list(e.path)}: {e.message}" for e in validator.iter_errors(data)]
        assert not errors, f"{path} does not match the corrections schema:\n" + "\n".join(errors)


def test_build_audit_captures_drop_before_values():
    from etl.data_corrections import build_audit

    tb = _make_table()
    records = build_audit(tb, [_correction(action="drop", entity="Panama", years=[2006, 2016])])
    assert len(records) == 1
    rec = records[0]
    assert rec["action"] == "drop" and rec["numeric"] is True
    ent = rec["entities"][0]
    assert ent["entity"] == "Panama"
    # Full pre-correction series is captured (all 3 Panama years).
    assert ent["series"] == [[2006, -1.0], [2008, -2.0], [2016, -3.0]]
    # Affected points carry the before value and None (removed) as the after.
    assert ent["affected"] == [[2006, -1.0, None], [2016, -3.0, None]]


def test_build_audit_captures_scale_before_after():
    from etl.data_corrections import build_audit

    tb = _make_table()
    records = build_audit(tb, [_correction(action="scale", factor=0.5, entity="France", years="all")])
    ent = records[0]["entities"][0]
    # before → before*factor for every affected point.
    assert ent["affected"] == [[2006, 10.0, 5.0], [2016, 20.0, 10.0]]


def test_build_audit_skips_non_integer_year_labels():
    from etl.data_corrections import build_audit

    # Some tables carry non-integer year labels for rows unrelated to the correction (e.g. a
    # reporting-period range like "1872-6") — those points should be dropped, not raise.
    tb = Table(
        {
            "country": ["France", "France", "France"],
            "year": ["1872-6", "2006", "2016"],
            "value": [5.0, 10.0, 20.0],
        }
    )
    records = build_audit(tb, [_correction(action="scale", factor=0.5, entity="France", years="all")])
    ent = records[0]["entities"][0]
    # The "1872-6" row is dropped from both the full series and (since `years: all` also matches it)
    # the affected points, while the two integer-year rows are kept.
    assert ent["series"] == [[2006, 10.0], [2016, 20.0]]
    assert ent["affected"] == [[2006, 10.0, 5.0], [2016, 20.0, 10.0]]


def test_audit_path_is_under_data_tree():
    from etl.data_corrections import audit_path_for
    from etl.paths import STEP_DIR

    p = STEP_DIR / "data" / "garden" / "gcp" / "2025-11-13" / "global_carbon_budget.corrections.yml"
    out = audit_path_for(p)
    assert out.name == "global_carbon_budget.audit.json"
    assert "corrections_audit" in out.parts and "garden" in out.parts


def test_load_corrections_rejects_both_entity_and_match(tmp_path):
    p = tmp_path / "x.corrections.yml"
    p.write_text(
        "- indicator: value\n"
        "  entity: Panama\n"
        "  years: [2006]\n"
        "  match: {value: 4.5}\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    with pytest.raises(AssertionError, match="exactly one of"):
        load_corrections(p)
